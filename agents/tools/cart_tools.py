from __future__ import annotations
import copy
import datetime
import json
import os
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import firestore  # google-cloud-firestore
from google.oauth2 import service_account
from dotenv import load_dotenv
from agents.helpers.firestore_utils import get_agent_id, get_tenant_id, user_root
from utils.logging import logger, debug_enabled
import re
load_dotenv()


def _is_meaningful_name(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    cleaned = value.strip()
    return bool(cleaned and cleaned.lower() not in {"item", "items"})


_SKU_CODE_RE = re.compile(r"^[A-Za-z0-9._/-]+$")
DEFAULT_SALES_INTELLIGENCE_ENDPOINT = "https://portal.agentflo.com/api/v2/basket/optimised"
PENDING_RECOMMENDATIONS_FIELD = "pending_recommendations"


def _looks_like_sku_code(value: Any) -> bool:
    if value is None:
        return False
    token = str(value).strip()
    if not token:
        return False
    if any(ch.isspace() for ch in token):
        return False
    return bool(_SKU_CODE_RE.match(token))


def _resolve_sales_intel_endpoint() -> str:
    """
    Resolve Sales Intelligence endpoint with backward-compatible env support.
    """
    raw = _clean_env_value(os.getenv("SALES_INTELLIGENCE_ENDPOINT") or os.getenv("SALES_INTELLIGENSE_ENDPOINT"))
    if not raw:
        return DEFAULT_SALES_INTELLIGENCE_ENDPOINT
    if "/api/v2/" in raw:
        return raw
    return DEFAULT_SALES_INTELLIGENCE_ENDPOINT


def _clean_env_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    token = str(value).strip()
    if not token:
        return None
    if len(token) >= 2 and token[0] == token[-1] and token[0] in {"'", '"'}:
        token = token[1:-1].strip()
    return token or None


def _redact_sensitive_for_log(data: Any) -> Any:
    if isinstance(data, dict):
        out = {}
        for key, value in data.items():
            key_l = str(key).lower()
            if any(secret in key_l for secret in ("token", "authorization", "api_key", "apikey", "secret", "password")):
                out[key] = "***REDACTED***"
            else:
                out[key] = _redact_sensitive_for_log(value)
        return out
    if isinstance(data, list):
        return [_redact_sensitive_for_log(v) for v in data]
    return data

def _enrich_item_with_product_details(sku_code: str, existing_metadata: dict) -> dict:
    """
    Fetch product details from API and return enriched metadata.
    Includes validation to ensure API result matches the requested SKU.
    """
    def _pick_name(product: dict) -> Optional[str]:
        name_val = (
            product.get("official_name")
            or product.get("product_name")
            or product.get("name")
            or product.get("sku_name")
            or product.get("display_name")
            or product.get("description")
            or product.get("sku_desc")
        )
        return name_val if _is_meaningful_name(name_val) else None

    def _pick_price(product: dict, keys: List[str]) -> Optional[float]:
        pricing = product.get("pricing") or {}
        for key in keys:
            raw = product.get(key)
            if raw is None and isinstance(pricing, dict):
                raw = pricing.get(key)
            try:
                if raw is not None:
                    return float(raw)
            except (TypeError, ValueError):
                continue
        return None

    # If we already have a meaningful name/price, trust the input (optimization)
    if _is_meaningful_name(existing_metadata.get("name")) and existing_metadata.get("base_price"):
        return existing_metadata

    try:
        from agents.tools.api_tools import search_products_by_sku, unwrap_tool_response
        import json
        
        # Call API
        api_response = search_products_by_sku([sku_code])
        ok, payload, err = unwrap_tool_response(api_response, system_name="search_products_by_sku")
        
        if not ok or not payload:
            return existing_metadata

        # Handle stringified JSON if necessary
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                return existing_metadata

        # Unwrap common data envelopes
        products = payload
        if isinstance(payload, dict):
            products = payload.get("data") or payload.get("products") or payload.get("items") or payload.get("result")

        if products and isinstance(products, list) and len(products) > 0:
            product = products[0]
            
            # --- CRITICAL FIX START: Verify SKU Match ---
            # Sometimes API fuzzy matches; we want to ensure we aren't enriching SKU A with SKU B's data
            api_sku = product.get("sku") or product.get("sku_code") or product.get("item_number")
            if api_sku:
                def _numeric_suffix(s): 
                    m = re.search(r'\d+$', str(s))
                    return m.group().lstrip('0') if m else None
                if _numeric_suffix(api_sku) != _numeric_suffix(sku_code):
                    logger.warning(f"Enrichment Mismatch: Requested '{sku_code}', got '{api_sku}'. Ignoring API result.")
                    return existing_metadata
            

            enriched = dict(existing_metadata)
            
            # 1. Enrich Name
            if not _is_meaningful_name(enriched.get("name")):
                enriched["name"] = _pick_name(product) or "Item"
            
            # 2. Enrich Price (Check multiple fields)
            if not enriched.get("base_price"):
                price_val = _pick_price(
                    product,
                    [
                        "base_price",
                        "consumer_price",
                        "list_price",
                        "mrp",
                        "unit_price",
                        "price",
                        "total_buy_price_virtual_pack",
                    ],
                )
                if price_val is not None:
                    enriched["base_price"] = price_val
            
            # 3. Ensure Final Price exists
            if not enriched.get("final_price"):
                final_val = _pick_price(
                    product,
                    [
                        "final_price",
                        "discounted_price",
                        "unit_price_final",
                        "unit_price",
                        "price",
                        "total_buy_price_virtual_pack",
                    ],
                )
                if final_val is not None:
                    enriched["final_price"] = final_val
                elif enriched.get("base_price") is not None:
                    enriched["final_price"] = enriched["base_price"]
            
            # 4. Enrich Retailer ID (Needed for Catalog matching)
            if not enriched.get("product_retailer_id"):
                enriched["product_retailer_id"] = (
                    product.get("product_retailer_id")
                    or product.get("product_id")
                    or product.get("productid")
                    or product.get("retailer_id")
                    or product.get("id")
                )
            
            return enriched
        
    except Exception as e:
        logger.warning(f"Failed to enrich item {sku_code}: {e}")
    
    return existing_metadata

def agentflo_cart_tool(payload: dict) -> dict:
    """
    PRODUCTION-SAFE Firestore-backed cart tool (per tenant + user + store)
    with pricing via Sales Intelligence API objective=CART_ITEMS.
    Promotion/pricing refresh is hardwired after every successful mutation
    (especially ADD_ITEM), so callers/LLM do not need separate CART_ITEMS calls.

    Identity (tenant_id is read from TENANT_ID env):
      user_id: str
      store_id: str  (store code / store reference)

    Pricing (recommended for any mutation):
      sales_intel_token: str
      sales_intel_cart_items_url: str
      customer_id: str (optional; defaults to store_id)

    Operations (list; applied in order; each op is an object):
      GET_CART
      ADD_ITEM: {op, sku_code|sku, qty, name?, product_retailer_id?, merge_strategy?, replace_confirmed?}
                Default behavior is INCREMENT.
                REPLACE is honored only when merge_strategy="REPLACE" AND replace_confirmed=true.
      SET_QTY: {op, sku_code|sku, qty}   (qty=0 removes)
      REMOVE_ITEM: {op, sku_code|sku}
      CLEAR_CART: {op}
      UNDO: {op}   (restores most recent snapshot)
      - For "remove everything" intent, prefer CLEAR_CART even if user wording varies
        (for example: clear cart, empty cart, khaali kardo, sab hata do, remove all, delete all).

    Recommended batching for multi-item requests:
      - Put all ADD/SET/REMOVE operations for that user request into ONE call.
      - Append {"op": "GET_CART"} as the final operation in the same request.
      - Avoid one-item-per-call loops when the full list is already known.

    Options:
      max_lines: int (default 200)
      max_undo: int (default 20)
      expected_cart_version: int (optional optimistic lock, checked on FIRST mutating op)
      sales_intel_timeout_s: int (default 20)

    Firestore layout:
      tenants/{tenant_id}/agent_id/{agent_id}/users/{user_id}/store_carts/{store_id}
      tenants/{tenant_id}/agent_id/{agent_id}/users/{user_id}/store_carts/{store_id}/undo/{undo_id}

    Returns:
      {
        "ok": bool,
        "cart": dict|None,
        "errors": [..],    # fatal
        "warnings": [..]   # non-fatal
      }
    """

    # Never allow caller-supplied tenant_id; always derive from environment
    if isinstance(payload, dict) and "tenant_id" in payload:
        logger.info("agentflo_cart_tool.tenant_id_ignored", supplied=payload.get("tenant_id"))
        payload = dict(payload)
        payload.pop("tenant_id", None)

    if debug_enabled():
        try:
            ops = payload.get("operations") if isinstance(payload, dict) else None
            op_names = [op.get("op") for op in ops if isinstance(op, dict)] if isinstance(ops, list) else []
            logger.info(
                "tool.call",
                tool="agentflo_cart_tool",
                user_id=(payload or {}).get("user_id"),
                store_id=(payload or {}).get("store_id"),
                op_count=len(op_names),
                ops=op_names[:8],
            )
        except Exception:
            pass

    # -------------------------
    # helpers
    # -------------------------
    def now_iso() -> str:
        return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()

    def as_int(x):
        try:
            return int(x)
        except Exception:
            return None

    def as_float(x):
        try:
            if x is None:
                return None
            if isinstance(x, str):
                x = x.replace(",", "").strip()
                if not x:
                    return None
            return float(x)
        except Exception:
            return None

    def as_bool(x) -> bool:
        if isinstance(x, bool):
            return x
        if isinstance(x, (int, float)):
            return x != 0
        if isinstance(x, str):
            token = x.strip().lower()
            if token in {"1", "true", "t", "yes", "y", "on"}:
                return True
            if token in {"0", "false", "f", "no", "n", "off"}:
                return False
        return False

    def is_placeholder_store_id(value: Any) -> bool:
        token = str(value or "").strip().lower()
        if token in {"", "unknown", "none", "null", "n/a", "na", "-", "0"}:
            return True
        # Reject any token that starts with "unknown" (catches "UNKNOWN_STORECODE", "UNKNOWN_STORE", etc.)
        if token.startswith("unknown"):
            return True
        # Reject values that look like phone numbers (11+ digits, typically used as fallback user_id)
        # Real store codes are alphanumeric short codes, not 10-13 digit phone numbers.
        import re as _re
        if _re.fullmatch(r"\+?\d{10,15}", token):
            return True
        return False

    def pick_first(d: dict, keys: List[str]) -> Any:
        for key in keys:
            if key in d and d.get(key) is not None:
                return d.get(key)
        return None

    def pick_price(d: dict, keys: List[str]) -> Optional[float]:
        pricing = d.get("pricing") if isinstance(d.get("pricing"), dict) else {}
        for key in keys:
            raw = d.get(key)
            if raw is None and isinstance(pricing, dict):
                raw = pricing.get(key)
            val = as_float(raw)
            if val is not None:
                return val
        return None

    def pick_name(d: dict) -> Optional[str]:
        raw = pick_first(
            d,
            [
                "name",
                "official_name",
                "product_name",
                "sku_name",
                "description",
                "description_en",
                "title",
                "sku_desc",
            ],
        )
        return raw if _is_meaningful_name(raw) else None

    def pick_product_retailer_id(d: dict) -> Optional[str]:
        raw = pick_first(d, ["product_retailer_id", "productid", "product_id", "retailer_id", "id"])
        if raw is None:
            return None
        raw_str = str(raw).strip()
        return raw_str or None

    def get_sku(d: dict) -> Optional[str]:
        v = pick_first(d, ["sku_code", "sku", "skucode", "sku_id", "item_number", "variant_code", "id"])
        if v is None:
            return None
        v_str = str(v).strip()
        if not v_str:
            return None
        if not _looks_like_sku_code(v_str):
            return None
        return v_str

    def normalize_items(items_any: Any) -> List[dict]:
        if not isinstance(items_any, list):
            return []
        out = []
        for it in items_any:
            if not isinstance(it, dict):
                continue
            sku = get_sku(it)
            qty = as_int(pick_first(it, ["qty", "quantity", "forecast_qty"]))
            if not sku or qty is None:
                raw_sku = pick_first(it, ["sku_code", "sku", "skucode", "sku_id", "item_number", "variant_code", "id"])
                if raw_sku is not None and not sku:
                    logger.warning(
                        "cart.normalize_item.invalid_sku_skipped",
                        raw_sku=str(raw_sku),
                        name=pick_first(it, ["name", "official_name", "product_name", "sku_name", "description"]),
                    )
                continue
            normalized = {
                "sku_code": sku,
                "sku": sku,  # alias
                "qty": qty,
            }

            name = pick_name(it)
            if name:
                normalized["name"] = name

            product_retailer_id = pick_product_retailer_id(it)
            if product_retailer_id:
                normalized["product_retailer_id"] = product_retailer_id

            price = pick_price(
                it,
                [
                    "price",
                    "unit_price",
                    "final_price",
                    "base_price",
                    "consumer_price",
                    "list_price",
                    "mrp",
                    "total_buy_price_virtual_pack",
                ],
            )
            base_price = pick_price(
                it,
                [
                    "base_price",
                    "consumer_price",
                    "list_price",
                    "mrp",
                    "unit_price",
                    "price",
                ],
            )
            final_price = pick_price(
                it,
                [
                    "final_price",
                    "discounted_price",
                    "unit_price_final",
                    "unit_price",
                    "price",
                    "total_buy_price_virtual_pack",
                ],
            )
            line_total = pick_price(it, ["line_total", "linetotal", "lineamount", "line_total_amount"])
            unit_discount = pick_price(it, ["discount_value", "unit_discount"])
            line_discount = pick_price(it, ["discount_value_line", "line_discount"])
            if unit_discount is None:
                unit_discount = pick_price(it, ["discount"])
            if unit_discount is None and line_discount is not None and qty > 0:
                unit_discount = round(float(line_discount) / float(qty), 4)
            if line_discount is None and unit_discount is not None and qty > 0:
                line_discount = round(float(unit_discount) * float(qty), 2)
            discount_pct = pick_price(it, ["discount_pct", "discountvalue", "discount_percentage"])
            # profit = pick_price(it, ["profit", "line_profit"])
            # profit_margin = pick_price(it, ["profit_margin", "profit_margin_pct", "margin_pct"])
            old_line_total = pick_price(it, ["old_line_total", "pre_discount_line_total"])

            if base_price is None and final_price is not None:
                base_price = final_price
            if final_price is None and base_price is not None:
                final_price = base_price
            if line_total is None and final_price is not None:
                line_total = round(float(final_price) * float(qty), 2)

            numeric_fields = {
                "price": price,
                "base_price": base_price,
                "final_price": final_price,
                "discount_value": unit_discount,
                "discount_value_line": line_discount,
                "discount_pct": discount_pct,
                "line_total": line_total,
                # "profit": profit,
                # "profit_margin": profit_margin,
                "old_line_total": old_line_total,
            }
            for key, val in numeric_fields.items():
                if val is not None:
                    normalized[key] = val

            tags = it.get("tags")
            if tags is not None:
                normalized["tags"] = tags

            primary_reason = pick_first(it, ["primary_reason", "reason"])
            if primary_reason is not None:
                normalized["primary_reason"] = primary_reason
            out.append(normalized)
        return out

    def normalize_cart_doc(tenant_id: str, user_id: str, store_id: str, cart_in: Optional[dict]) -> dict:
        cart = copy.deepcopy(cart_in) if isinstance(cart_in, dict) else {}
        cart["tenant_id"] = tenant_id
        cart["user_id"] = user_id
        cart["store_id"] = store_id

        cart["items"] = normalize_items(cart.get("items") or cart.get("skus") or [])

        cart.setdefault("cart_version", 0)
        cart.setdefault("last_updated", now_iso())

        # undo_stack holds small pointers only
        cart.setdefault("undo_stack", [])  # [{"id": "...", "ts": "..."}]

        # priced snapshot (from Sales Intel)
        cart.setdefault("basket", None)
        cart.setdefault("totals", None)
        cart.setdefault("explanations", None)
        cart.setdefault("objective_used", None)
        cart.setdefault("basket_id", None)

        return cart

    def find_idx(cart: dict, sku_code: str) -> Optional[int]:
        for i, it in enumerate(cart.get("items") or []):
            if it.get("sku_code") == sku_code:
                return i
        return None

    def build_items_for_pricing(cart: dict) -> List[dict]:
        items = []
        for it in (cart.get("items") or []):
            sku = get_sku(it)
            qty = as_int(it.get("qty")) or 0
            if sku and qty > 0:
                items.append({"sku": sku, "qty": qty})
        return items

    def call_sales_intel_cart_items_api(
        *,
        tenant_id: Optional[str],
        agent_id: Optional[str],
        customer_id: str,
        items: List[dict],
        timeout_s: int,
    ) -> Tuple[Optional[dict], Optional[dict]]:

        api_url = _resolve_sales_intel_endpoint()
        tenant_id = tenant_id or _get_tenant_id()
        sales_intel_token = _clean_env_value(os.getenv("API_JWT_TOKEN"))
        if not items:
            # empty cart -> synthetic response
            return {
                "tenant_id": tenant_id,
                "customer_id": customer_id,
                "objective_used": "CART_ITEMS",
                "basket": {
                    "basket_id": None,
                    "objective": "CART_ITEMS",
                    "items": [],
                    "totals": {"subtotal": 0.0, "discount_total": 0.0, "grand_total": 0.0},
                    "explanations": ["Empty cart"],
                    "reason_breakdown": []
                }
            }, None

        req_json = {
            "tenant_id": tenant_id,
            "customer_id": customer_id,
            "sales_intel_token": sales_intel_token,
            "objective": "CART_ITEMS",
            "items": items,
        }
        if agent_id:
            req_json["agent_id"] = str(agent_id).strip()

        headers = {
            "Content-Type": "application/json",
            "X-Requested-With": "XMLHttpRequest",
        }

        try:
            logger.info(
                "sales_intel.cart_items.request",
                endpoint=api_url,
                payload=_redact_sensitive_for_log(req_json),
            )
            r = requests.post(api_url, headers=headers, json=req_json, timeout=timeout_s)
            resp_payload = None
            resp_text = None
            try:
                resp_payload = r.json()
            except Exception:
                resp_text = r.text[:2000] if r.text else None
            logger.info(
                "sales_intel.cart_items.response",
                endpoint=api_url,
                status_code=r.status_code,
                payload=_redact_sensitive_for_log(resp_payload) if isinstance(resp_payload, (dict, list)) else None,
                body_preview=resp_text,
            )
            if r.status_code >= 400:
                return None, {
                    "code": "SALES_INTEL_API_ERROR",
                    "http_status": r.status_code,
                    "message": "Sales Intelligence API returned an error.",
                    "body": (r.text[:1500] if r.text else None),
                }
            data = resp_payload if isinstance(resp_payload, dict) else r.json()
            if isinstance(data, dict):
                # Unwrap common envelopes: data/body/payload/result
                for _ in range(2):
                    unwrapped = None
                    for key in ("data", "body", "payload", "result"):
                        if key not in data:
                            continue
                        inner = data.get(key)
                        if isinstance(inner, str):
                            try:
                                inner = json.loads(inner)
                            except Exception:
                                inner = None
                        if isinstance(inner, dict):
                            unwrapped = inner
                            break
                    if isinstance(unwrapped, dict):
                        data = unwrapped
                    else:
                        break

                # Some orchestrator variants return root-level items/totals without basket wrapper.
                if "basket" not in data and any(k in data for k in ("items", "totals", "summary", "explanations")):
                    data = {
                        "tenant_id": data.get("tenant_id") or tenant_id,
                        "customer_id": data.get("customer_id") or customer_id,
                        "objective_used": data.get("objective_used") or data.get("objective") or "CART_ITEMS",
                        "basket": {
                            "basket_id": data.get("basket_id") or data.get("id"),
                            "objective": data.get("objective_used") or data.get("objective") or "CART_ITEMS",
                            "items": data.get("items") if isinstance(data.get("items"), list) else [],
                            "totals": data.get("totals") if isinstance(data.get("totals"), dict) else {},
                            "summary": data.get("summary") if isinstance(data.get("summary"), dict) else {},
                            "explanations": data.get("explanations") if isinstance(data.get("explanations"), list) else [],
                        },
                    }

            if not isinstance(data, dict) or "basket" not in data:
                return None, {
                    "code": "BAD_API_RESPONSE",
                    "message": "Unexpected response from Sales Intelligence API.",
                    "response_keys": list(data.keys())[:20] if isinstance(data, dict) else None,
                }
            return data, None
        except requests.Timeout:
            return None, {"code": "SALES_INTEL_TIMEOUT", "message": "Sales Intelligence API timed out."}
        except Exception as e:
            return None, {"code": "SALES_INTEL_EXCEPTION", "message": f"Sales Intelligence API call failed: {type(e).__name__}"}

    def merge_pricing_into_cart_items(cart_items: List[dict], basket_items: List[dict]) -> List[dict]:
        """
        basket_items include per-unit prices + line_total.
        discount_value in your sample is per-unit; we store both per-unit and line-level.
        """
        def _collect_applied_promotions(pit: dict) -> tuple[list[str], list[str]]:
            promo_lines: list[str] = []
            promo_codes: list[str] = []

            raw_promos = pit.get("applied_promotions")
            promo_entries: list[Any] = []
            if isinstance(raw_promos, list):
                promo_entries = raw_promos
            elif isinstance(raw_promos, dict):
                promo_entries = [raw_promos]
            elif isinstance(raw_promos, str) and raw_promos.strip():
                promo_entries = [raw_promos.strip()]

            for entry in promo_entries:
                if isinstance(entry, dict):
                    desc = (
                        entry.get("description")
                        or entry.get("promotion_description")
                        or entry.get("offer_text")
                    )
                    code = (
                        entry.get("promotion_id")
                        or entry.get("promotioncode")
                        or entry.get("promotion_code")
                        or entry.get("code")
                    )
                    if isinstance(desc, str) and desc.strip():
                        promo_lines.append(desc.strip())
                    elif code:
                        promo_lines.append("Promotion applied")
                    if code:
                        promo_codes.append(str(code).strip())
                elif isinstance(entry, str) and entry.strip():
                    promo_lines.append(entry.strip())

            top_level_code = (
                pit.get("promotioncode")
                or pit.get("promotion_code")
                or pit.get("promotion_id")
            )
            if top_level_code:
                promo_codes.append(str(top_level_code).strip())
                if not promo_lines:
                    promo_lines.append("Promotion applied")

            # Stable dedupe preserving order
            dedup_lines: list[str] = []
            seen_lines = set()
            for line in promo_lines:
                key = line.strip().lower()
                if key and key not in seen_lines:
                    seen_lines.add(key)
                    dedup_lines.append(line)

            dedup_codes: list[str] = []
            seen_codes = set()
            for code in promo_codes:
                token = code.strip()
                key = token.lower()
                if key and key not in seen_codes:
                    seen_codes.add(key)
                    dedup_codes.append(token)

            return dedup_lines, dedup_codes

        by_sku = {}
        for pit in basket_items:
            if not isinstance(pit, dict):
                continue
            pit_sku = get_sku(pit)
            if isinstance(pit_sku, str):
                by_sku[pit_sku.strip().lower()] = pit

        merged = []
        for it in cart_items:
            sku = it.get("sku_code")
            qty = as_int(it.get("qty")) or 0
            pit = by_sku.get(str(sku).strip().lower()) if sku else None
            out = dict(it)

            if isinstance(pit, dict):
                # Only take the basket name if the cart item has no meaningful name.
                # Basket often returns placeholders like "SKU 36" that we don't want
                # overwriting a real name already stored from the catalog lookup.
                if not pick_name(out):
                    candidate_name = pick_name(pit)
                    if candidate_name:
                        out["name"] = candidate_name

                base_price = pick_price(
                    pit,
                    [
                        "base_price",
                        "consumer_price",
                        "list_price",
                        "mrp",
                        "unit_price",
                        "price",
                    ],
                )
                final_price = pick_price(
                    pit,
                    [
                        "final_price",
                        "discounted_price",
                        "unit_price_final",
                        "unit_price",
                        "price",
                        "total_buy_price_virtual_pack",
                    ],
                )
                line_total = pick_price(pit, ["line_total", "linetotal", "lineamount", "line_total_amount"])
                old_line_total = pick_price(
                    pit,
                    ["old_line_total", "pre_discount_line_total", "line_total_before_discount", "total_before_discount"],
                )
                discount_pct = pick_price(pit, ["discount_pct", "discountvalue", "discount_percentage"])
                discount_unit = pick_price(pit, ["discount_value", "unit_discount"])
                line_discount = pick_price(pit, ["line_discount"])
                if line_discount is None and old_line_total is not None and line_total is not None:
                    try:
                        delta_line = round(float(old_line_total) - float(line_total), 4)
                        if delta_line > 0:
                            line_discount = delta_line
                    except Exception:
                        pass
                if discount_unit is None and line_discount is not None and qty > 0:
                    discount_unit = round(float(line_discount) / float(qty), 4)
                if discount_unit is None:
                    discount_unit = pick_price(pit, ["discount"])
                # profit = pick_price(pit, ["profit", "line_profit"])
                # profit_margin = pick_price(pit, ["profit_margin", "profit_margin_pct", "margin_pct"])

                if base_price is None and old_line_total is not None and qty > 0:
                    try:
                        base_price = round(float(old_line_total) / float(qty), 4)
                    except Exception:
                        pass
                if base_price is None and final_price is not None:
                    if discount_pct is not None and 0 <= float(discount_pct) < 100:
                        base_price = round(float(final_price) / (1.0 - float(discount_pct) / 100.0), 4)
                    elif discount_unit is not None:
                        base_price = round(float(final_price) + float(discount_unit), 4)
                    else:
                        base_price = final_price
                if final_price is None and base_price is not None:
                    if discount_pct is not None and 0 <= float(discount_pct) < 100:
                        final_price = round(float(base_price) * (1.0 - float(discount_pct) / 100.0), 4)
                    elif discount_unit is not None:
                        final_price = round(float(base_price) - float(discount_unit), 4)
                    else:
                        final_price = base_price
                if line_total is None and final_price is not None:
                    line_total = round(float(final_price) * float(qty), 2)

                if discount_unit is None and base_price is not None and final_price is not None:
                    try:
                        delta = round(float(base_price) - float(final_price), 4)
                        if delta > 0:
                            discount_unit = delta
                    except Exception:
                        pass
                if discount_pct is None and old_line_total is not None and line_total is not None and float(old_line_total) > 0:
                    try:
                        discount_pct = round(((float(old_line_total) - float(line_total)) / float(old_line_total)) * 100.0, 4)
                    except Exception:
                        pass

                if base_price is not None:
                    out["base_price"] = base_price
                if final_price is not None:
                    out["final_price"] = final_price
                if line_total is not None:
                    out["line_total"] = line_total
                if old_line_total is not None:
                    out["old_line_total"] = old_line_total
                if discount_pct is not None:
                    out["discount_pct"] = discount_pct
                # if profit is not None:
                #     out["profit"] = profit
                # if profit_margin is not None:
                #     out["profit_margin"] = profit_margin

                reason = pick_first(pit, ["primary_reason", "reason", "source"])
                if reason is not None:
                    out["primary_reason"] = reason
                tags = pit.get("tags") or pit.get("recommendation_tags")
                if tags is not None:
                    out["tags"] = tags

                promo_lines, promo_codes = _collect_applied_promotions(pit)
                if promo_lines:
                    out["applied_promotions"] = promo_lines
                if promo_codes:
                    out["promotion_codes"] = promo_codes

                # discount_value in response appears per-unit; compute line-level too
                if discount_unit is not None:
                    out["discount_value"] = discount_unit
                    out["discount_value_per_unit"] = round(discount_unit, 4)
                    if line_discount is not None:
                        out["discount_value_line"] = round(float(line_discount), 2)
                    else:
                        out["discount_value_line"] = round(discount_unit * qty, 2)

            merged.append(out)

        return merged

    def _build_local_pricing_from_products(cart_state: dict) -> tuple[list[dict], dict]:
        """
        DEMO-only fallback: derive basic pricing from local data/products.json
        when external pricing is unavailable.
        """
        try:
            # Locate data/products.json relative to this file.
            base_dir = Path(__file__).resolve().parents[2]  # Agentflo-YTL/
            products_path = base_dir / "data" / "products.json"
            if not products_path.is_file():
                return [], {}

            with products_path.open("r", encoding="utf-8") as f:
                data = json.load(f) or {}

            products = data.get("products") if isinstance(data, dict) else None
            if not isinstance(products, list):
                return [], {}

            # Index products by SKU code.
            index: dict[str, dict] = {}
            for p in products:
                if not isinstance(p, dict):
                    continue
                code = (p.get("sku_code") or p.get("sku") or "").strip().upper()
                if code:
                    index[code] = p

            basket_items: list[dict] = []
            items = cart_state.get("items") or cart_state.get("skus") or []

            def _to_float(val: Any) -> Optional[float]:
                try:
                    if val is None:
                        return None
                    return float(val)
                except (TypeError, ValueError):
                    return None

            for it in items:
                if not isinstance(it, dict):
                    continue
                sku_raw = it.get("sku_code") or it.get("sku") or it.get("item_number")
                sku = (str(sku_raw).strip().upper() if sku_raw else "")
                if not sku:
                    continue

                product = index.get(sku)
                if not isinstance(product, dict):
                    continue

                qty = as_int(it.get("qty") if it.get("qty") is not None else it.get("quantity"))
                if qty is None or qty <= 0:
                    continue

                name_val = (
                    product.get("official_name")
                    or product.get("product_name")
                    or product.get("name")
                    or sku
                )

                price_candidate = (
                    product.get("base_price")
                    or product.get("price_per_m3")
                    or product.get("price_per_trip")
                )
                pricing = product.get("pricing") or {}
                if price_candidate is None and isinstance(pricing, dict):
                    price_candidate = (
                        pricing.get("sell_price")
                        or pricing.get("price_per_m3")
                        or pricing.get("price_per_trip")
                    )

                unit_price = _to_float(price_candidate)
                if unit_price is None:
                    continue

                line_total = round(float(unit_price) * float(qty), 2)

                basket_items.append(
                    {
                        "sku": sku,
                        "sku_code": sku,
                        "name": name_val,
                        "base_price": unit_price,
                        "final_price": unit_price,
                        "line_total": line_total,
                        "qty": qty,
                    }
                )

            if not basket_items:
                return [], {}

            grand_total = sum(float(bi.get("line_total") or 0.0) for bi in basket_items)
            total_qty = sum(as_int(bi.get("qty")) or 0 for bi in basket_items)

            totals = {
                "subtotal": grand_total,
                "discount_total": 0.0,
                "grand_total": grand_total,
                "total_qty": total_qty,
            }

            return basket_items, totals
        except Exception as e:
            logger.warning("cart.local_pricing.error", error=str(e))
            return [], {}

    def ensure_totals_fields(totals_in: Optional[dict], items: List[dict]) -> dict:
        totals = dict(totals_in) if isinstance(totals_in, dict) else {}

        subtotal = as_float(totals.get("subtotal"))
        if subtotal is None:
            subtotal = as_float(totals.get("total_list_price"))
        if subtotal is None:
            subtotal_sum = 0.0
            has_subtotal = False
            for it in items:
                qty = as_int(it.get("qty")) or 0
                base_price = as_float(it.get("base_price"))
                if base_price is None:
                    base_price = as_float(it.get("price"))
                if base_price is None:
                    continue
                subtotal_sum += float(base_price) * float(qty)
                has_subtotal = True
            if has_subtotal:
                subtotal = round(subtotal_sum, 2)
                totals["subtotal"] = subtotal

        grand_total = as_float(totals.get("grand_total"))
        if grand_total is None:
            grand_total = as_float(totals.get("achieved_total"))
        if grand_total is None:
            total_val = as_float(totals.get("total"))
            if total_val is not None:
                grand_total = total_val
            else:
                total_sum = 0.0
                has_total = False
                for it in items:
                    qty = as_int(it.get("qty")) or 0
                    line_total = as_float(it.get("line_total"))
                    if line_total is None:
                        unit_price = as_float(it.get("final_price"))
                        if unit_price is None:
                            unit_price = as_float(it.get("price"))
                        if unit_price is None:
                            unit_price = as_float(it.get("base_price"))
                        if unit_price is None:
                            continue
                        line_total = float(unit_price) * float(qty)
                    total_sum += float(line_total)
                    has_total = True
                if has_total:
                    grand_total = round(total_sum, 2)
            if grand_total is not None:
                totals.setdefault("grand_total", grand_total)
                totals.setdefault("total", grand_total)

        discount_total = as_float(totals.get("discount_total"))
        if discount_total is None:
            discount_total = as_float(totals.get("total_discount"))
        if discount_total is None:
            discount_total = as_float(totals.get("discount"))
        if discount_total is None:
            subtotal_val = subtotal if subtotal is not None else as_float(totals.get("subtotal"))
            grand_total_val = grand_total if grand_total is not None else as_float(totals.get("grand_total"))
            if subtotal_val is not None and grand_total_val is not None:
                discount_total = max(float(subtotal_val) - float(grand_total_val), 0.0)
        if discount_total is not None:
            totals["discount_total"] = round(float(discount_total), 2)
            totals.setdefault("total_discount", round(float(discount_total), 2))
            totals.setdefault("discount", round(float(discount_total), 2))

        # Hide profit/margin fields from cart totals payloads.
        for key in (
            "profit",
            "profit_total",
            "total_profit",
            "profit_margin",
            "profit_margin_pct",
            "margin_pct",
        ):
            totals.pop(key, None)

        # profit = as_float(totals.get("profit"))
        # if profit is None:
        #     profit = as_float(totals.get("profit_total"))
        # if profit is None:
        #     profit_sum = 0.0
        #     has_profit = False
        #     for it in items:
        #         it_profit = as_float(it.get("profit"))
        #         if it_profit is None:
        #             it_profit_margin = as_float(it.get("profit_margin"))
        #             line_total = as_float(it.get("line_total"))
        #             if it_profit_margin is not None and line_total is not None:
        #                 it_profit = (float(it_profit_margin) / 100.0) * float(line_total)
        #         if it_profit is None:
        #             continue
        #         profit_sum += float(it_profit)
        #         has_profit = True
        #     if has_profit:
        #         profit = round(profit_sum, 2)

        # if totals.get("profit") is None:
        #     totals["profit"] = profit
        # if totals.get("profit_total") is None:
        #     totals["profit_total"] = profit

        # profit_margin = as_float(totals.get("profit_margin"))
        # if profit_margin is None:
        #     profit_margin = as_float(totals.get("profit_margin_pct"))
        # if profit_margin is None:
        #     profit_margin = as_float(totals.get("margin_pct"))
        # if profit_margin is None and profit is not None:
        #     subtotal_val = as_float(totals.get("subtotal"))
        #     if subtotal_val not in (None, 0):
        #         profit_margin = round((float(profit) / float(subtotal_val)) * 100.0, 2)

        # if totals.get("profit_margin") is None:
        #     totals["profit_margin"] = profit_margin
        # if totals.get("profit_margin_pct") is None:
        #     totals["profit_margin_pct"] = profit_margin
        # if totals.get("margin_pct") is None:
        #     totals["margin_pct"] = profit_margin

        return totals

    # -------------------------
    # validate required
    # -------------------------
    errors: List[dict] = []
    warnings: List[dict] = []

    def _missing_field_error(code: str, field: str, message: str, hint: str) -> dict:
        return {
            "code": code,
            "message": message,
            "missing_fields": [field],
            "hint": hint,
        }

    def _get_tenant_id() -> str:
        return get_tenant_id()

    def _get_agent_id() -> str:
        return get_agent_id()

    def _firestore_client() -> firestore.Client:
        """
        Create a Firestore client with a forgiving fallback:
        - Prefer GOOGLE_APPLICATION_CREDENTIALS
        - Else try the bundled agentflo_firestore.json (if present)
        - Else fall back to default ADC (may be Cloud Run / GCE metadata)
        """
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
        if not creds_path:
            bundled = Path(__file__).resolve().parent / "agentflo_firestore.json"
            if bundled.exists():
                creds_path = str(bundled)

        if creds_path and Path(creds_path).exists():
            creds = service_account.Credentials.from_service_account_file(creds_path)
            project = os.getenv("GOOGLE_CLOUD_PROJECT") or creds.project_id
            return firestore.Client(project=project, credentials=creds)

        # Fall back to default credentials (works on Cloud Run / GCE)
        return firestore.Client()

    tenant_id = _get_tenant_id()
    try:
        agent_id = _get_agent_id()
    except Exception:
        agent_id = None
    user_id = payload.get("user_id")
    store_id = payload.get("store_id")

    if not isinstance(tenant_id, str) or not tenant_id.strip():
        return {"ok": False, "cart": None, "errors": [
            _missing_field_error("MISSING_TENANT_ID", "tenant_id", "tenant_id is required", "Set TENANT_ID in the environment.")
        ], "warnings": []}
    if not isinstance(agent_id, str) or not agent_id.strip():
        return {"ok": False, "cart": None, "errors": [
            _missing_field_error("MISSING_AGENT_ID", "agent_id", "agent_id is required", "Set AGENT_ID in the environment.")
        ], "warnings": []}
    if not isinstance(user_id, str) or not user_id.strip():
        return {"ok": False, "cart": None, "errors": [
            _missing_field_error("MISSING_USER_ID", "user_id", "user_id is required", "Provide user_id as a non-empty string.")
        ], "warnings": []}
    if not isinstance(store_id, str) or not store_id.strip():
        return {"ok": False, "cart": None, "errors": [
            _missing_field_error("MISSING_STORE_ID", "store_id", "store_id is required", "Provide store_id (store code) as a non-empty string.")
        ], "warnings": []}

    tenant_id = tenant_id.strip()
    agent_id = agent_id.strip()
    user_id = user_id.strip()
    store_id = store_id.strip()

    customer_id = payload.get("customer_id")
    if not isinstance(customer_id, str) or not customer_id.strip():
        customer_id = store_id

    options = payload.get("options") or {}
    max_lines = as_int(options.get("max_lines")) or 200
    max_undo = as_int(options.get("max_undo")) or 20
    expected_version = options.get("expected_cart_version")
    expected_version = as_int(expected_version) if expected_version is not None else None
    timeout_s = as_int(options.get("sales_intel_timeout_s")) or 20

    operations = payload.get("operations") or []
    if not isinstance(operations, list):
        operations = []
        warnings.append({"code": "BAD_OPERATIONS_TYPE", "message": "operations must be a list; treated as empty."})

    def validate_operations_list(ops: List[Any]) -> Tuple[List[dict], List[dict]]:
        fatal: List[dict] = []
        warn: List[dict] = []
        allowed_ops = {"GET_CART", "ADD_ITEM", "SET_QTY", "REMOVE_ITEM", "CLEAR_CART", "UNDO"}

        for idx, op in enumerate(ops):
            if not isinstance(op, dict):
                fatal.append({
                    "code": "INVALID_OPERATION",
                    "message": "Operation must be an object/dict.",
                    "op_index": idx,
                    "hint": "Pass each operation as a JSON object like {'op': 'ADD_ITEM', 'sku_code': '...', 'qty': 2}."
                })
                continue

            op_type = str(op.get("op") or "").strip().upper()
            if not op_type:
                fatal.append({
                    "code": "MISSING_OP_TYPE",
                    "message": "Operation is missing 'op' field.",
                    "op_index": idx,
                    "missing_fields": ["op"],
                    "hint": "Set op to one of GET_CART, ADD_ITEM, SET_QTY, REMOVE_ITEM, CLEAR_CART, UNDO."
                })
                continue

            if op_type not in allowed_ops:
                fatal.append({
                    "code": "UNKNOWN_OP",
                    "message": f"Unsupported op: {op_type}",
                    "op_index": idx,
                    "op_type": op_type,
                    "hint": "Use one of GET_CART, ADD_ITEM, SET_QTY, REMOVE_ITEM, CLEAR_CART, UNDO."
                })
                continue

            if op_type in {"ADD_ITEM", "SET_QTY", "REMOVE_ITEM"}:
                sku = get_sku(op)
                if not sku:
                    # Recovery: if sku/name are swapped, use name as sku when it looks code-like.
                    raw_sku = pick_first(op, ["sku_code", "sku", "skucode", "sku_id", "item_number", "variant_code", "id"])
                    name_hint = pick_first(op, ["name", "official_name", "product_name", "sku_name", "description"])
                    if _looks_like_sku_code(name_hint):
                        op["sku_code"] = str(name_hint).strip()
                        if raw_sku and (not _is_meaningful_name(op.get("name"))):
                            op["name"] = str(raw_sku).strip()
                        warn.append(
                            {
                                "code": "SKU_NAME_SWAPPED_RECOVERED",
                                "message": "Recovered operation where sku/name appeared swapped.",
                                "op_index": idx,
                                "op_type": op_type,
                            }
                        )
                        sku = get_sku(op)

                if not sku:
                    raw_sku = pick_first(op, ["sku_code", "sku", "skucode", "sku_id", "item_number", "variant_code", "id"])
                    raw_sku_s = str(raw_sku).strip() if raw_sku is not None else ""
                    if raw_sku_s and any(ch.isspace() for ch in raw_sku_s):
                        fatal.append(
                            {
                                "code": "INVALID_SKU_FORMAT",
                                "message": "sku_code looks like a product name (contains spaces).",
                                "op_index": idx,
                                "op_type": op_type,
                                "provided_sku": raw_sku_s,
                                "hint": "Pass the actual SKU code (e.g. SKU00059), not the item name.",
                            }
                        )
                        continue
                    fatal.append({
                        "code": "MISSING_SKU",
                        "message": "sku_code (or sku) is required.",
                        "op_index": idx,
                        "op_type": op_type,
                        "missing_fields": ["sku_code"],
                        "hint": "Provide sku_code/sku string for the item."
                    })
                    continue

            if op_type == "ADD_ITEM":
                qty_val = as_int(op.get("qty") if op.get("qty") is not None else op.get("quantity"))
                if qty_val is None or qty_val <= 0:
                    fatal.append({
                        "code": "INVALID_QTY",
                        "message": "qty must be > 0 for ADD_ITEM.",
                        "op_index": idx,
                        "op_type": op_type,
                        "missing_fields": ["qty"] if qty_val is None else [],
                        "hint": "Provide a positive integer qty for ADD_ITEM."
                    })
                merge_strategy = str(op.get("merge_strategy") or "").strip().upper()
                if merge_strategy == "REPLACE" and not as_bool(op.get("replace_confirmed")):
                    warn.append(
                        {
                            "code": "REPLACE_NOT_CONFIRMED_DEFAULT_INCREMENT",
                            "message": "merge_strategy=REPLACE ignored because replace_confirmed was not true.",
                            "op_index": idx,
                            "op_type": op_type,
                        }
                    )

            if op_type == "SET_QTY":
                qty_val = as_int(op.get("qty") if op.get("qty") is not None else op.get("quantity"))
                if qty_val is None or qty_val < 0:
                    fatal.append({
                        "code": "INVALID_QTY",
                        "message": "qty must be >= 0 for SET_QTY.",
                        "op_index": idx,
                        "op_type": op_type,
                        "missing_fields": ["qty"] if qty_val is None else [],
                        "hint": "Provide a non-negative integer qty for SET_QTY (0 removes the item)."
                    })

        return fatal, warn

    sales_intel_token = _clean_env_value(os.getenv("API_JWT_TOKEN"))
    api_url = _resolve_sales_intel_endpoint()

    validation_errors, validation_warnings = validate_operations_list(operations)
    if validation_warnings:
        warnings.extend(validation_warnings)
    if validation_errors:
        errors.extend(validation_errors)
        return {"ok": False, "cart": None, "errors": errors, "warnings": warnings}

    # -------------------------
    # firestore
    # -------------------------
    try:
        db = _firestore_client()
    except Exception as e:
        errors.append({
            "code": "FIRESTORE_INIT_ERROR",
            "message": "Unable to initialize Firestore client.",
            "detail": repr(e),
        })
        return {"ok": False, "cart": None, "errors": errors, "warnings": warnings}
    user_ref = user_root(db, user_id, tenant_id=tenant_id, agent_id=agent_id)
    undo_pushed_for_request = False  # ensure a single undo snapshot per request

    # Resolve customer_id for Sales Intelligence pricing. Promotions are often customer/store-specific,
    # so avoid placeholder IDs like UNKNOWN/phone fallback whenever possible.
    candidate_customer_id = str(customer_id or "").strip() or store_id
    if is_placeholder_store_id(candidate_customer_id) or candidate_customer_id == user_id:
        try:
            user_snap = user_ref.get()
            if user_snap.exists:
                user_data = user_snap.to_dict() or {}
                cached_store = (
                    user_data.get("store_id")
                    or (user_data.get("order_drafts") or {}).get("store_id")
                )
                cached_store_s = str(cached_store or "").strip()
                if cached_store_s and not is_placeholder_store_id(cached_store_s):
                    candidate_customer_id = cached_store_s
        except Exception as e:
            logger.warning("cart.customer_id.cache_lookup_failed", user_id=user_id, error=str(e))

    if is_placeholder_store_id(candidate_customer_id) or candidate_customer_id == user_id:
        try:
            from agents.tools.api_tools import search_customer_by_phone, unwrap_tool_response

            raw_customer = search_customer_by_phone(user_id)
            ok, payload_data, err = unwrap_tool_response(raw_customer, system_name="search_customer_by_phone")
            if ok and isinstance(payload_data, dict):
                data = payload_data.get("data", {}) or {}
                resolved_store = (
                    (data.get("additional_info") or {}).get("storecode")
                    or data.get("storecode")
                )
                resolved_store_s = str(resolved_store or "").strip()
                if resolved_store_s and not is_placeholder_store_id(resolved_store_s):
                    candidate_customer_id = resolved_store_s
                    try:
                        user_ref.set({"store_id": resolved_store_s}, merge=True)
                    except Exception:
                        pass
            elif err:
                logger.warning("cart.customer_id.api_lookup_failed", user_id=user_id, error=err)
        except Exception as e:
            logger.warning("cart.customer_id.api_lookup_exception", user_id=user_id, error=str(e))

    customer_id = str(candidate_customer_id or store_id).strip() or store_id
    if is_placeholder_store_id(customer_id):
        warnings.append(
            {
                "code": "SUSPECT_CUSTOMER_ID",
                "message": "customer_id resolved to a placeholder value; promotions may be incomplete.",
                "customer_id": customer_id,
            }
        )

    # Keep cart path aligned with resolved customer/store context when incoming store_id is placeholder-like.
    if (is_placeholder_store_id(store_id) or store_id == user_id) and not is_placeholder_store_id(customer_id):
        if store_id != customer_id:
            warnings.append(
                {
                    "code": "STORE_ID_CANONICALIZED",
                    "message": "store_id normalized to resolved customer store for consistent pricing/promotions.",
                    "from_store_id": store_id,
                    "to_store_id": customer_id,
                }
            )
            logger.info(
                "cart.store_id_canonicalized",
                user_id=user_id,
                from_store_id=store_id,
                to_store_id=customer_id,
            )
        store_id = customer_id

    cart_ref = user_ref.collection("store_carts").document(store_id)
    undo_col = cart_ref.collection("undo")

    def clear_pending_recommendations() -> None:
        try:
            user_ref.set({PENDING_RECOMMENDATIONS_FIELD: firestore.DELETE_FIELD}, merge=True)
        except Exception as e:
            logger.warning("cart.pending_recommendations.clear_failed", user_id=user_id, error=str(e))

    # -------------------------
    # Transaction per operation (safe + simple)
    # -------------------------
    def apply_one_op(op: dict, enforce_expected: bool) -> Tuple[Optional[dict], Optional[dict]]:
        """
        Returns (cart, err) where err is fatal.
        """
        nonlocal undo_pushed_for_request
        if not isinstance(op, dict):
            return None, {"code": "INVALID_OPERATION", "message": "Operation must be an object/dict."}

        op_type = str(op.get("op") or "").strip().upper()
        mutating_ops = {"ADD_ITEM", "SET_QTY", "REMOVE_ITEM", "CLEAR_CART", "UNDO"}
        is_mutating = op_type in mutating_ops and op_type != "GET_CART"

        @firestore.transactional
        def _txn(transaction: firestore.Transaction) -> dict:
            snap = cart_ref.get(transaction=transaction)
            cart_doc = snap.to_dict() if snap.exists else None
            cart = normalize_cart_doc(tenant_id, user_id, store_id, cart_doc)
            undo_pushed_now = False

            if enforce_expected and expected_version is not None and cart.get("cart_version") != expected_version:
                return {"conflict": True, "cart": cart, "undo_pushed": undo_pushed_now}

            before_snapshot = {
                "tenant_id": tenant_id,
                "user_id": user_id,
                "store_id": store_id,
                "items": copy.deepcopy(cart.get("items") or []),
                "cart_version": cart.get("cart_version"),
                "last_updated": cart.get("last_updated"),
                "basket": copy.deepcopy(cart.get("basket")),
                "totals": copy.deepcopy(cart.get("totals")),
                "explanations": copy.deepcopy(cart.get("explanations")),
                "objective_used": cart.get("objective_used"),
                "basket_id": cart.get("basket_id"),
            }

            # helpers inside txn
            def push_undo():
                nonlocal undo_pushed_now
                if undo_pushed_for_request or undo_pushed_now:
                    return
                undo_id = uuid.uuid4().hex
                undo_ref = undo_col.document(undo_id)
                transaction.set(undo_ref, {
                    "ts_iso": now_iso(),
                    "op": op,
                    "snapshot": before_snapshot,
                })
                stack = list(cart.get("undo_stack") or [])
                stack.append({"id": undo_id, "ts": now_iso()})
                # trim oldest strictly (no queries needed)
                while len(stack) > max_undo:
                    oldest = stack.pop(0)
                    if isinstance(oldest, dict) and oldest.get("id"):
                        transaction.delete(undo_col.document(oldest["id"]))
                cart["undo_stack"] = stack
                undo_pushed_now = True

            # Apply op
            if op_type == "GET_CART":
                pass

            elif op_type == "CLEAR_CART":
                push_undo()
                cart["items"] = []
                cart["totals"] = None
                basket = cart.get("basket")
                if isinstance(basket, dict):
                    basket["items"] = []
                    basket["totals"] = None
                else:
                    basket = {"items": [], "totals": None}
                cart["basket"] = basket

            elif op_type == "UNDO":
                stack = list(cart.get("undo_stack") or [])
                if not stack:
                    return {"error": {"code": "NOTHING_TO_UNDO", "message": "No previous change to undo."}, "cart": cart, "undo_pushed": undo_pushed_now}
                last = stack.pop()  # most recent
                undo_id = last.get("id") if isinstance(last, dict) else None
                if not undo_id:
                    return {"error": {"code": "BAD_UNDO_POINTER", "message": "Undo pointer missing id."}, "cart": cart, "undo_pushed": undo_pushed_now}

                undo_snap = undo_col.document(undo_id).get(transaction=transaction)
                if not undo_snap.exists:
                    return {"error": {"code": "UNDO_SNAPSHOT_NOT_FOUND", "message": "Undo snapshot not found."}, "cart": cart, "undo_pushed": undo_pushed_now}
                data = undo_snap.to_dict() or {}
                snap_cart = data.get("snapshot")
                if not isinstance(snap_cart, dict):
                    return {"error": {"code": "BAD_UNDO_SNAPSHOT", "message": "Undo snapshot corrupted."}, "cart": cart, "undo_pushed": undo_pushed_now}

                cart = normalize_cart_doc(tenant_id, user_id, store_id, snap_cart)
                cart["undo_stack"] = stack
                transaction.delete(undo_col.document(undo_id))

            elif op_type in ("ADD_ITEM", "SET_QTY", "REMOVE_ITEM"):
                sku = get_sku(op)
                if not sku:
                    return {"error": {"code": "MISSING_SKU", "message": "sku_code (or sku) is required."}, "cart": cart, "undo_pushed": undo_pushed_now}

                idx = find_idx(cart, sku)

                if op_type == "REMOVE_ITEM":
                    if idx is None:
                        return {"error": {"code": "NOT_IN_CART", "message": "SKU not in cart.", "sku": sku}, "cart": cart, "undo_pushed": undo_pushed_now}
                    push_undo()
                    cart["items"].pop(idx)

                elif op_type == "SET_QTY":
                    qty = as_int(op.get("qty") if op.get("qty") is not None else op.get("quantity"))
                    if qty is None or qty < 0:
                        return {"error": {"code": "INVALID_QTY", "message": "qty must be >= 0.", "sku": sku}, "cart": cart, "undo_pushed": undo_pushed_now}
                    if idx is None:
                        return {"error": {"code": "NOT_IN_CART", "message": "SKU not in cart.", "sku": sku}, "cart": cart, "undo_pushed": undo_pushed_now}
                    push_undo()
                    if qty == 0:
                        cart["items"].pop(idx)
                    else:
                        cart["items"][idx]["qty"] = qty

                elif op_type == "ADD_ITEM":
                    qty = as_int(op.get("qty") if op.get("qty") is not None else op.get("quantity"))
                    if qty is None or qty <= 0:
                        return {"error": {"code": "INVALID_QTY", "message": "qty must be > 0.", "sku": sku}, "cart": cart, "undo_pushed": undo_pushed_now}

                    # Default is incremental. REPLACE is allowed only when explicitly confirmed.
                    merge_strategy = str(op.get("merge_strategy") or "").strip().upper()
                    replace_confirmed = as_bool(op.get("replace_confirmed"))
                    use_replace = merge_strategy == "REPLACE" and replace_confirmed
                    if merge_strategy == "REPLACE" and not replace_confirmed:
                        logger.info(
                            "cart.add_item.replace_not_confirmed_default_increment",
                            sku=sku,
                        )
                    elif merge_strategy not in ("", "INCREMENT", "REPLACE"):
                        logger.info(
                            "cart.add_item.unknown_merge_strategy_default_increment",
                            requested_merge_strategy=op.get("merge_strategy"),
                            sku=sku,
                        )
                    name = pick_name(op)
                    product_retailer_id = pick_product_retailer_id(op)

                    metadata = {}
                    if name is not None:
                        metadata["name"] = name
                    if product_retailer_id is not None:
                        metadata["product_retailer_id"] = product_retailer_id

                    numeric_aliases = {
                        "price": [
                            "price",
                            "unit_price",
                            "final_price",
                            "base_price",
                            "consumer_price",
                            "list_price",
                            "mrp",
                            "total_buy_price_virtual_pack",
                        ],
                        "base_price": ["base_price", "consumer_price", "list_price", "mrp", "unit_price", "price"],
                        "final_price": [
                            "final_price",
                            "discounted_price",
                            "unit_price_final",
                            "unit_price",
                            "price",
                            "total_buy_price_virtual_pack",
                        ],
                        "discount_value": ["discount_value", "unit_discount", "discount"],
                        "discount_value_line": ["discount_value_line", "line_discount"],
                        "discount_pct": ["discount_pct", "discountvalue", "discount_percentage"],
                        "line_total": ["line_total", "linetotal", "lineamount", "line_total_amount"],
                        # "profit": ["profit", "line_profit"],
                        # "profit_margin": ["profit_margin", "profit_margin_pct", "margin_pct"],
                        "old_line_total": ["old_line_total", "pre_discount_line_total"],
                    }
                    for target_field, keys in numeric_aliases.items():
                        val = pick_price(op, keys)
                        if val is not None:
                            metadata[target_field] = val

                    if metadata.get("base_price") is None and metadata.get("final_price") is not None:
                        metadata["base_price"] = metadata["final_price"]
                    if metadata.get("final_price") is None and metadata.get("base_price") is not None:
                        metadata["final_price"] = metadata["base_price"]
                    unit_discount = as_float(metadata.get("discount_value"))
                    line_discount = as_float(metadata.get("discount_value_line"))
                    if unit_discount is None and line_discount is not None and qty > 0:
                        metadata["discount_value"] = round(float(line_discount) / float(qty), 4)
                    if line_discount is None and unit_discount is not None and qty > 0:
                        metadata["discount_value_line"] = round(float(unit_discount) * float(qty), 2)

                    for field in ["tags"]:
                        if op.get(field) is not None:
                            metadata[field] = op.get(field)
                    reason = pick_first(op, ["primary_reason", "reason"])
                    if reason is not None:
                        metadata["primary_reason"] = reason

                    # ============ ADD THIS: ENRICH METADATA WITH API DETAILS ============
                    metadata = _enrich_item_with_product_details(sku, metadata)
                    # ============ END ENRICHMENT ============

                    push_undo()

                    if idx is None:
                        new_item = {
                            "sku_code": sku,
                            "sku": sku,
                            "qty": qty,
                        }
                        new_item.update(metadata)
                        cart["items"].append(new_item)
                    else:
                        if use_replace:
                            cart["items"][idx]["qty"] = qty
                        else:
                            cart["items"][idx]["qty"] = (as_int(cart["items"][idx].get("qty")) or 0) + qty
                        for key, val in metadata.items():
                            cart["items"][idx][key] = val

            else:
                return {"error": {"code": "UNKNOWN_OP", "message": f"Unsupported op: {op_type}"}, "cart": cart, "undo_pushed": undo_pushed_now}

            # enforce max_lines only on mutating ops
            if is_mutating and len(cart["items"]) > max_lines:
                cart["items"] = cart["items"][:max_lines]
                # keep as warning outside transaction (returned result)
                cart.setdefault("_tool_warnings", [])
                cart["_tool_warnings"].append({"code": "MAX_LINES_TRUNCATED", "message": f"Cart exceeded max_lines={max_lines}; truncated."})

            # bump version + persist only on mutating ops (including successful undo/clear/add/set/remove)
            if is_mutating:
                cart["cart_version"] = (as_int(cart.get("cart_version")) or 0) + 1
                cart["last_updated"] = now_iso()
                transaction.set(cart_ref, cart, merge=True)
            return {"conflict": False, "cart": cart, "undo_pushed": undo_pushed_now}

        try:
            result = _txn(db.transaction())
        except Exception as e:
            return None, {"code": "FIRESTORE_TXN_ERROR", "message": "Firestore transaction failed.", "detail": repr(e)}

        if result.get("undo_pushed"):
            undo_pushed_for_request = True
        if result.get("conflict"):
            return result.get("cart"), {"code": "CART_VERSION_CONFLICT", "message": "Cart changed. Re-read and retry with expected_cart_version."}

        cart_out = result.get("cart")
        if isinstance(cart_out, dict) and cart_out.get("_tool_warnings"):
            # bubble warnings (remove from stored doc too)
            for w in cart_out.get("_tool_warnings", []):
                warnings.append(w)
            # clean in DB (optional; avoid extra write here)
            cart_out.pop("_tool_warnings", None)

        if result.get("error"):
            return cart_out, result["error"]

        return cart_out, None

    # apply operations (expected_version checked only on first mutating op)
    cart_state: Optional[dict] = None
    enforced = False
    any_mutation = False
    successful_mutation = False

    if not operations:
        # default behavior: GET_CART (no-op)
        operations = [{"op": "GET_CART"}]

    op_types_for_pruning = [
        str(op.get("op") or "").strip().upper()
        for op in operations
        if isinstance(op, dict)
    ]
    prune_cart_output = bool(op_types_for_pruning) and all(
        op_type == "GET_CART" for op_type in op_types_for_pruning
    )

    def _prune_cart(cart_obj: Optional[dict]) -> Optional[dict]:
        if not prune_cart_output or not isinstance(cart_obj, dict):
            return cart_obj
        basket = cart_obj.get("basket")
        basket_items = []
        basket_totals = None
        if isinstance(basket, dict):
            if isinstance(basket.get("items"), list):
                basket_items = basket.get("items")
            if isinstance(basket.get("totals"), dict):
                basket_totals = basket.get("totals")

        items = cart_obj.get("items") if isinstance(cart_obj.get("items"), list) else []
        cart_totals = cart_obj.get("totals") if isinstance(cart_obj.get("totals"), dict) else {}

        def _has_enriched_fields(seq: list) -> bool:
            for it in seq:
                if not isinstance(it, dict):
                    continue
                name_val = pick_name(it)
                # Treat placeholder "item" (or empty) as not enriched
                has_meaningful_name = _is_meaningful_name(name_val)
                has_any_price = any(
                    pick_price(it, [k]) is not None
                    for k in (
                        "line_total",
                        "linetotal",
                        "lineamount",
                        "line_total_amount",
                        "final_price",
                        "discounted_price",
                        "unit_price_final",
                        "base_price",
                        "unit_price",
                        "price",
                    )
                )
                if has_any_price or has_meaningful_name:
                    return True
            return False

        def _merge_qty_into_priced(priced_items: list, source_cart_items: list) -> list:
            """
            Pricing snapshot (basket_items) often lacks qty; backfill from cart items.
            """
            qty_by_sku = {}
            name_by_sku = {}

            for it in source_cart_items:
                if not isinstance(it, dict):
                    continue
                sku_key_raw = get_sku(it)
                if not sku_key_raw:
                    continue
                sku_key = sku_key_raw.strip().lower()
                qty_val = it.get("qty") or it.get("quantity")
                if qty_val is not None and sku_key not in qty_by_sku:
                    qty_by_sku[sku_key] = qty_val
                # Record meaningful name to preserve catalog labels even when pricing is missing names
                name_val = pick_name(it)
                if _is_meaningful_name(name_val) and sku_key not in name_by_sku:
                    name_by_sku[sku_key] = name_val.strip()

            merged = []
            for pit in priced_items:
                if not isinstance(pit, dict):
                    continue
                merged_item = dict(pit)
                sku_key_raw = get_sku(merged_item)
                if sku_key_raw:
                    sku_key = sku_key_raw.strip().lower()
                    if merged_item.get("sku_code") is None:
                        merged_item["sku_code"] = sku_key_raw
                    if merged_item.get("qty") in (None, 0) and qty_by_sku.get(sku_key) is not None:
                        merged_item["qty"] = qty_by_sku[sku_key]
                    # Prefer the original cart name if pricing snapshot has placeholder/empty name
                    cart_name = name_by_sku.get(sku_key)
                    if cart_name:
                        pit_name = pick_name(merged_item)
                        has_meaningful_priced_name = _is_meaningful_name(pit_name)
                        if not has_meaningful_priced_name:
                            merged_item["name"] = cart_name
                merged.append(merged_item)
            return merged

        def _filter_priced_items_to_cart(priced_items: list, source_cart_items: list) -> list:
            """
            Keep only pricing rows that map to explicit cart SKUs.
            Some pricing responses can include recommended extras; those must never
            appear as cart lines unless user explicitly added them.
            """
            allowed_skus = set()
            for it in source_cart_items:
                if not isinstance(it, dict):
                    continue
                sku_raw = get_sku(it)
                if isinstance(sku_raw, str) and sku_raw.strip():
                    allowed_skus.add(sku_raw.strip().lower())

            if not allowed_skus:
                return []

            filtered = []
            dropped = 0
            for pit in priced_items:
                if not isinstance(pit, dict):
                    continue
                sku_raw = get_sku(pit)
                sku_key = sku_raw.strip().lower() if isinstance(sku_raw, str) and sku_raw.strip() else None
                if sku_key and sku_key in allowed_skus:
                    filtered.append(pit)
                else:
                    dropped += 1

            if dropped:
                logger.warning(
                    "cart.get_cart.filtered_non_cart_basket_items",
                    dropped_count=dropped,
                    allowed_count=len(allowed_skus),
                )
            return filtered

        def _canonicalize_item_for_display(raw_item: dict) -> dict:
            out_item = dict(raw_item)
            for key in ("profit", "line_profit", "profit_margin", "profit_margin_pct", "margin_pct"):
                out_item.pop(key, None)
            sku_val = get_sku(raw_item)
            if sku_val:
                out_item["sku_code"] = sku_val
                out_item.setdefault("sku", sku_val)

            qty_val = as_int(pick_first(raw_item, ["qty", "quantity", "forecast_qty"]))
            if qty_val is not None:
                out_item["qty"] = qty_val

            name_val = pick_name(raw_item)
            if not name_val:
                # Try raw name fields that pick_name may have filtered (e.g. "SKU 36" placeholders)
                raw_name = pick_first(raw_item, ["official_name", "name", "product_name", "sku_name", "description"])
                if isinstance(raw_name, str) and raw_name.strip() and raw_name.strip().lower() not in {"item", "items"}:
                    name_val = raw_name.strip()
            if not name_val and sku_val:
                # Last resort: use the SKU code itself — always better than the generic "item" fallback
                name_val = sku_val
            if name_val:
                out_item["name"] = name_val

            base_price = pick_price(
                raw_item,
                ["base_price", "consumer_price", "list_price", "mrp", "unit_price", "price"],
            )
            final_price = pick_price(
                raw_item,
                ["final_price", "discounted_price", "unit_price_final", "unit_price", "price", "total_buy_price_virtual_pack"],
            )
            line_total = pick_price(raw_item, ["line_total", "linetotal", "lineamount", "line_total_amount"])
            unit_discount = pick_price(raw_item, ["discount_value", "unit_discount"])
            line_discount = pick_price(raw_item, ["discount_value_line", "line_discount"])
            if unit_discount is None:
                unit_discount = pick_price(raw_item, ["discount"])
            if unit_discount is None and line_discount is not None and qty_val is not None and qty_val > 0:
                unit_discount = round(float(line_discount) / float(qty_val), 4)
            if line_discount is None and unit_discount is not None and qty_val is not None and qty_val > 0:
                line_discount = round(float(unit_discount) * float(qty_val), 2)
            discount_pct = pick_price(raw_item, ["discount_pct", "discountvalue", "discount_percentage"])

            if base_price is None and final_price is not None:
                base_price = final_price
            if final_price is None and base_price is not None:
                final_price = base_price
            if line_total is None and final_price is not None and qty_val is not None:
                line_total = round(float(final_price) * float(qty_val), 2)

            if base_price is not None:
                out_item["base_price"] = base_price
            if final_price is not None:
                out_item["final_price"] = final_price
            if line_total is not None:
                out_item["line_total"] = line_total
            if unit_discount is not None:
                out_item["discount_value"] = unit_discount
            if line_discount is not None:
                out_item["discount_value_line"] = line_discount
            if discount_pct is not None:
                out_item["discount_pct"] = discount_pct

            return out_item

        # Prefer pricing snapshot for cart SKUs only; never surface non-cart recommendations.
        filtered_basket_items = _filter_priced_items_to_cart(basket_items, items) if basket_items else []
        selected_items = _merge_qty_into_priced(filtered_basket_items, items) if filtered_basket_items else items
        if not selected_items:
            selected_items = items
        selected_items = [
            _canonicalize_item_for_display(it)
            for it in selected_items
            if isinstance(it, dict)
        ]

        totals_out = {}
        for src in (basket_totals, cart_totals):
            if isinstance(src, dict):
                for k, v in src.items():
                    if totals_out.get(k) is None and v is not None:
                        totals_out[k] = v
        for key in (
            "profit",
            "profit_total",
            "total_profit",
            "profit_margin",
            "profit_margin_pct",
            "margin_pct",
        ):
            totals_out.pop(key, None)

        # Backfill totals from selected line items when upstream totals are missing.
        computed_grand = 0.0
        computed_subtotal = 0.0
        has_grand = False
        has_subtotal = False
        for it in selected_items:
            if not isinstance(it, dict):
                continue
            qty = as_int(pick_first(it, ["qty", "quantity", "forecast_qty"])) or 0
            if qty <= 0:
                continue

            line_total = pick_price(it, ["line_total", "linetotal", "lineamount", "line_total_amount"])
            if line_total is None:
                final_price = pick_price(
                    it,
                    ["final_price", "discounted_price", "unit_price_final", "unit_price", "price", "total_buy_price_virtual_pack"],
                )
                if final_price is not None:
                    line_total = round(float(final_price) * float(qty), 2)
            if line_total is not None:
                computed_grand += float(line_total)
                has_grand = True

            base_price = pick_price(
                it,
                ["base_price", "consumer_price", "list_price", "mrp", "unit_price", "price", "final_price"],
            )
            if base_price is not None:
                computed_subtotal += float(base_price) * float(qty)
                has_subtotal = True

        if totals_out.get("grand_total") is None and has_grand:
            totals_out["grand_total"] = round(computed_grand, 2)
        if totals_out.get("total") is None and has_grand:
            totals_out["total"] = round(computed_grand, 2)
        if totals_out.get("subtotal") is None:
            if has_subtotal:
                totals_out["subtotal"] = round(computed_subtotal, 2)
            elif has_grand:
                totals_out["subtotal"] = round(computed_grand, 2)

        subtotal_f = as_float(totals_out.get("subtotal"))
        grand_f = as_float(totals_out.get("grand_total") if totals_out.get("grand_total") is not None else totals_out.get("total"))
        if totals_out.get("discount_total") is None and subtotal_f is not None and grand_f is not None:
            discount_val = round(max(float(subtotal_f) - float(grand_f), 0.0), 2)
            totals_out["discount_total"] = discount_val
            if totals_out.get("total_discount") is None:
                totals_out["total_discount"] = discount_val
            if totals_out.get("discount") is None:
                totals_out["discount"] = discount_val

        return {
            "items": selected_items,
            "totals": totals_out or None,
            "user_id": cart_obj.get("user_id"),
        }

    for op in operations:
        op_type = str(op.get("op") or "").strip().upper() if isinstance(op, dict) else ""
        is_mutating = op_type in {"ADD_ITEM", "SET_QTY", "REMOVE_ITEM", "CLEAR_CART", "UNDO"}
        enforce_expected_here = (expected_version is not None) and (not enforced) and is_mutating
        cart_state, err = apply_one_op(op, enforce_expected_here)
        if enforce_expected_here:
            enforced = True
        if is_mutating:
            any_mutation = True
        if err:
            errors.append(err)
            # For WhatsApp UX, you may prefer "stop on first fatal error"
            # We'll stop to avoid confusing partial chains.
            break
        if is_mutating:
            successful_mutation = True

    if not cart_state:
        # last resort read
        snap = cart_ref.get()
        cart_state = normalize_cart_doc(tenant_id, user_id, store_id, snap.to_dict() if snap.exists else None)

    if successful_mutation:
        clear_pending_recommendations()

    # -------------------------
    # Pricing phase (external call), then conditional write back
    # -------------------------
    if not any_mutation:
        # Pure reads should not create undo traces or writes; return current cart
        return {"ok": len(errors) == 0, "cart": _prune_cart(cart_state), "errors": errors, "warnings": warnings}

    cart_version_before_pricing = as_int(cart_state.get("cart_version")) or 0
    items_for_api = build_items_for_pricing(cart_state)

    basket = {}
    basket_items = None
    totals = None
    explanations = None
    objective_used = "CART_ITEMS"
    basket_id = None

    # Primary pricing path (Sales Intelligence API)
    primary_available = (isinstance(api_url, str) and api_url.strip()) and (isinstance(sales_intel_token, str) and sales_intel_token.strip())
    if primary_available:
        si_resp, si_err = call_sales_intel_cart_items_api(
            tenant_id=tenant_id,
            agent_id=agent_id,
            customer_id=customer_id,
            items=items_for_api,
            timeout_s=timeout_s,
        )

        if si_err:
            warnings.append(si_err)
        else:
            basket = (si_resp.get("basket") or {}) if isinstance(si_resp, dict) else {}
            basket_items = basket.get("items") if isinstance(basket.get("items"), list) else []
            totals = basket.get("totals") if isinstance(basket.get("totals"), dict) else None
            explanations = basket.get("explanations") if isinstance(basket.get("explanations"), list) else None
            objective_used = si_resp.get("objective_used") if isinstance(si_resp, dict) else "CART_ITEMS"
            basket_id = basket.get("basket_id") if isinstance(basket, dict) else None
    else:
        warnings.append({
            "code": "MISSING_SALES_INTEL_CREDS",
            "message": "sales_intel_cart_items_url or token missing; attempting fallback pricing.",
            "hint": "Set SALES_INTELLIGENSE_ENDPOINT/SALES_INTELLIGENCE_ENDPOINT and API_JWT_TOKEN for full pricing."
        })

    # Fallback to optimised basket helper if primary pricing unavailable/failed
    if not basket_items:
        try:
            from agents.tools.order_draft_tools import fetch_optimised_basket  # lazy import to avoid circulars

            fallback_resp = fetch_optimised_basket(store_id=customer_id, items=cart_state.get("items") or cart_state.get("skus") or [])
            fallback_items = list((fallback_resp.get("items_by_sku") or {}).values()) if isinstance(fallback_resp, dict) else []
            if fallback_items:
                basket_items = fallback_items
                totals = fallback_resp.get("basket_totals") if isinstance(fallback_resp, dict) else None
                meta = fallback_resp.get("basket_meta") if isinstance(fallback_resp, dict) else {}
                objective_used = (meta.get("objective") if isinstance(meta, dict) else None) or objective_used
                basket_id = (meta.get("basket_id") if isinstance(meta, dict) else None) or basket_id
                basket = {"items": basket_items, "totals": totals, "basket_id": basket_id, "objective": objective_used}
        except Exception as e:
            warnings.append({
                "code": "FALLBACK_PRICING_ERROR",
                "message": "Fallback pricing via fetch_optimised_basket failed.",
                "detail": repr(e),
            })

    # DEMO-only local pricing fallback (YTL products.json) when all external pricing fails
    if not basket_items:
        is_demo = (
            str(store_id or "").strip().upper() == "DEMO"
            or str(customer_id or "").strip().upper() == "DEMO"
        )
        if is_demo:
            local_items, local_totals = _build_local_pricing_from_products(cart_state or {})
            if local_items:
                basket_items = local_items
                totals = local_totals
                basket = {
                    "items": basket_items,
                    "totals": totals,
                    "basket_id": basket_id,
                    "objective": objective_used,
                }
                logger.info(
                    "cart.local_pricing.applied",
                    user_id=user_id,
                    store_id=store_id,
                    customer_id=customer_id,
                )

    if not basket_items:
        warnings.append({
            "code": "PRICING_NOT_AVAILABLE",
            "message": "Pricing could not be fetched; returning unpriced cart.",
            "customer_id_used": customer_id,   # logged so GCP traces reveal which store_code failed
        })
        logger.warning(
            "cart.pricing_not_available",
            user_id=user_id,
            customer_id=customer_id,
            store_id=store_id,
        )
        return {"ok": len(errors) == 0, "cart": _prune_cart(cart_state), "errors": errors, "warnings": warnings}

    merged_items = merge_pricing_into_cart_items(cart_state.get("items") or [], basket_items)
    totals = ensure_totals_fields(totals, merged_items)
    if isinstance(basket, dict):
        basket["totals"] = totals

    # write priced snapshot only if cart_version still matches
    @firestore.transactional
    def write_pricing_txn(transaction: firestore.Transaction) -> dict:
        snap = cart_ref.get(transaction=transaction)
        current = normalize_cart_doc(tenant_id, user_id, store_id, snap.to_dict() if snap.exists else None)

        if (as_int(current.get("cart_version")) or 0) != cart_version_before_pricing:
            return {"conflict": True, "cart": current}

        current["items"] = merged_items
        current["basket"] = basket
        current["totals"] = totals
        current["explanations"] = explanations
        current["objective_used"] = objective_used
        current["basket_id"] = basket_id
        current["last_updated"] = now_iso()

        transaction.set(cart_ref, current, merge=True)
        return {"conflict": False, "cart": current}

    try:
        priced_result = write_pricing_txn(db.transaction())
    except Exception as e:
        warnings.append({
            "code": "PRICING_WRITE_FAILED",
            "message": "Failed to persist priced cart snapshot.",
            "detail": repr(e),
        })
        return {"ok": len(errors) == 0, "cart": cart_state, "errors": errors, "warnings": warnings}
    if priced_result.get("conflict"):
        # someone changed cart while pricing was computed; don't overwrite
        warnings.append({
            "code": "CART_CHANGED_DURING_PRICING",
            "message": "Cart changed while pricing was computed. Re-run GET_CART to get fresh priced totals."
        })
        return {"ok": len(errors) == 0, "cart": priced_result.get("cart"), "errors": errors, "warnings": warnings}

    return {"ok": len(errors) == 0, "cart": priced_result.get("cart"), "errors": errors, "warnings": warnings}
