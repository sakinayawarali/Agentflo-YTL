
import datetime
import time
import json
import os
import re
import traceback
from typing import Optional, Tuple, Any, List
from dotenv import load_dotenv
from google.cloud import firestore
import requests  
from utils.logging import logger, debug_enabled
from agents.helpers.billing_logger import (
    TENANT_ID,
    generate_billing_event_v2,
    send_billing_event_fire_and_forget,
)
from google.adk.agents import LlmAgent
from google.adk.tools.function_tool import FunctionTool  # <-- This is the correct manual tool class
from pydantic import BaseModel, Field
from google.adk.agents.llm_agent import LlmRequest
from google.adk.agents.callback_context import CallbackContext
from agents.tools.llm_safe_decorator import llm_safe
from agents.tools.templates import (
    format_sku_price_block,
    order_draft_template,
    MULTI_MESSAGE_DELIMITER,
)

# This import assumes your schema is at this path, as provided
from agents.tools.tool_schemas.order_draft_schema import OrderDraft, OrderDraftItem
from agents.tools.tool_schemas.aws_lambda_schema import InvoiceDataAWS , InvoiceItemAWS , InvoiceItemTotalsAWS
from agents.tools.api_tools import search_customer_by_phone, unwrap_tool_response
from ..helpers.session_helper import SessionStore
from agents.helpers.firestore_utils import get_tenant_id, user_root

SALESFLO_API_ENDPOINT = "https://qe63yda6ybsmbi52gtow465qua0lpbhh.lambda-url.us-east-1.on.aws/salesflo-api"
WHATSAPP_API_URL = "https://graph.facebook.com/v23.0"
WA_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WA_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")

load_dotenv()

db = firestore.Client()
TENANT_ID = get_tenant_id()

_sessions = SessionStore()


def _user_ref(user_id: str):
    return user_root(db, user_id, tenant_id=TENANT_ID)

# ==============================================================================
# Helpers
# ==============================================================================

def _post_with_retries(
    url: str,
    *,
    json_payload: dict,
    headers: dict,
    connect_timeout: float = 5.0,
    read_timeout: float = 90.0,
    max_attempts: int = 3,
    base_backoff: float = 0.8,
    raise_for_status: bool = True,
) -> requests.Response:
    """
    POST with small, bounded retry policy on timeouts and 5xx.
    Raises on final failure. Uses (connect, read) timeout tuple.
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, max_attempts + 1):
        try:
            resp = requests.post(
                url,
                json=json_payload,
                headers=headers,
                timeout=(connect_timeout, read_timeout),
            )
            # Retry on transient server errors
            if 500 <= resp.status_code < 600:
                last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
            else:
                if raise_for_status:
                    resp.raise_for_status()
                return resp
        except (requests.Timeout, requests.ConnectionError) as e:
            last_err = e
        except requests.RequestException:
            # For 4xx or other non-retryables, bubble up immediately
            if raise_for_status:
                raise
            # If caller opted out of raise_for_status, still return the response when available.
            if hasattr(locals().get("resp", None), "status_code"):
                return resp  # type: ignore[return-value]
            raise

        # Backoff before next attempt
        if attempt < max_attempts:
            sleep_for = base_backoff * (2 ** (attempt - 1))
            logger.warning(
                "invoice.lambda.retrying",
                attempt=attempt,
                max_attempts=max_attempts,
                sleep_for=sleep_for,
            )
            time.sleep(sleep_for)

    # Exhausted attempts
    raise last_err if last_err else RuntimeError("Unknown error calling invoice lambda")


def _pick_default_thumbnail_retailer_id() -> Optional[str]:
    """
    Best-effort guess for Meta catalog's product_retailer_id to display as thumbnail
    in WhatsApp Cloud API `catalog_message`.
    """
    env_val = (
        os.getenv("WHATSAPP_CATALOG_THUMBNAIL_PRODUCT_RETAILER_ID")
        or os.getenv("WHATSAPP_THUMBNAIL_PRODUCT_RETAILER_ID")
        or os.getenv("THUMBNAIL_PRODUCT_RETAILER_ID")
    )
    if env_val:
        return str(env_val).strip()
    try:
        # Repo-relative path inside container/runtime
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # Agentflo-YTL/
        products_path = os.path.join(base_dir, "data", "products.json")
        with open(products_path, "r", encoding="utf-8") as f:
            js = json.load(f) or {}
        products = js.get("products") if isinstance(js, dict) else None
        if isinstance(products, list) and products:
            p0 = products[0] or {}
            if isinstance(p0, dict):
                for k in ("sku_code", "sku", "product_retailer_id", "id"):
                    v = p0.get(k)
                    if v:
                        return str(v).strip()
    except Exception:
        pass
    # No safe fallback: a wrong retailer_id causes WhatsApp Cloud API 400
    # with "Products not found in FB Catalog". Prefer leaving it unset and
    # letting WhatsApp choose a default thumbnail (or retry without it).
    return None


_RETAILER_ID_TO_INTERNAL_SKU_CACHE: Optional[dict[str, str]] = None


def _retailer_id_to_internal_sku_map() -> dict[str, str]:
    """
    Map Meta catalog retailer_id/content_id -> internal sku_code.

    YTL's Meta catalog uses numeric content IDs (e.g. "7"), while internal catalog
    uses sku_codes like "ECOBUILD". This mapping lets cart rendering/enrichment
    display proper names and pricing even when the cart contains retailer IDs.
    """
    global _RETAILER_ID_TO_INTERNAL_SKU_CACHE
    if _RETAILER_ID_TO_INTERNAL_SKU_CACHE is not None:
        return _RETAILER_ID_TO_INTERNAL_SKU_CACHE
    mapping: dict[str, str] = {}
    try:
        base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))  # Agentflo-YTL/
        products_path = os.path.join(base_dir, "data", "products.json")
        with open(products_path, "r", encoding="utf-8") as f:
            js = json.load(f) or {}
        products = js.get("products") if isinstance(js, dict) else None
        if isinstance(products, list):
            for p in products:
                if not isinstance(p, dict):
                    continue
                rid = str(p.get("product_retailer_id") or "").strip()
                sku = str(p.get("sku_code") or p.get("sku") or "").strip()
                if rid and sku:
                    mapping[rid] = sku
    except Exception:
        mapping = {}
    _RETAILER_ID_TO_INTERNAL_SKU_CACHE = mapping
    return mapping


def _maybe_map_retailer_id_sku(sku: Optional[str]) -> Optional[str]:
    """
    If sku looks like a Meta retailer/content id (e.g. "7"), map it to internal sku_code.
    Otherwise return original.
    """
    if sku is None:
        return None
    token = str(sku).strip()
    if not token:
        return None
    if token.isdigit():
        mapped = _retailer_id_to_internal_sku_map().get(token)
        if mapped:
            return mapped
    return token
 
 
def legacy_json_schema(model: type[BaseModel]):
    """Generate ADK-compatible schema with nested definitions included."""
    schema = model.model_json_schema(ref_template="#/definitions/{model}")

    if "$defs" in schema:
        schema["definitions"] = schema.pop("$defs")
    if "definitions" not in schema:
        schema["definitions"] = {}
 
    # Convert $defs -> definitions
    if "$defs" in schema:
        schema["definitions"] = schema.pop("$defs")

    # Ensure nested model definitions exist (Pydantic sometimes omits them)
    if "definitions" not in schema:
        schema["definitions"] = {}

    # Merge in referenced models manually
    for field in model.model_fields.values():
        field_type = getattr(field.annotation, "__name__", None)
        if hasattr(field.annotation, "model_json_schema"):
            nested = field.annotation.model_json_schema(ref_template="#/definitions/{model}")
            if "$defs" in nested:
                nested["definitions"] = nested.pop("$defs")
            for k, v in nested.get("definitions", {}).items():
                schema["definitions"][k] = v
            schema["definitions"].setdefault(field_type, nested)

    return schema


def set_invoice_output_schema(callback_context: CallbackContext, llm_request: LlmRequest) -> None:
    trigger_phrase = "generate an invoice"

    if (
        llm_request.contents
        and llm_request.contents[-1].parts
        and trigger_phrase in llm_request.contents[-1].parts[-1].text.lower()
    ):
        print("Trigger phrase found. Setting output schema.")
        schema = legacy_json_schema(InvoiceDataAWS)
        llm_request.set_output_schema(schema)
        llm_request.tools_dict.clear()


def _safe_json(response: requests.Response) -> Tuple[Optional[dict], Optional[str]]:
    """
    Try to parse JSON; if it fails, return (None, raw_text) and emit noisy debug info.
    """
    try:
        j = response.json()
        print("[_safe_json] Parsed JSON OK.")
        logger.debug("[_safe_json] Parsed JSON OK.")
        return j, None
    except ValueError as ve:
        print(f"[_safe_json] JSON parse error: {ve}")
        logger.debug("[_safe_json] JSON parse error", error=str(ve))
        text = None
        try:
            text = response.text
            print(f"[_safe_json] Falling back to response.text (len={len(text) if text else 0}).")
            logger.debug("[_safe_json] Fallback to text", length=len(text) if text else 0)
        except Exception as te:
            print(f"[_safe_json] Could not read response.text: {te}")
            logger.debug("[_safe_json] Could not read response.text", error=str(te))
            text = None
        return None, text

def _is_success_payload(payload: dict) -> bool:
    """
    Consider various success shapes:
    - {"response": "SUCCESS"} or {"Response": "SUCCESS"}
    - {"status": "SUCCESS"} | {"status":"OK"} (any case)
    - {"result": "SUCCESS"} | {"result":"OK"}
    - {"success": true}
    """
    candidates = ["response", "Response", "status", "Status", "result", "Result", "success", "Success"]
    for key in candidates:
        if key in payload:
            val = payload[key]
            if isinstance(val, bool):
                if val is True:
                    return True
            if isinstance(val, str):
                if val.strip().upper() in ("SUCCESS", "OK"):
                    return True
    return False

def _best_message(payload: dict) -> Optional[str]:
    for key in ("message", "Message", "error", "Error", "detail", "Detail"):
        if key in payload:
            val = payload.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    return None

def _ordinal_suffix(day: int) -> str:
    if 11 <= (day % 100) <= 13:
        return "th"
    return {1: "st", 2: "nd", 3: "rd"}.get(day % 10, "th")

def _format_delivery_date_human(raw_date: Any) -> Optional[str]:
    if raw_date is None:
        return None

    # Support date/datetime objects directly.
    if isinstance(raw_date, datetime.datetime):
        dt = raw_date
    elif isinstance(raw_date, datetime.date):
        dt = datetime.datetime.combine(raw_date, datetime.time.min)
    else:
        raw = str(raw_date).strip()
        if not raw:
            return None

        dt = None
        iso_candidate = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            dt = datetime.datetime.fromisoformat(iso_candidate)
        except ValueError:
            pass

        if dt is None:
            for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
                try:
                    dt = datetime.datetime.strptime(raw, fmt)
                    break
                except ValueError:
                    continue

        if dt is None:
            return raw

    day = dt.day
    return f"{dt.strftime('%A')} {day}{_ordinal_suffix(day)} {dt.strftime('%B %Y')}"

def _extract_delivery_date(payload: dict) -> Optional[Any]:
    if not isinstance(payload, dict):
        return None

    for key in ("DeliveryDate", "delivery_date", "deliveryDate"):
        val = payload.get(key)
        if val:
            return val

    body = payload.get("body")
    if isinstance(body, dict):
        for key in ("DeliveryDate", "delivery_date", "deliveryDate"):
            val = body.get(key)
            if val:
                return val

    return None

def _build_delivery_notice(payload: dict, strings: dict) -> str:
    raw_delivery_date = _extract_delivery_date(payload)
    formatted_date = _format_delivery_date_human(raw_delivery_date)
    if not formatted_date:
        return ""

    line_1 = strings.get(
        "delivery_notice_with_date",
        "Your order has been confirmed and will be delivered to you on\n*{delivery_date}*",
    )
    line_2 = strings.get(
        "delivery_notice_followup",
        "For further details, message us on this date to enquire about your delivery driver.",
    )

    try:
        first_line = line_1.format(delivery_date=formatted_date)
    except Exception:
        first_line = f"{line_1}\n*{formatted_date}*"

    return f"{first_line}\n{line_2}".strip()

def _truncate(s: Optional[str], n: int = 1500) -> Optional[str]:
    if s is None:
        return None
    if len(s) <= n:
        return s
    return s[:n] + " …[truncated]"

def _diagnostic_string(resp: requests.Response, raw_text: Optional[str]) -> str:
    """
    Build a concise diagnostic string with status code, content-type, content-length,
    and a short preview of the body (if available).
    """
    try:
        headers = dict(resp.headers or {})
    except Exception:
        headers = {}
    ct = headers.get("Content-Type")
    cl = headers.get("Content-Length")
    preview = _truncate(raw_text)
    return f"HTTP {resp.status_code}; Content-Type: {ct}; Content-Length: {cl}; Body: {preview}"

# --------------------------------------------------------------------------
# Debug dump helper for HTTP responses
# --------------------------------------------------------------------------
def _debug_dump_response(resp: requests.Response) -> None:
    try:
        print(f"[resp] status={resp.status_code}")
        print(f"[resp] headers={dict(resp.headers or {})}")
        logger.debug("[resp] status and headers", status=resp.status_code, headers=dict(resp.headers or {}))
    except Exception as e:
        print(f"[resp] header dump error: {e}")
        logger.debug("[resp] header dump error", error=str(e))
    try:
        txt = resp.text
        preview = _truncate(txt, 2000)
        print(f"[resp] text_preview={preview}")
        logger.debug("[resp] text preview", preview=preview)
    except Exception as e:
        print(f"[resp] text read error: {e}")
        logger.debug("[resp] text read error", error=str(e))

# ------------------------------------------------------------------------------
# Helper functions for unwrapping API envelopes and JSON-in-string
# ------------------------------------------------------------------------------
def _parse_possible_wrapped_json(text: Optional[str]) -> Optional[dict]:
    if not text:
        print("[_parse_possible_wrapped_json] No text provided.")
        logger.debug("[_parse_possible_wrapped_json] No text provided.")
        return None
    s = text.strip()
    print(f"[_parse_possible_wrapped_json] incoming_len={len(text)} startswith={{ {s[:1]} }}")
    logger.debug("[_parse_possible_wrapped_json] incoming", length=len(text), startswith=s[:1] if s else "")
    if not s:
        return None
    if not (s.startswith("{") or s.startswith("[")):
        print("[_parse_possible_wrapped_json] Text does not look like JSON.")
        logger.debug("[_parse_possible_wrapped_json] Text does not look like JSON.")
        return None
    try:
        parsed = json.loads(s)
        print("[_parse_possible_wrapped_json] Parsed inner JSON successfully.")
        logger.debug("[_parse_possible_wrapped_json] Parsed inner JSON successfully.")
        return parsed
    except Exception as e:
        print(f"[_parse_possible_wrapped_json] json.loads failed: {e}")
        logger.debug("[_parse_possible_wrapped_json] json.loads failed", error=str(e))
        return None

def _unwrap_api_envelope(payload: Optional[dict], resp: requests.Response) -> Optional[dict]:
    """
    Unwrap common API envelopes like:
        { "statusCode": 200, "body": "{...json...}" }
        { "raw": "{...json...}" }
        { "data": {...} }
    If an inner JSON string is found, it is parsed and returned.
    Otherwise returns the original payload.
    """
    if payload is None:
        # Try parsing the raw HTTP body if available
        return _parse_possible_wrapped_json(getattr(resp, "text", None))

    # If the server already returned a dict, check common envelope keys
    if isinstance(payload, dict):
        for key in ("body", "Body", "raw", "Raw", "data", "Data", "payload", "Payload"):
            if key in payload:
                inner = payload.get(key)
                print(f"[_unwrap_api_envelope] Found envelope key='{key}' type={type(inner).__name__}")
                logger.debug("[_unwrap_api_envelope] envelope", key=key, inner_type=type(inner).__name__)
                if isinstance(inner, dict):
                    return inner
                if isinstance(inner, str):
                    parsed = _parse_possible_wrapped_json(inner)
                    if parsed is not None:
                        return parsed
                    else:
                        print(f"[_unwrap_api_envelope] Inner string not JSON. First 200 chars: {inner[:200]}")
                        logger.debug("[_unwrap_api_envelope] inner string not JSON", first200=inner[:200])
                if isinstance(inner, list):
                    return {key: inner}
    return payload

JWT_TOKEN = os.getenv("API_JWT_TOKEN")
REQUEST_HEADERS = {
    # We let requests set Content-Type when using json=...
    "Authorization": f"Bearer {JWT_TOKEN}",
    "Content-Type": "application/json",
}

DEFAULT_SALES_INTEL_ENDPOINT = "https://portal.agentflo.com/api/v2/basket/optimised"


def _safe_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


_SKU_CODE_RE = re.compile(r"^[A-Za-z0-9._/-]+$")


def _looks_like_sku_code(value: Any) -> bool:
    if value is None:
        return False
    token = str(value).strip()
    if not token:
        return False
    if any(ch.isspace() for ch in token):
        return False
    return bool(_SKU_CODE_RE.match(token))


def _normalize_sku_code(value: Any) -> Optional[str]:
    if not _looks_like_sku_code(value):
        return None
    return str(value).strip()


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


def _coerce_qty(val: Any) -> int:
    try:
        qty_val = int(val)
        return max(qty_val, 0)
    except (TypeError, ValueError):
        return 0


def _ensure_sales_intel_endpoint(raw_url: Optional[str]) -> str:
    """
    Always point to the v2 orchestrator, even if env has an older URL/typo.
    """
    if not raw_url:
        return DEFAULT_SALES_INTEL_ENDPOINT
    if "/api/v2/" in raw_url:
        return raw_url
    return DEFAULT_SALES_INTEL_ENDPOINT


def _clean_env_value(value: Any) -> Optional[str]:
    if value is None:
        return None
    token = str(value).strip()
    if not token:
        return None
    if len(token) >= 2 and token[0] == token[-1] and token[0] in {"'", '"'}:
        token = token[1:-1].strip()
    return token or None


def _resolve_cart_prices(item: dict, qty: int) -> dict:
    """
    Normalize price fields from Sales Intelligence cart_items payload.
    """
    base_price = _safe_float(
        item.get("base_price")
        or item.get("unit_price")
        or item.get("price")
        or item.get("consumer_price")
        or item.get("list_price")
    )
    final_price = _safe_float(
        item.get("final_price")
        or item.get("discounted_price")
        or item.get("unit_price_final")
        or item.get("unit_price")
        or item.get("price")
    )
    unit_discount = _safe_float(item.get("discount_value") or item.get("unit_discount"))
    line_discount = _safe_float(item.get("discount_value_line") or item.get("line_discount"))
    discount_value = unit_discount
    discount_pct = _safe_float(item.get("discount_pct") or item.get("discountvalue"))
    old_line_total = _safe_float(
        item.get("old_line_total")
        or item.get("pre_discount_line_total")
        or item.get("line_total_before_discount")
        or item.get("total_before_discount")
    )

    if base_price is None and old_line_total is not None and qty > 0:
        try:
            base_price = round(float(old_line_total) / float(qty), 4)
        except Exception:
            base_price = None

    if discount_value is None and line_discount is not None and qty > 0:
        try:
            discount_value = round(float(line_discount) / float(qty), 4)
        except Exception:
            discount_value = None

    if discount_value is None and base_price is not None and final_price is not None:
        try:
            discount_value = round(float(base_price) - float(final_price), 2)
        except Exception:
            discount_value = None

    if discount_pct is None and discount_value is not None and base_price not in (None, 0):
        try:
            discount_pct = round((float(discount_value) / float(base_price)) * 100.0, 2)
        except Exception:
            discount_pct = None

    line_total = _safe_float(
        item.get("line_total")
        or item.get("linetotal")
        or item.get("lineamount")
        or item.get("line_total_amount")
    )
    if line_total is None and final_price is None and old_line_total is not None and qty > 0 and base_price is not None:
        # If only pre-discount totals are available, assume no line-level override.
        line_total = round(float(base_price) * float(qty), 2)

    if discount_value is None and old_line_total is not None and line_total is not None and qty > 0:
        try:
            line_discount = float(old_line_total) - float(line_total)
            if line_discount > 0:
                discount_value = round(line_discount / float(qty), 4)
        except Exception:
            pass

    if line_discount is None and discount_value is not None and qty > 0:
        try:
            line_discount = round(float(discount_value) * float(qty), 2)
        except Exception:
            line_discount = None

    if discount_pct is None and old_line_total not in (None, 0) and line_total is not None:
        try:
            discount_pct = round(((float(old_line_total) - float(line_total)) / float(old_line_total)) * 100.0, 2)
        except Exception:
            pass
    if line_total is None:
        unit_price = final_price if final_price is not None else base_price
        if unit_price is not None:
            try:
                line_total = round(float(unit_price) * float(qty or 0), 2)
            except Exception:
                line_total = None

    price_for_display = final_price if final_price is not None else base_price

    return {
        "price": price_for_display,
        "base_price": base_price,
        "final_price": final_price,
        "discount_value": discount_value,
        "discount_value_line": line_discount,
        "discount_pct": discount_pct,
        "line_total": line_total,
    }


def _normalize_cart_item(raw: dict) -> dict:
    """
    Extract a consistent item structure from the cart_items API response.
    """
    sku = (
        raw.get("sku_code")
        or raw.get("sku")
        or raw.get("sku_id")
        or raw.get("skucode")
        or raw.get("item_number")
        or raw.get("promotion_code")
        or raw.get("promotioncode")
    )
    qty = _coerce_qty(raw.get("qty") or raw.get("quantity") or raw.get("forecast_qty"))
    name = (
        raw.get("official_name")
        or raw.get("name")
        or raw.get("product_name")
        or raw.get("sku_name")
        or raw.get("description")
        or raw.get("description_en")
        or raw.get("title")
        or raw.get("sku_desc")
    )

    pricing = _resolve_cart_prices(raw, qty)
    # profit = _safe_float(raw.get("profit") or raw.get("line_profit"))
    # profit_margin = _safe_float(raw.get("profit_margin") or raw.get("profit_margin_pct"))
    # if profit is None and profit_margin is not None and pricing.get("line_total") not in (None, 0):
    #     try:
    #         profit = round((float(profit_margin) / 100.0) * float(pricing["line_total"]), 2)
    #     except Exception:
    #         profit = None

    return {
        "sku_code": _normalize_sku_code(sku),
        "name": str(name) if name is not None else None,
        "qty": qty,
        **pricing,
        # "profit": profit,
        # "profit_margin": profit_margin,
        "product_retailer_id": raw.get("product_retailer_id")
        or raw.get("productid")
        or raw.get("product_id"),
    }


def _ensure_total_amount_field(draft_data: dict) -> dict:
    """
    Ensure total_amount is present for OrderDraft validation by deriving it from
    totals, grand_total/subtotal, or by summing line totals/prices.
    """
    if not isinstance(draft_data, dict):
        return draft_data or {}

    draft = dict(draft_data)
    if _safe_float(draft.get("total_amount")) is not None:
        return draft

    totals = draft.get("totals") if isinstance(draft.get("totals"), dict) else {}
    candidates = [
        draft.get("total_amount"),
        totals.get("grand_total") if totals else None,
        totals.get("subtotal") if totals else None,
        draft.get("grand_total"),
        draft.get("subtotal"),
    ]

    derived_total = None
    for val in candidates:
        val_f = _safe_float(val)
        if val_f is not None:
            derived_total = val_f
            break

    if derived_total is None:
        items = draft.get("items") or draft.get("skus") or []
        computed = 0.0
        for it in items:
            if not isinstance(it, dict):
                continue
            qty = _coerce_qty(it.get("qty"))
            line_total = _safe_float(it.get("line_total"))
            if line_total is None:
                final_price = _safe_float(
                    it.get("final_price")
                    or it.get("price")
                    or it.get("base_price")
                    or it.get("unit_price")
                )
                if final_price is None:
                    continue
                line_total = round(final_price * qty, 2)
            computed += float(line_total)
        if computed > 0:
            derived_total = round(computed, 2)

    if derived_total is not None:
        draft["total_amount"] = derived_total
    return draft


def _extract_sales_intel_sections(api_response: dict) -> dict:
    """
    Normalize Sales Intelligence response shape to a consistent structure.
    """
    sections = {
        "items": [],
        "totals": {},
        "summary": {},
        "objective_used": None,
        "customer_id": None,
        "basket_id": None,
    }
    basket = api_response.get("basket") if isinstance(api_response, dict) else {}
    if isinstance(basket, dict):
        sections["items"] = basket.get("items") or sections["items"]
        sections["totals"] = basket.get("totals") or sections["totals"]
        sections["summary"] = basket.get("summary") or sections["summary"]
        sections["objective_used"] = basket.get("objective_used") or basket.get("objective") or sections["objective_used"]
        sections["customer_id"] = basket.get("customer_id") or basket.get("store_id") or sections["customer_id"]
        sections["basket_id"] = basket.get("basket_id") or basket.get("id") or basket.get("basketid") or sections["basket_id"]

    if isinstance(api_response, dict):
        sections["items"] = api_response.get("items") or sections["items"]
        sections["totals"] = api_response.get("totals") or sections["totals"]
        sections["summary"] = api_response.get("summary") or sections["summary"]
        sections["objective_used"] = api_response.get("objective_used") or api_response.get("objective") or sections["objective_used"]
        sections["customer_id"] = api_response.get("customer_id") or api_response.get("store_id") or sections["customer_id"]
        if sections["basket_id"] is None:
            sections["basket_id"] = api_response.get("basket_id") or api_response.get("id") or api_response.get("basketid")

    return sections


def _compute_cart_totals(normalized_items: list[dict], totals_raw: dict, summary_raw: dict) -> dict:
    """
    Compute basket-level totals with sensible fallbacks when API omits fields.
    """
    subtotal = _safe_float(
        totals_raw.get("subtotal")
        or summary_raw.get("subtotal")
        or summary_raw.get("total_list_price")
    )
    grand_total = _safe_float(
        totals_raw.get("grand_total")
        or totals_raw.get("total")
        or summary_raw.get("grand_total")
        or summary_raw.get("total_amount")
        or summary_raw.get("achieved_total")
    )
    discount_total = _safe_float(
        totals_raw.get("discount_total")
        or totals_raw.get("discount")
        or summary_raw.get("discount_total")
        or summary_raw.get("total_discount")
    )
    # total_profit = _safe_float(
    #     totals_raw.get("profit")
    #     or totals_raw.get("total_profit")
    #     or summary_raw.get("total_profit")
    # )
    # profit_margin_pct = _safe_float(
    #     summary_raw.get("profit_margin")
    #     or summary_raw.get("profit_margin_pct")
    #     or totals_raw.get("profit_margin")
    # )

    if subtotal is None:
        subtotal = 0.0
        for itm in normalized_items:
            qty_val = itm.get("qty") or 0
            bp = _safe_float(itm.get("base_price"))
            if bp is not None:
                subtotal += float(bp) * float(qty_val)

    if grand_total is None:
        line_total_sum = 0.0
        for itm in normalized_items:
            lt = itm.get("line_total")
            if lt is not None:
                line_total_sum += float(lt)
            else:
                fp = itm.get("final_price")
                if fp is not None:
                    line_total_sum += float(fp) * float(itm.get("qty") or 0)
        grand_total = line_total_sum

    if discount_total is None and subtotal is not None and grand_total is not None:
        try:
            discount_total = max(float(subtotal) - float(grand_total), 0.0)
        except Exception:
            discount_total = None

    # if total_profit is None:
    #     profit_sum = 0.0
    #     has_profit = False
    #     for itm in normalized_items:
    #         prof = itm.get("profit")
    #         if prof is not None:
    #             profit_sum += float(prof)
    #             has_profit = True
    #     if has_profit:
    #         total_profit = profit_sum

    # if profit_margin_pct is None and total_profit is not None and subtotal not in (None, 0):
    #     try:
    #         profit_margin_pct = round((float(total_profit) / float(subtotal)) * 100.0, 2)
    #     except Exception:
    #         profit_margin_pct = None

    return {
        "subtotal": subtotal,
        "grand_total": grand_total,
        "discount_total": discount_total,
        # "profit": total_profit,
        # "profit_total": total_profit,
        # "profit_margin": profit_margin_pct,
        # "profit_margin_pct": profit_margin_pct,
        "total": grand_total,
    }


def _normalize_cart_items_response(api_response: dict) -> Tuple[dict, dict, dict]:
    """
    Convert the API response into (items_by_sku, totals, meta).
    """
    sections = _extract_sales_intel_sections(api_response)
    items_by_sku: dict[str, dict] = {}
    normalized_items: list[dict] = []

    for itm in sections.get("items") or []:
        if not isinstance(itm, dict):
            continue
        normalized = _normalize_cart_item(itm)
        sku_key = normalized.get("sku_code")
        if not sku_key:
            continue
        normalized_items.append(normalized)
        candidate_keys = {
            str(normalized.get("sku_code") or ""),
            str(itm.get("sku") or ""),
            str(itm.get("sku_code") or ""),
            str(itm.get("promotion_code") or ""),
            str(itm.get("promotioncode") or ""),
        }
        for key in candidate_keys:
            if key and key not in items_by_sku:
                items_by_sku[key] = normalized

    totals = _compute_cart_totals(normalized_items, sections.get("totals") or {}, sections.get("summary") or {})
    meta = {
        "basket_id": sections.get("basket_id"),
        "objective": sections.get("objective_used"),
        "customer_id": sections.get("customer_id"),
    }
    return items_by_sku, totals, meta

def _extract_customer_store_from_api(user_id: str) -> dict:
    """
    Fetch customer/store info, using Firestore as a cache.

    Returns:
        {
            "store_name_en": str | None,
            "customer_name": str | None,
            "store_id": str,
            "customer_phone": str,
        }

    Behaviour:
    - First, check tenants/{TENANT_ID}/agent_id/{AGENT_ID}/users/{user_id} Firestore doc for existing store details.
    - If store_id is present there, return it (no API call).
    - Otherwise, call search_customer_by_phone(user_id) ONCE,
      parse the result, store it into Firestore, and return.
    - On any failure, fall back to minimal defaults.
    """
    defaults = {
        "customer_phone": user_id,
        "store_id": "UNKNOWN_STORECODE",  # explicit sentinel to avoid phone-number store_id
        "storecode": "UNKNOWN_STORECODE",
        "store_name_en": "Unknown Store",
        "customer_name": "Unknown Customer",
    }

    # 1) Try Firestore cache first
    try:
        user_ref = _user_ref(user_id)
        snap = user_ref.get()
        if snap.exists:
            user_data = snap.to_dict() or {}
            cached_store_id = (
                user_data.get("store_id")
                or user_data.get("storecode")
                or (user_data.get("order_drafts") or {}).get("store_id")
            )
            cached_store_name = user_data.get("store_name_en")
            cached_customer_name = user_data.get("customer_name")
            cached_phone = user_data.get("customer_phone") or user_id

            if cached_store_id:
                # Cache hit – return immediately
                return {
                    "store_name_en": str(cached_store_name) if cached_store_name else defaults["store_name_en"],  # ← FIX
                    "customer_name": str(cached_customer_name) if cached_customer_name else defaults["customer_name"],  # ← FIX
                    "store_id": str(cached_store_id),
                    "storecode": str(cached_store_id),
                    "customer_phone": str(cached_phone),
                }
    except Exception as e:
        logger.error(
            "customer.lookup.cache_read_error user_id=%s error=%s",
            user_id,
            str(e),
        )
        # fall back to defaults + API

    # 2) No cache hit → call API once
    try:
        if search_customer_by_phone is None:
            logger.warning("customer.lookup.import_missing user_id=%s", user_id)
            return defaults

        raw = search_customer_by_phone(user_id)
        ok, payload, err = unwrap_tool_response(raw, system_name="search_customer_by_phone")
        if not ok or not payload:
            logger.warning(
                "customer.lookup.error",
                user_id=user_id,
                error=err,
            )
            return defaults

        if not isinstance(payload, dict):
            logger.warning(
                "customer.lookup.invalid_payload",
                user_id=user_id,
                payload_type=type(payload).__name__,
            )
            return defaults

        data = payload.get("data", {}) or {}

        store_name_en = data.get("display_name") or defaults["store_name_en"]  # ← FIX: Use default if None
        customer_name = data.get("contact_name") or defaults["customer_name"]  # ← FIX: Use default if None
        store_code = (
            (data.get("additional_info") or {}).get("storecode")
            or data.get("storecode")
            or defaults["store_id"]
        )
        customer_phone = user_id

        logger.info(
            "customer.lookup.success user_id=%s store_name=%s customer_name=%s store_id=%s",
            user_id,
            store_name_en,
            customer_name,
            store_id,
        )

        # 3) Write back to Firestore for future calls
        try:
            user_ref = _user_ref(user_id)
            user_ref.set(
                {
                    "store_id": str(store_code),
                    "storecode": str(store_code),
                    "store_name_en": str(store_name_en),  # ← Always store non-None value
                    "customer_name": str(customer_name),  # ← Always store non-None value
                    "customer_phone": str(customer_phone),
                },
                merge=True,
            )
        except Exception as e:
            logger.error(
                "customer.lookup.cache_write_error user_id=%s error=%s",
                user_id,
                str(e),
            )

        return {
            "store_name_en": str(store_name_en),
            "customer_name": str(customer_name),
            "store_id": str(store_code),
            "storecode": str(store_code),
            "customer_phone": str(customer_phone),
        }

    except Exception as e:
        logger.error("customer.lookup.exception user_id=%s error=%s", user_id, str(e))
        return defaults
    
def _resolve_store_id_for_cart(user_id: str, store_id: Optional[str] = None) -> str:
    """
    Resolve a store_id for cart operations, preferring an explicit value and
    falling back to the cached customer lookup.
    """
    if store_id and str(store_id).strip():
        return str(store_id).strip()

    try:
        customer = _extract_customer_store_from_api(user_id)
        resolved = customer.get("storecode") or customer.get("store_id")
        if resolved:
            return str(resolved).strip()
    except Exception as e:
        logger.warning("cart.store_id.resolve_failed", user_id=user_id, error=str(e))

    # Last resort: keep backward-compatible path, but avoid returning a pure phone number if possible.
    return "UNKNOWN_STORECODE"

import os
import requests
from typing import Any, Dict, List
from utils.logging import logger  # or wherever your logger comes from


DEFAULT_V2_ENDPOINT = "https://portal.agentflo.com/api/v2/basket/optimised"


def _safe_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except (TypeError, ValueError):
        return None
    
def _ensure_v2_endpoint(raw_url: str | None) -> str:
    """
    Force the orchestrator endpoint to v2 even if an older URL is configured.
    Mirrors the logic in sales_intelligence_engine but kept local
    to avoid circular imports.
    """
    if not raw_url:
        return DEFAULT_V2_ENDPOINT
    if "/api/v2/" in raw_url:
        return raw_url
    return DEFAULT_V2_ENDPOINT


def fetch_optimised_basket(store_id: str, items: list) -> dict:
    """
    Calls the Sales Intelligence CART_ITEMS endpoint to price/discount the cart.
    Always returns normalized per-SKU data and basket-level totals.
    """
    def _empty_result():
        return {"items_by_sku": {}, "basket_totals": {}, "basket_meta": {}, "raw": {}}

    tenant_id = TENANT_ID
    sales_intel_token = _clean_env_value(os.getenv("API_JWT_TOKEN"))
    raw_endpoint = _clean_env_value(os.getenv("SALES_INTELLIGENCE_ENDPOINT") or os.getenv("SALES_INTELLIGENSE_ENDPOINT"))
    url = _ensure_sales_intel_endpoint(raw_endpoint)

    cleaned_items = []
    for item in items or []:
        if isinstance(item, OrderDraftItem):
            sku = getattr(item, "sku_code", None)
            qty_val = getattr(item, "qty", None)
        elif isinstance(item, dict):
            sku = (
                item.get("sku_code")
                or item.get("sku")
                or item.get("skucode")
                or item.get("sku_id")
                or item.get("item_number")
            )
            qty_val = item.get("qty") or item.get("quantity")
        else:
            sku = None
            qty_val = None

        sku_code = _normalize_sku_code(sku)
        if not sku_code:
            if sku is not None:
                logger.warning("optimised_basket.invalid_sku_skipped", raw_sku=str(sku))
            continue
        qty_int = _coerce_qty(qty_val)
        if qty_int <= 0:
            continue
        cleaned_items.append({"sku": sku_code, "qty": qty_int})

    has_tenant = bool(tenant_id)
    has_token = bool(sales_intel_token)
    has_url = bool(url)

    if not (has_tenant and has_token and has_url):
        logger.warning(
            "optimised_basket.missing_env",
            has_tenant=bool(tenant_id),
            has_token=bool(sales_intel_token),
            has_url=bool(url),
        )
        return _empty_result()

    if not cleaned_items:
        logger.warning("optimised_basket.no_items", store_id=store_id)
        return _empty_result()

    payload = {
        "tenant_id": tenant_id,
        "customer_id": store_id,
        "sales_intel_token": sales_intel_token,
        "objective": "CART_ITEMS",
        "items": cleaned_items,
    }
    agent_id = _clean_env_value(os.getenv("AGENT_ID"))
    if agent_id:
        payload["agent_id"] = agent_id

    headers = {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        logger.info(
            "sales_intel.optimised_basket.request",
            endpoint=url,
            payload=_redact_sensitive_for_log(payload),
        )
        response = requests.post(url, headers=headers, json=payload, timeout=25)
        response.raise_for_status()
        data, raw_text = _safe_json(response)
        if data is None:
            data = _parse_possible_wrapped_json(raw_text) or {}
        logger.info(
            "sales_intel.optimised_basket.response",
            endpoint=url,
            status_code=response.status_code,
            payload=_redact_sensitive_for_log(data) if isinstance(data, (dict, list)) else None,
            body_preview=(raw_text[:2000] if isinstance(raw_text, str) else None),
        )

        items_by_sku, totals, meta = _normalize_cart_items_response(data or {})
        
        # Log what we actually got back
        logger.info(
            "optimised_basket.items_by_sku",
            items_by_sku=list(items_by_sku.keys()),
            totals=totals,
            sample_item=items_by_sku.get(list(items_by_sku.keys())[0]) if items_by_sku else None
        )
        
        return {
            "items_by_sku": items_by_sku,
            "basket_totals": totals,
            "basket_meta": meta,
            "raw": data or {},
        }

    except Exception as e:
        logger.error("optimised_basket.api_error", error=str(e))
        return _empty_result()

@llm_safe("order_draft.update")
def update_order_draft(user_id: str, order_draft: OrderDraft | dict) -> bool:
    """
    Update the cart using agentflo_cart_tool (Firestore-backed with pricing support).
    Supports additive deltas via use_qty_as_delta/merge_mode hints and adjust_qty_by fields.
    Default mode is additive. Replace mode requires explicit replace confirmation.
    """
    if not user_id:
        return False

    def _coerce_int(val: Any) -> Optional[int]:
        try:
            return int(val)
        except Exception:
            return None

    def _coerce_bool(val: Any) -> bool:
        if isinstance(val, bool):
            return val
        if isinstance(val, (int, float)):
            return val != 0
        if isinstance(val, str):
            token = val.strip().lower()
            if token in {"1", "true", "t", "yes", "y", "on"}:
                return True
            if token in {"0", "false", "f", "no", "n", "off"}:
                return False
        return False

    def _normalize_item(raw: Any) -> Optional[dict]:
        if isinstance(raw, OrderDraftItem):
            return raw.model_dump()
        if isinstance(raw, dict):
            return raw
        return None

    try:
        payload = order_draft or {}
        merge_mode_hint = ""
        use_qty_as_delta = False
        replace_confirmed = False
        incoming_items: List[Any] = []
        incoming_store_id = None

        if isinstance(order_draft, OrderDraft):
            incoming_store_id = getattr(order_draft, "store_id", None)
            incoming_items = [itm.model_dump() for itm in order_draft.items]
            merge_mode_hint = "increment"
        elif isinstance(order_draft, dict):
            incoming_store_id = payload.get("store_id")
            merge_mode_hint = str(payload.get("merge_mode") or payload.get("merge_strategy") or "").strip().lower()
            use_qty_as_delta = _coerce_bool(payload.get("use_qty_as_delta") or payload.get("qty_is_delta"))
            replace_confirmed = _coerce_bool(
                payload.get("replace_confirmed")
                or payload.get("replace_confirmed_by_user")
            )
            incoming_items = payload.get("items") or payload.get("skus") or []
        else:
            raise TypeError("order_draft must be a dict or OrderDraft object")

        store_id = _resolve_store_id_for_cart(user_id, incoming_store_id)

        # Best-effort pricing enrichment so cart lines carry base/final/discounts
        priced_lookup: dict[str, dict] = {}
        try:
            priced_result = fetch_optimised_basket(store_id, incoming_items)
            priced_lookup = priced_result.get("items_by_sku") or {}
        except Exception as e:
            logger.warning("order_draft.optimised_basket_failed", user_id=user_id, error=str(e))

        # Explicit clear request or empty payload
        if not incoming_items or merge_mode_hint in {"clear", "clear_cart", "clear_draft", "remove_all", "delete", "empty_cart", "empty"}:
            resp = agentflo_cart_tool(
                {"user_id": user_id, "store_id": store_id, "operations": [{"op": "CLEAR_CART"}]}
            )
            ok = bool(isinstance(resp, dict) and resp.get("ok"))
            (logger.info if ok else logger.warning)(
                "order_draft.cleared" if ok else "order_draft.clear_failed",
                user_id=user_id,
                reason="empty_payload",
                errors=resp.get("errors") if isinstance(resp, dict) else None,
            )
            return ok

        replace_requested = merge_mode_hint in {"replace", "set", "overwrite", "absolute", "reset"}
        replace_mode = replace_requested and replace_confirmed
        if replace_requested and not replace_confirmed:
            logger.info(
                "order_draft.replace_not_confirmed_default_increment",
                user_id=user_id,
                merge_mode_hint=merge_mode_hint,
            )
        if merge_mode_hint in {"add", "delta", "increment", "increase", "accumulate"}:
            use_qty_as_delta = True

        try:
            existing_cart = get_cart(user_id, store_id=store_id) or {}
        except Exception as e:
            existing_cart = {}
            logger.warning("order_draft.prefetch_failed", user_id=user_id, error=str(e))

        existing_qty: dict[str, int] = {}
        for existing in (existing_cart.get("items") or existing_cart.get("skus") or []):
            if not isinstance(existing, dict):
                continue
            sku_key = str(existing.get("sku_code") or existing.get("sku") or "").strip()
            qty_val = _coerce_int(existing.get("qty"))
            if sku_key:
                existing_qty[sku_key] = qty_val if qty_val is not None else 0

        operations: List[dict] = [{"op": "CLEAR_CART"}] if replace_mode else []

        for raw_item in incoming_items:
            item = _normalize_item(raw_item)
            if not item:
                continue

            sku_raw = (
                item.get("sku_code")
                or item.get("sku")
                or item.get("skucode")
                or item.get("sku_id")
                or item.get("item_number")
                or ""
            )
            sku = _normalize_sku_code(_maybe_map_retailer_id_sku(sku_raw))
            if not sku:
                if sku_raw:
                    logger.warning("order_draft.update.invalid_sku_skipped", user_id=user_id, raw_sku=str(sku_raw))
                continue

            name = (
                item.get("name")
                or item.get("official_name")
                or item.get("product_name")
                or item.get("sku_name")
                or item.get("description")
                or item.get("description_en")
                or item.get("title")
            )
            product_retailer_id = (
                item.get("product_retailer_id")
                or item.get("productid")
                or item.get("product_id")
                or item.get("retailer_id")
            )
            qty_val = _coerce_int(
                item.get("qty")
                if "qty" in item
                else (item.get("quantity") or item.get("forecast_qty"))
            )
            adjust_delta = _coerce_int(item.get("adjust_qty_by"))

            if adjust_delta is not None:
                base_qty = existing_qty.get(sku, 0)
                target_qty = max(base_qty + adjust_delta, 0)
                operations.append({"op": "SET_QTY", "sku_code": sku, "qty": target_qty})
                existing_qty[sku] = target_qty
                continue

            if qty_val is None:
                continue

            if use_qty_as_delta and not replace_mode and qty_val < 0:
                base_qty = existing_qty.get(sku, 0)
                target_qty = max(base_qty + qty_val, 0)
                operations.append({"op": "SET_QTY", "sku_code": sku, "qty": target_qty})
                existing_qty[sku] = target_qty
                continue

            if qty_val < 0:
                # Negative absolute qty is ignored unless handled via delta logic above
                continue

            if use_qty_as_delta and not replace_mode:
                op = {
                    "op": "ADD_ITEM",
                    "sku_code": sku,
                    "qty": qty_val,
                    "merge_strategy": "INCREMENT",
                }
            else:
                op = {
                    "op": "ADD_ITEM",
                    "sku_code": sku,
                    "qty": qty_val,
                }

            if name:
                op["name"] = name
            if product_retailer_id:
                op["product_retailer_id"] = product_retailer_id

            # Enrich with pricing metadata from Sales Intel (fallback to provided item fields)
            pricing_hint = priced_lookup.get(sku) if priced_lookup else {}
            field_aliases = {
                "name": ["name", "official_name", "product_name", "sku_name", "description", "description_en", "title"],
                "product_retailer_id": ["product_retailer_id", "productid", "product_id", "retailer_id"],
                "price": ["price", "unit_price", "final_price", "base_price", "consumer_price", "list_price", "mrp", "total_buy_price_virtual_pack"],
                "base_price": ["base_price", "consumer_price", "list_price", "mrp", "unit_price", "price"],
                "final_price": ["final_price", "discounted_price", "unit_price_final", "unit_price", "price", "total_buy_price_virtual_pack"],
                "discount_value": ["discount_value", "unit_discount", "discount"],
                "discount_value_line": ["discount_value_line", "line_discount"],
                "discount_pct": ["discount_pct", "discountvalue", "discount_percentage"],
                "line_total": ["line_total", "linetotal", "lineamount", "line_total_amount"],
                # "profit": ["profit", "line_profit"],
                # "profit_margin": ["profit_margin", "profit_margin_pct", "margin_pct"],
                "primary_reason": ["primary_reason", "reason", "source"],
                "tags": ["tags", "recommendation_tags"],
            }
            for field_name in [
                "name",
                "product_retailer_id",
                "price",
                "base_price",
                "final_price",
                "discount_value",
                "discount_value_line",
                "discount_pct",
                "line_total",
                # "profit",
                # "profit_margin",
                "tags",
                "primary_reason",
            ]:
                val = None
                if isinstance(pricing_hint, dict):
                    for alias in field_aliases.get(field_name, [field_name]):
                        val = pricing_hint.get(alias)
                        if val is not None:
                            break
                if val is None and isinstance(item, dict):
                    for alias in field_aliases.get(field_name, [field_name]):
                        val = item.get(alias)
                        if val is not None:
                            break

                if field_name in {"name", "product_retailer_id", "tags", "primary_reason"}:
                    if val is not None:
                        op[field_name] = val
                    continue

                coerced = _safe_float(val)
                if coerced is not None:
                    op[field_name] = coerced

            op_qty = _coerce_qty(op.get("qty"))
            unit_discount = _safe_float(op.get("discount_value"))
            line_discount = _safe_float(op.get("discount_value_line"))
            if unit_discount is None and line_discount is not None and op_qty > 0:
                op["discount_value"] = round(float(line_discount) / float(op_qty), 4)
            if line_discount is None and unit_discount is not None and op_qty > 0:
                op["discount_value_line"] = round(float(unit_discount) * float(op_qty), 2)

            operations.append(op)

        if not operations:
            return True

        resp = agentflo_cart_tool({"user_id": user_id, "store_id": store_id, "operations": operations})
        ok = bool(isinstance(resp, dict) and resp.get("ok"))

        if not ok:
            logger.warning(
                "order_draft.update.failed",
                user_id=user_id,
                errors=resp.get("errors") if isinstance(resp, dict) else None,
                warnings=resp.get("warnings") if isinstance(resp, dict) else None,
            )
        else:
            if isinstance(resp, dict) and resp.get("warnings"):
                logger.warning(
                    "order_draft.update.warnings",
                    user_id=user_id,
                    warnings=resp.get("warnings"),
                )
            logger.info("order_draft.updated", user_id=user_id, store_id=store_id, op_count=len(operations))
        return ok

    except Exception as e:
        logger.error("order_draft.update.error", user_id=user_id, error=str(e))
        return False

from agents.tools.cart_tools import agentflo_cart_tool

def get_cart(
    user_id: str,
    store_id: Optional[str] = None,
) -> dict:
    """
    Fetch the latest cart snapshot using agentflo_cart_tool with GET_CART.
    Falls back to customer lookup to resolve store_id if not provided.
    NOW ENRICHES WITH PRODUCT NAMES and PRICING from search_products_by_sku.

    Robust SKU matching: normalizes (strip/lower) SKU keys so "SKU00905" == " sku00905 ".
    """
    if not user_id:
        return {}

    resolved_tenant = TENANT_ID
    resolved_store = _resolve_store_id_for_cart(user_id, store_id)

    if not resolved_tenant:
        logger.warning("order_draft.get_cart.missing_tenant", user_id=user_id)
        return {}

    try:
        resp = agentflo_cart_tool(
            {
                "user_id": user_id,
                "store_id": resolved_store,
                "operations": [{"op": "GET_CART"}],
            }
        ) or {}
    except Exception as e:
        logger.warning("order_draft.get_cart.failed", user_id=user_id, error=str(e))
        return {}

    if isinstance(resp, dict):
        cart = resp.get("cart") or {}
        # Backward compatibility: if canonical store path is empty, try legacy user_id store path.
        if (
            isinstance(cart, dict)
            and not (cart.get("items") or [])
            and str(resolved_store).strip() != str(user_id).strip()
        ):
            try:
                legacy_resp = agentflo_cart_tool(
                    {
                        "user_id": user_id,
                        "store_id": str(user_id),
                        "operations": [{"op": "GET_CART"}],
                    }
                ) or {}
                legacy_cart = legacy_resp.get("cart") if isinstance(legacy_resp, dict) else {}
                if isinstance(legacy_cart, dict) and (legacy_cart.get("items") or []):
                    logger.info(
                        "order_draft.get_cart.legacy_fallback_used",
                        user_id=user_id,
                        resolved_store=resolved_store,
                    )
                    cart = legacy_cart
            except Exception as e:
                logger.warning("order_draft.get_cart.legacy_fallback_failed", user_id=user_id, error=str(e))
        if isinstance(cart, dict):
            cart.setdefault("tenant_id", resolved_tenant)
            cart.setdefault("store_id", resolved_store)

            # ✅ NEW: Enrich cart items with product names AND pricing
            items = cart.get("items") or []
            if items:
                # Collect all SKU codes
                sku_codes = []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    sku_raw = (
                        item.get("sku_code")
                        or item.get("sku")
                        or item.get("skucode")
                        or item.get("sku_id")
                        or item.get("item_number")
                    )
                    # If cart contains Meta retailer/content id (e.g. "7"), map to internal sku_code
                    # so downstream product lookup and rendering use proper names/prices.
                    sku_raw = _maybe_map_retailer_id_sku(sku_raw)
                    sku_norm = _normalize_sku_code(sku_raw)
                    if sku_norm:
                        sku_codes.append(sku_norm)
                    elif sku_raw is not None:
                        logger.warning(
                            "cart.lookup.invalid_sku_skipped",
                            user_id=user_id,
                            raw_sku=str(sku_raw),
                            name=item.get("name"),
                        )

                if sku_codes:
                    # Keep order stable while de-duplicating
                    seen = set()
                    sku_codes = [s for s in sku_codes if not (s in seen or seen.add(s))]

                if sku_codes:
                    try:
                        # Fetch product details from API
                        from agents.tools.api_tools import (
                            search_products_by_sku,
                            unwrap_tool_response,
                        )

                        product_resp = search_products_by_sku(sku_codes)
                        ok, product_data, err = unwrap_tool_response(
                            product_resp,
                            system_name="search_products_by_sku",
                        )

                        # Debug: Log what we got from the API (wrapper vs data)
                        logger.info(
                            "cart.product_lookup_response",
                            user_id=user_id,
                            ok=ok,
                            has_data=bool(product_data),
                            data_type=type(product_data).__name__ if product_data else None,
                            top_level_keys=list(product_data.keys())[:10]
                            if isinstance(product_data, dict)
                            else None,
                            list_len=len(product_data) if isinstance(product_data, list) else None,
                            first_entry_keys=(list(product_data[0].keys())[:30] if isinstance(product_data, list) and product_data and isinstance(product_data[0], dict) else None),
                            error=err,
                        )

                        if ok and product_data:
                            # ------------------------------------------------------------------
                            # FIX: product_data might be a wrapper dict (success/message/data),
                            #      a list of product dicts, OR an already-built sku->details map.
                            #      We normalize all of them into product_map[norm_sku] = details.
                            # ------------------------------------------------------------------
                            products_obj = product_data

                            # If it's a wrapper dict with "data", use that.
                            if isinstance(products_obj, dict) and "data" in products_obj:
                                products_obj = products_obj.get("data")

                            # Unwrap common list wrappers e.g. {"products": [...]}
                            if isinstance(products_obj, dict):
                                for key in ("products", "items", "result"):
                                    val = products_obj.get(key)
                                    if isinstance(val, list):
                                        products_obj = val
                                        break

                            product_map: dict[str, dict] = {}

                            def _extract_product_record(raw_obj: Any) -> Optional[dict]:
                                if not isinstance(raw_obj, dict):
                                    return None
                                # Common wrappers seen across product endpoints
                                for key in ("product", "item", "details", "value", "data"):
                                    nested = raw_obj.get(key)
                                    if isinstance(nested, dict):
                                        return nested
                                return raw_obj

                            def _extract_sku_from_product(prod_obj: dict) -> Optional[str]:
                                for key in (
                                    "sku",
                                    "sku_code",
                                    "skucode",
                                    "skuCode",
                                    "SKU",
                                    "SKUCode",
                                    "item_number",
                                    "itemNumber",
                                    "variant_code",
                                    "id",
                                ):
                                    val = prod_obj.get(key)
                                    if val is None:
                                        continue
                                    token = str(val).strip()
                                    if token:
                                        return token
                                return None

                            if isinstance(products_obj, list):
                                for p in products_obj:
                                    p_obj = _extract_product_record(p)
                                    if not isinstance(p_obj, dict):
                                        continue
                                    sku = _extract_sku_from_product(p_obj)
                                    if not sku:
                                        if isinstance(p, dict):
                                            logger.warning(
                                                "cart.product_entry_missing_sku",
                                                entry_keys=list(p.keys())[:20],
                                                nested_keys=list(p_obj.keys())[:20] if isinstance(p_obj, dict) else None,
                                            )
                                        continue
                                    norm_key = str(sku).strip().lower()
                                    product_map[norm_key] = p_obj

                                    # Debug: Log product structure
                                    logger.info(
                                        "cart.product_map_entry",
                                        sku=sku,
                                        norm_sku=norm_key,
                                        has_name=bool(
                                            p_obj.get("official_name")
                                            or p_obj.get("product_name")
                                            or p_obj.get("name")
                                            or p_obj.get("sku_name")
                                        ),
                                        has_pricing=bool(p_obj.get("pricing")),
                                        pricing_keys=list(p_obj.get("pricing", {}).keys())
                                        if isinstance(p_obj.get("pricing"), dict)
                                        else None,
                                    )

                            elif isinstance(products_obj, dict):
                                # Could be sku->details map OR still a wrapper-like dict without "data"
                                for sku, details in products_obj.items():
                                    details_obj = _extract_product_record(details)
                                    if not isinstance(details_obj, dict):
                                        continue
                                    sku_from_details = _extract_sku_from_product(details_obj)
                                    sku_key = sku_from_details or str(sku).strip()
                                    if not sku_key:
                                        continue
                                    norm_key = str(sku_key).strip().lower()
                                    product_map[norm_key] = details_obj

                                    logger.info(
                                        "cart.product_map_entry",
                                        sku=sku_key,
                                        norm_sku=norm_key,
                                        has_name=bool(
                                            details_obj.get("official_name")
                                            or details_obj.get("product_name")
                                            or details_obj.get("name")
                                            or details_obj.get("sku_name")
                                        ),
                                        has_pricing=bool(details_obj.get("pricing")),
                                        pricing_keys=list(details_obj.get("pricing", {}).keys())
                                        if isinstance(details_obj.get("pricing"), dict)
                                        else None,
                                    )

                            else:
                                logger.warning(
                                    "cart.product_lookup_unexpected_shape",
                                    user_id=user_id,
                                    products_obj_type=type(products_obj).__name__,
                                )

                            if not product_map:
                                sample_entry_keys = None
                                sample_nested_keys = None
                                if isinstance(products_obj, list) and products_obj:
                                    first = products_obj[0]
                                    if isinstance(first, dict):
                                        sample_entry_keys = list(first.keys())[:30]
                                        nested = first.get("product") or first.get("item") or first.get("details") or first.get("data")
                                        if isinstance(nested, dict):
                                            sample_nested_keys = list(nested.keys())[:30]
                                logger.warning(
                                    "cart.product_lookup_empty_map",
                                    user_id=user_id,
                                    products_obj_type=type(products_obj).__name__,
                                    sample_entry_keys=sample_entry_keys,
                                    sample_nested_keys=sample_nested_keys,
                                )

                            # Enrich each cart item
                            enriched_count = 0
                            for item in items:
                                if not isinstance(item, dict):
                                    continue

                                # Normalize sku in-place if it came in as retailer/content id
                                sku = item.get("sku_code") or item.get("sku")
                                mapped_sku = _maybe_map_retailer_id_sku(sku)
                                if mapped_sku and mapped_sku != sku:
                                    item["sku_code"] = mapped_sku
                                    sku = mapped_sku
                                sku_lookup = str(sku).strip().lower() if sku else None

                                if not sku_lookup or sku_lookup not in product_map:
                                    logger.warning(
                                        "cart.sku_not_in_product_map",
                                        sku=sku,
                                        sku_lookup=sku_lookup,
                                        available_skus=list(product_map.keys())[:50],
                                    )
                                    continue

                                product = product_map[sku_lookup]

                                # Set name if missing - CHECK MULTIPLE POSSIBLE FIELDS
                                name_val = item.get("name")
                                has_meaningful_name = (
                                    isinstance(name_val, str)
                                    and name_val.strip()
                                    and name_val.strip().lower() not in {"item", "items"}
                                )
                                if not has_meaningful_name:
                                    item["name"] = (
                                        product.get("official_name")
                                        or product.get("product_name")
                                        or product.get("name")
                                        or product.get("sku_name")
                                        or product.get("description")
                                        or product.get("description_en")
                                        or product.get("sku_desc")
                                        or "Item"
                                    )
                                    logger.info(
                                        "cart.name_set",
                                        sku=sku,
                                        sku_lookup=sku_lookup,
                                        name=item.get("name"),
                                        source="product_api",
                                    )
                                    enriched_count += 1

                                # Ensure pricing fields exist
                                pricing = product.get("pricing") or {}
                                if isinstance(pricing, dict):
                                    if _safe_float(item.get("base_price")) is None:
                                        base = (
                                            pricing.get("base_price")
                                            or pricing.get("consumer_price")
                                            or pricing.get("mrp")
                                            or pricing.get("list_price")
                                            or pricing.get("unit_price")
                                            or pricing.get("price")
                                        )
                                        if _safe_float(base) is not None:
                                            item["base_price"] = base
                                            logger.info(
                                                "cart.base_price_set",
                                                sku=sku,
                                                sku_lookup=sku_lookup,
                                                base_price=base,
                                            )

                                    if _safe_float(item.get("final_price")) is None:
                                        final = (
                                            pricing.get("final_price")
                                            or pricing.get("discounted_price")
                                            or pricing.get("total_buy_price_virtual_pack")
                                            or pricing.get("unit_price")
                                            or pricing.get("price")
                                        )
                                        if _safe_float(final) is not None:
                                            item["final_price"] = final
                                            logger.info(
                                                "cart.final_price_set",
                                                sku=sku,
                                                sku_lookup=sku_lookup,
                                                final_price=final,
                                            )

                                    if _safe_float(item.get("line_total")) is None:
                                        qty = item.get("qty") or 1
                                        final_price = _safe_float(item.get("final_price"))
                                        if final_price is not None:
                                            item["line_total"] = float(final_price) * float(qty)
                                            logger.info(
                                                "cart.line_total_calculated",
                                                sku=sku,
                                                sku_lookup=sku_lookup,
                                                line_total=item["line_total"],
                                            )

                            logger.info(
                                "cart.enriched_with_names",
                                user_id=user_id,
                                enriched_count=enriched_count,
                                total_items=len(items),
                            )
                        else:
                            logger.warning(
                                "cart.product_lookup_failed",
                                user_id=user_id,
                                ok=ok,
                                error=err,
                            )

                    except Exception as e:
                        logger.warning(
                            "cart.name_enrichment_failed",
                            user_id=user_id,
                            error=str(e),
                        )

            return cart

    return {}

def get_order_draft(tenant_id: Optional[str] = None, user_id: Optional[str] = None, store_id: Optional[str] = None) -> dict:
    """
    Legacy wrapper kept for compatibility; delegates to agentflo_cart_tool GET_CART.
    Accepts either (user_id) only or (tenant_id, user_id, store_id).
    """
    # Handle call style get_order_draft(user_id)
    if user_id is None and tenant_id is not None and store_id is None:
        user_id = tenant_id
        tenant_id = None

    return get_cart(user_id=user_id or "", store_id=store_id)


def delete_order_draft(user_id: str, store_id: Optional[str] = None) -> bool:
    """Delete/clear the cart via agentflo_cart_tool, and clean up legacy storage."""
    resolved_store = _resolve_store_id_for_cart(user_id, store_id)
    ok = False
    try:
        resp = agentflo_cart_tool(
            {"user_id": user_id, "store_id": resolved_store, "operations": [{"op": "CLEAR_CART"}]}
        )
        ok = bool(isinstance(resp, dict) and resp.get("ok"))
    except Exception as e:
        logger.error("order_draft.delete.error", user_id=user_id, error=str(e))

    try:
        user_ref = _user_ref(user_id)
        user_ref.update({'order_drafts': firestore.DELETE_FIELD})
    except Exception as e:
        logger.warning("order_draft.legacy_delete.error", user_id=user_id, error=str(e))

    if ok:
        logger.info("order_draft.deleted", user_id=user_id, store_id=resolved_store)
    return ok

def get_user_data(user_id: str) -> dict:
    """Get complete user document from Firestore"""
    try:
        user_ref = _user_ref(user_id)
        doc = user_ref.get()

        if doc.exists:
            user_data = doc.to_dict()
            if user_data is not None:
                return user_data
            raise ValueError("User document exists but data is None")
        else:
            raise ValueError(f"User not found: {user_id}")
    except Exception as e:
        print(f"Error reading from Firestore: {e}")
        raise

def write_user_data(user_id: str, user_data: dict, merge: bool = True) -> bool:
    """Write user data to Firestore"""
    try:
        user_ref = _user_ref(user_id)
        user_ref.set(user_data, merge=merge)
        return True
    except Exception as e:
        print(f"Error writing to Firestore: {e}")
        return False

from agents.tools.catalog_search import find_first_retailer_id  # <-- adjust import path
@llm_safe("order_draft.update")
# def send_cart_multi_product_message(user_id: str) -> str:
#     """
#     Sends a free interactive multi-product (product_list) message that shows the
#     current cart as a WhatsApp catalog list, so user can visually edit items.

#     Returns:
#         str: "" on success so the LLM doesn't echo extra text.
#              Otherwise, a user-facing Urdu/English error message.
#     """
#     from dotenv import load_dotenv
#     load_dotenv()

#     WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
#     WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")
#     WHATSAPP_CATALOG_ID = os.getenv("WHATSAPP_CATALOG_ID")

#     if not (WHATSAPP_PHONE_NUMBER_ID and WHATSAPP_ACCESS_TOKEN and WHATSAPP_CATALOG_ID):
#         logger.error(
#             "cart_mpm.missing_creds",
#             user_id=user_id,
#             have_phone=bool(WHATSAPP_PHONE_NUMBER_ID),
#             have_token=bool(WHATSAPP_ACCESS_TOKEN),
#             have_catalog=bool(WHATSAPP_CATALOG_ID),
#         )
#         # 🔹 NEW: fallback to generic catalogue instead of pure text
#         try:
#             send_product_catalogue(user_id)
#             return ""  # generic catalog sent, no extra text
#         except Exception as e:
#             logger.error("cart_mpm.catalog_fallback_error", user_id=user_id, error=str(e))
#             return (
#                 "Bhai, cart ko WhatsApp catalog ki tarah bhejne mein issue aa gaya hai "
#                 "(internal settings missing). Aapka order draft safe hai, "
#                 "main text summary se hi confirm kar sakti hun."
#             )

#     # 1) Get current draft
#     try:
#         draft_data = get_order_draft(user_id) or {}
#     except Exception as e:
#         logger.error("cart_mpm.draft_read_error", user_id=user_id, error=str(e))
#         return (
#             "Mujhe aapka current cart read karne mein issue aa gaya hai. "
#             "Thori der baad phir try karein ya items dobara bhej dein."
#         )

#     if not draft_data:
#         return (
#             "Bhai, abhi aapke cart mein koi items nahi hain. "
#             "Pehlay products add kar lein phir main catalog bhejti hun."
#         )

#     try:
#         draft = OrderDraft.model_validate(draft_data)
#     except Exception as e:
#         logger.error("cart_mpm.draft_validation_error", user_id=user_id, error=str(e))
#         return (
#             "Mujhe aapke cart ka data theek se samajh nahi aa raha. "
#             "Ek dafa items dobara share kar dein ya thora adjust kar ke bhej dein."
#         )

#     # 2) Build product_items using stored or looked-up retailer IDs
#     product_items: list[dict[str, str]] = []
#     missing_after_lookup: list[str] = []
#     looked_up: list[dict] = []

#     for item in draft.skus:
#         retailer_id = getattr(item, "product_retailer_id", None)
#         sku = getattr(item, "sku_code", None) or "UNKNOWN"
#         name = getattr(item, "name", "") or ""

#         if not retailer_id:
#             query = name or sku
#             try:
#                 rid = find_first_retailer_id(query)  # returns retailer_id or None
#             except Exception as e:
#                 logger.error(
#                     "cart_mpm.catalog_lookup_error",
#                     user_id=user_id,
#                     sku_code=sku,
#                     name=name,
#                     error=str(e),
#                 )
#                 rid = None

#             if rid:
#                 retailer_id = rid
#                 looked_up.append({"sku_code": sku, "name": name, "retailer_id": rid})
#             else:
#                 missing_after_lookup.append(sku)

#         if not retailer_id:
#             continue

#         product_items.append({"product_retailer_id": str(retailer_id)})

#     if looked_up:
#         logger.info(
#             "cart_mpm.catalog_ids_populated",
#             user_id=user_id,
#             items=looked_up,
#         )

#     if missing_after_lookup and product_items:
#         logger.warning(
#             "cart_mpm.partial_missing_retailer_id",
#             user_id=user_id,
#             missing_sku_codes=missing_after_lookup,
#         )

#     # 🔹 KEY CHANGE: if no mappable items → fallback to generic catalogue
#     if not product_items:
#         logger.info(
#             "cart_mpm.no_mappable_items_for_product_list",
#             user_id=user_id,
#             missing_sku_codes=missing_after_lookup,
#         )
#         try:
#             send_product_catalogue(user_id)
#             return ""  # generic catalog sent; nothing for LLM to echo
#         except Exception as e:
#             logger.error("cart_mpm.catalog_fallback_error", user_id=user_id, error=str(e))
#             return (
#                 "Bhai, cart ko catalog ki form mein dikhane ke liye mujhe WhatsApp "
#                 "product IDs nahi mil rahe. Lekin tension nahi, main text summary se "
#                 "hi order confirm kar sakti hun."
#             )

#     url = f"https://graph.facebook.com/v23.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
#     headers = {
#         "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
#         "Content-Type": "application/json",
#     }

#     payload = {
#         "messaging_product": "whatsapp",
#         "recipient_type": "individual",
#         "to": user_id,
#         "type": "interactive",
#         "interactive": {
#             "type": "product_list",
#             "header": {
#                 "type": "text",
#                 "text": "Peek Freans Cart"
#             },
#             "body": {
#                 "text": "Yahan se items add/remove aur quantity adjust kar sakte hain"
#             },
#             "footer": {
#                 "text": "Catalog items only"
#             },
#             "action": {
#                 "catalog_id": str(WHATSAPP_CATALOG_ID),
#                 "sections": [
#                     {
#                         "title": "Current Order",
#                         "product_items": product_items,
#                     }
#                 ],
#             },
#         },
#     }

#     logger.info("cart_mpm.payload", user_id=user_id, payload=payload)

#     try:
#         resp = requests.post(url, headers=headers, json=payload, timeout=15)
#         logger.info(
#             "cart_mpm.response",
#             user_id=user_id,
#             status=resp.status_code,
#             body=resp.text[:500]
#         )
#         resp.raise_for_status()
#         logger.info("cart_mpm.sent", user_id=user_id, status=resp.status_code)
#         return ""
#     except requests.exceptions.HTTPError as e:
#         error_body = ""
#         try:
#             error_body = e.response.text if e.response else ""
#         except Exception:
#             pass
#         logger.error(
#             "cart_mpm.send_error",
#             user_id=user_id,
#             error=str(e),
#             status_code=getattr(e.response, 'status_code', None),
#             response_body=error_body[:500]
#         )
#         # 🔹 NEW: last-chance generic catalogue
#         try:
#             send_product_catalogue(user_id)
#             return ""
#         except Exception:
#             return (
#                 "Cart ko catalog ki form mein bhejne mein error aa gaya hai bhai... "
#                 "Main text mein hi cart summary bhej deti hun."
#             )
#     except Exception as e:
#         logger.error("cart_mpm.send_error", user_id=user_id, error=str(e))
#         try:
#             send_product_catalogue(user_id)
#             return ""
#         except Exception:
#             return (
#                 "Cart ko catalog ki form mein bhejne mein error aa gaya hai bhai... "
#                 "Main text mein hi cart summary bhej deti hun."
#             )


def _button_already_clicked(user_id: str, button_id: str) -> bool:
    """Check if user already clicked this button in current order session."""
    try:
        doc_ref = _user_ref(user_id).collection("button_clicks").document(button_id)
        doc = doc_ref.get()
        if doc.exists:
            data = doc.to_dict() or {}
            # 5 minute TTL for button clicks
            if (time.time() - data.get("ts", 0)) < 300:
                return True
        return False
    except Exception as e:
        logger.warning("button_click.check_failed", user_id=user_id, error=str(e))
        return False  # fail open

def _mark_button_clicked(user_id: str, button_id: str):
    """Mark that user clicked this button."""
    try:
        doc_ref = _user_ref(user_id).collection("button_clicks").document(button_id)
        doc_ref.set({"ts": time.time(), "button_id": button_id}, merge=True)
    except Exception as e:
        logger.warning("button_click.mark_failed", user_id=user_id, error=str(e))



def send_product_catalogue(user_id: str, session_id: Optional[str] = None) -> str:
    """
    Sends a WhatsApp interactive multi-product catalog message (product_list)
    with section titles for ECOCem™, ECODrymix™ and ECOConcrete™.

    Returns:
        "" on success so the agent doesn't echo internal text.
    """
    # Guardrail: avoid duplicate sends when we've already sent recently (e.g., auto + tool back-to-back)
    try:
        cooldown = int(os.getenv("CATALOG_COOLDOWN_SEC", "15"))
    except Exception:
        cooldown = 15
    try:
        last_ts = _sessions.get_last_catalog_sent_at(user_id)
        if last_ts and (time.time() - last_ts) < max(0, cooldown):
            logger.info(
                "catalog.send.skipped_recent",
                user_id=user_id,
                seconds_since=round(time.time() - last_ts, 1),
            )
            return ""
    except Exception as e:
        logger.warning("catalog.cooldown_check_failed", user_id=user_id, error=str(e))

    phone_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID") or WA_PHONE_NUMBER_ID
    token = os.getenv("WHATSAPP_ACCESS_TOKEN") or WA_ACCESS_TOKEN
    # Prefer env overrides, but fall back to the known YTL catalog id so
    # autosend works even when ENGRO_CATALOG_ID is not set.
    catalog_id = (
        os.getenv("WHATSAPP_CATALOG_ID")
        or os.getenv("ENGRO_CATALOG_ID")
        or os.getenv("CATALOG_ID")
        or "1381891263693403"
    )

    if not (phone_id and token and catalog_id):
        logger.warning(
            "catalog.send.skip_missing_creds",
            user_id=user_id,
            have_phone=bool(phone_id),
            have_token=bool(token),
            have_catalog=bool(catalog_id),
        )
        raise ValueError("Missing WhatsApp catalog credentials (phone/token/catalog_id).")

    url = f"{WHATSAPP_API_URL}/{phone_id}/messages"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # WhatsApp Cloud API: interactive multi-product list (grouped sections).
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": str(user_id),
        "type": "interactive",
        "interactive": {
            "type": "product_list",
            "header": {
                "type": "text",
                "text": "YTL Cement Catalog",
            },
            "body": {
                "text": "Here's our full range of products.",
            },
            "footer": {
                "text": "Tap a section to see products.",
            },
            "action": {
                "catalog_id": str(catalog_id),
                "sections": [
                    {
                        "title": "ECOCem – Cement",
                        "product_items": [
                            {"product_retailer_id": "1"},
                            {"product_retailer_id": "2"},
                            {"product_retailer_id": "3"},
                            {"product_retailer_id": "4"},
                            {"product_retailer_id": "5"},
                        ],
                    },
                    {
                        "title": "ECODrymix – Mortars",
                        "product_items": [
                            {"product_retailer_id": "6"},
                            {"product_retailer_id": "7"},
                            {"product_retailer_id": "8"},
                            {"product_retailer_id": "9"},
                            {"product_retailer_id": "10"},
                        ],
                    },
                    {
                        "title": "ECOConcrete – Systems",
                        "product_items": [
                            {"product_retailer_id": "11"},
                            {"product_retailer_id": "12"},
                        ],
                    },
                ],
            },
        },
    }

    def _send(p: Dict[str, Any]):
        return _post_with_retries(
            url,
            json_payload=p,
            headers=headers,
            connect_timeout=5.0,
            read_timeout=15.0,
            max_attempts=2,
            base_backoff=0.6,
            raise_for_status=False,
        )

    resp = _send(payload)

    if not (200 <= resp.status_code < 300):
        err_msg = None
        err_code = None
        err_subcode = None
        err_details = None
        try:
            j = resp.json() if (resp.text or "").strip().startswith("{") else {}
            if isinstance(j, dict):
                e = j.get("error") or {}
                if isinstance(e, dict):
                    err_msg = e.get("message") or e.get("error_user_msg")
                    err_code = e.get("code")
                    err_subcode = e.get("error_subcode")
                    ed = e.get("error_data") or {}
                    if isinstance(ed, dict):
                        err_details = ed.get("details")
        except Exception:
            pass

        # If the thumbnail_product_retailer_id is wrong / not present in the connected FB catalog,
        # WhatsApp returns 131009 with details like "Products not found in FB Catalog".
        # In that case retry once WITHOUT a thumbnail so WhatsApp can pick a default.
        if (
            resp.status_code == 400
            and thumb_id
            and (err_code == 131009 or str(err_code) == "131009")
            and isinstance(err_details, str)
            and "products not found" in err_details.lower()
        ):
            logger.warning(
                "catalog.send.retry_without_thumbnail",
                user_id=user_id,
                thumbnail_product_retailer_id=str(thumb_id),
                err_details=err_details,
            )
            payload_no_thumb = {
                **payload,
                "interactive": {
                    **payload.get("interactive", {}),
                    "action": {"name": "catalog_message"},
                },
            }
            resp2 = _send(payload_no_thumb)
            if 200 <= resp2.status_code < 300:
                logger.info("catalog.sent", user_id=user_id, mode="catalog_message", retry="without_thumbnail")
                return ""
            # Fall through to error logging with resp2 details
            resp = resp2
            err_msg = err_code = err_subcode = err_details = None
            try:
                j = resp.json() if (resp.text or "").strip().startswith("{") else {}
                if isinstance(j, dict):
                    e = j.get("error") or {}
                    if isinstance(e, dict):
                        err_msg = e.get("message") or e.get("error_user_msg")
                        err_code = e.get("code")
                        err_subcode = e.get("error_subcode")
                        ed = e.get("error_data") or {}
                        if isinstance(ed, dict):
                            err_details = ed.get("details")
            except Exception:
                pass

        logger.error(
            "catalog.send.failed",
            user_id=user_id,
            catalog_id=str(catalog_id),
            status=resp.status_code,
            err_code=err_code,
            err_subcode=err_subcode,
            err_msg=err_msg,
            err_details=err_details,
            body=(resp.text or "")[:400],
        )
        raise ValueError(
            f"Catalog send failed: HTTP {resp.status_code}"
            + (f" (code={err_code} subcode={err_subcode})" if err_code or err_subcode else "")
            + (f": {err_msg}" if err_msg else "")
        )

    logger.info("catalog.send.ok", user_id=user_id, catalog_id=str(catalog_id))
    _sessions.mark_catalog_sent(user_id, session_id)
    return ""
class SendCatalogueInput(BaseModel):
    """No extra fields; user_id is injected from runtime."""
    pass


@llm_safe("catalog.send")
def send_product_catalogue_tool(user_id: str) -> str:
    """
    Tool wrapper so the LLM can send the product catalogue explicitly.
    Returns empty string on success so the agent doesn't echo internal text.
    """
    try:
        _ = send_product_catalogue(user_id)
        return ""
    except Exception as e:
        logger.error("catalog.send.error", user_id=user_id, error=str(e))
        return "I couldn't send the catalogue right now. Please try again in a moment."


sendProductCatalogueTool = FunctionTool(
    func=send_product_catalogue_tool,
)

##### old version that uses paid template message
# def send_product_catalogue(user_id: str):
    # """
    # Sends the product catalogue to the user.
    # """
    # WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
    # url = f"https://graph.facebook.com/v23.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    # WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")

    # headers = {
    #     "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
    #     "Content-Type": "application/json"
    # }
    # to_number = user_id
    # data = {
    #     "messaging_product": "whatsapp",
    #     "recipient_type": "individual",
    #     "to": to_number,
    #     "type": "template",
    #     "template": {
    #         "name": "catalog_message",
    #         "language": {  
    #             "code": "en"
    #         },
    #         "components": [
    #             {
    #                 "type": "button",
    #                 "sub_type": "catalog",
    #                 "index": "1"
    #             }
    #         ]
    #     }
    # }

    # try:
    #     response = requests.post(url, headers=headers, json=data)
    #     response.raise_for_status()
    #     return "Product catalogue sent successfully."
    # except requests.exceptions.RequestException as e:
    #     detail = getattr(e.response, "text", None)
    #     status = getattr(getattr(e, "response", None), "status_code", None)
    #     print(f"Error sending catalogue: {e}")
    #     raise ValueError(f"Failed to send product catalogue. Status code: {status}, Response: {detail}")

# def send_order_confirmation_buttons(user_id: str, draft_summary_text: str) -> bool:
#     """
#     Sends a WhatsApp interactive button message to confirm the current order.
#     Buttons:
#       - ORDER_CONFIRM_YES
#       - ORDER_CONFIRM_NO

#     The route_handler already knows how to handle these in the 'interactive' branch.
#     """
#     if not (WA_PHONE_NUMBER_ID and WA_ACCESS_TOKEN):
#         logger.warning(
#             "wa.confirm_buttons.skip",
#             user_id=user_id,
#             reason="missing_creds",
#             have_phone=bool(WA_PHONE_NUMBER_ID),
#             have_token=bool(WA_ACCESS_TOKEN),
#         )
#         return False

#     body_text = (
#         "Theek hai bhai, aapke cart mein yeh items hain:\n\n"
#         f"{draft_summary_text}\n\n"
#         "Kya yeh final order hai?"
#     )

#     url = f"{WHATSAPP_API_URL}/{WA_PHONE_NUMBER_ID}/messages"
#     headers = {
#         "Authorization": f"Bearer {WA_ACCESS_TOKEN}",
#         "Content-Type": "application/json",
#     }
#     payload = {
#         "messaging_product": "whatsapp",
#         "to": user_id,
#         "type": "interactive",
#         "interactive": {
#             "type": "button",
#             "body": {"text": body_text},
#             "action": {
#                 "buttons": [
#                     {
#                         "type": "reply",
#                         "reply": {
#                             "id": "ORDER_CONFIRM_YES",
#                             "title": "Haan ✅",
#                         },
#                     },
#                     {
#                         "type": "reply",
#                         "reply": {
#                             "id": "ORDER_CONFIRM_NO",
#                             "title": "Nahi, change karna hai",
#                         },
#                     },
#                 ]
#             },
#         },
#     }

#     try:
#         resp = requests.post(url, headers=headers, json=payload, timeout=10)
#         resp.raise_for_status()
#         logger.info(
#             "wa.confirm_buttons.sent",
#             user_id=user_id,
#             status_code=resp.status_code,
#         )
#         return True
#     except requests.RequestException as e:
#         logger.error("wa.confirm_buttons.error", user_id=user_id, error=str(e))
#         return False


def send_image(to_number: str, image_url: str, caption=None) -> tuple[bool, Optional[str]]:
    """
    Sends an image to a specific WhatsApp number using the configured transport.
    Returns (sent_ok, message_id_or_none).
    """
    transport = (os.getenv("WHATSAPP_TRANSPORT", "meta") or "meta").strip().lower()
    if transport == "twilio":
        try:
            from agents.helpers.adk_helper import ADKHelper  # lazy import to avoid cycles
            helper = ADKHelper()
            sent, sid = helper._twilio_send_media(
                to_number,
                image_url,
                body=caption or "",
                content_type="image",
                return_sid=True,
            )
            if sent:
                logger.info("twilio.image.sent", to=to_number, sid=sid)
                return True, sid
            logger.error("twilio.image.send_failed", to=to_number)
            return False, None
        except Exception as e:
            logger.error("twilio.image.exception", to=to_number, error=str(e))
            raise

    elif transport=="meta":
    
        WHATSAPP_API_URL = "https://graph.facebook.com/v23.0"
        WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
        WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")

        if not WHATSAPP_PHONE_NUMBER_ID or not WHATSAPP_ACCESS_TOKEN:
            print("Could not send image.")
            return False

        url = f"{WHATSAPP_API_URL}/{WHATSAPP_PHONE_NUMBER_ID}/messages"
        headers = {
            "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        }
        data = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "image",
            "image": {"link": image_url, "caption": caption}
        }

        try:
            response = requests.post(url, headers=headers, data=json.dumps(data))
            response.raise_for_status()
            msg_id = None
            try:
                resp_json = response.json() or {}
                msg_id = (resp_json.get("messages") or [{}])[0].get("id")
            except Exception:
                msg_id = None
            logger.info(f"Image sent to {to_number}", to=to_number, status_code=response.status_code, msg_id=msg_id)
            if response.status_code == 200:
                return True, msg_id
            else:
                raise requests.exceptions.RequestException("Failed to send image")
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending WhatsApp image to {to_number}", error=e)
            raise

# ==============================================================================
# MODIFIED/NEW FUNCTIONS
# ==============================================================================
# def build_confirmation_message_for_current_draft(user_id: str) -> str:
#     """
#     Use this tool when the user indicates they want to place / confirm their order
#     (e.g. 'order place kar do', 'confirm my order', 'yehi final hai').

#     Behavior:
#     - If there is NO order draft or no items in it: returns a message telling the user
#       that their cart is empty and they should add items first.
#     - If there IS a draft with items: sends a WhatsApp interactive YES/NO confirmation
#       with the current cart summary, and returns a short status string for the LLM.
#     """
#     try:
#         draft_data = get_order_draft(user_id) or {}
#     except Exception as e:
#         logger.error("confirm_draft.read.error", user_id=user_id, error=str(e))
#         return (
#             "Mujhe aapka current cart read karne mein issue aa gaya hai. "
#             "Thori der baad phir try karein ya items dobara bhej dein."
#         )

#     # No draft stored
#     if not draft_data:
#         return (
#             "Bhai, abhi aapke cart mein koi items nahi hain. "
#             "Pehlay products add kar lein, phir mein order place karne mein help karungi."
#         )

#     # Validate with schema
#     try:
#         draft = OrderDraft.model_validate(draft_data)
#     except Exception as e:
#         logger.error("confirm_draft.validation.error", user_id=user_id, error=str(e))
#         return (
#             "Mujhe aapke cart ka data theek se samajh nahi aa raha hai. "
#             "Ek dafa items dobara share kar dein ya thora sa adjust kar ke bhej dein."
#         )

#     # Empty SKU list
#     if not draft.skus:
#         return (
#             "Bhai, abhi aapke cart mein koi items nahi hain. "
#             "Pehlay kuch products add karein, phir mein order place karungi."
#         )

#     # Non-empty cart → format and send interactive buttons
#     summary_text = _format_draft_for_reply(draft.model_dump())
#     sent = send_order_confirmation_buttons(user_id, summary_text)

#     if sent:
#         # This text goes *only* back into the LLM as tool result, not to WhatsApp directly.
#         return (
#             "Theek hai, confirmation ke liye YES/NO buttons bhej diye hain. "
#             "User jab button press karega to uske hisaab se next step lena."
#         )

#     # Fallback if WA interactive send failed → plain text confirmation
#     return (
#         "Theek hai bhai, aapke cart mein yeh items hain:\n\n"
#         f"{summary_text}\n\n"
#         "Kya yeh final order hai? Agar aap confirm karein (haan / yes / confirm), "
#         "toh mein yehi order place kar dungi. 🙂"
#     )


# def build_confirmation_message_for_current_draft(user_id: str) -> str:
#     """
#     Use this tool when the user indicates they want to place / confirm their order
#     (e.g. 'order place kar do', 'confirm my order', 'yehi final hai').

#     Behavior:
#     - If there is NO order draft or no items in it: returns a message telling the user
#       that their cart is empty and they should add items first.
#     - If there IS a draft with items: returns a WhatsApp-ready summary of the current
#       cart plus a clear confirmation question. This tool DOES NOT place the order.

#     The agent should then wait for a clear 'yes/confirm' style reply before calling
#     the actual placeOrderTool.
#     """
#     try:
#         draft_data = get_order_draft(user_id) or {}
#     except Exception as e:
#         logger.error("confirm_draft.read.error", user_id=user_id, error=str(e))
#         return "Mujhe aapka current cart read karne mein issue aa gaya hai. Thori der baad phir try karein ya items dobara bhej dein."

#     # No draft stored
#     if not draft_data:
#         return (
#             "Bhai, abhi aapke cart mein koi items nahi hain. "
#             "Pehlay products add kar lein, phir mein order place karne mein help karungi."
#         )

#     # Validate with schema
#     try:
#         draft = OrderDraft.model_validate(draft_data)
#     except Exception as e:
#         logger.error("confirm_draft.validation.error", user_id=user_id, error=str(e))
#         return (
#             "Mujhe aapke cart ka data theek se samajh nahi aa raha hai. "
#             "Ek dafa items dobara share kar dein ya thora sa adjust kar ke bhej dein."
#         )

#     # Empty SKU list
#     if not draft.skus:
#         return (
#             "Bhai, abhi aapke cart mein koi items nahi hain. "
#             "Pehlay kuch products add karein, phir mein order place karungi."
#         )

#     # Non-empty cart → format nicely
#     summary_text = _format_draft_for_reply(draft.model_dump())
#     return (
#         "Theek hai bhai, aapke cart mein yeh items hain:\n\n"
#         f"{summary_text}\n\n"
#         "Kya yeh final order hai? Agar aap confirm karein (haan / yes / confirm), "
#         "toh mein yehi order place kar dungi. 🙂"
#     )

def send_order_confirmation_buttons(user_id: str) -> str:
    """
    Sends a single WhatsApp interactive button message showing:
    - current draft items + total (in body text)
    - two buttons: YES (confirm), NO (edit)

    Returns:
        str: short status for LLM (empty string on success so it doesn’t echo).
    """
    from dotenv import load_dotenv
    load_dotenv()

    WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
    WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")

    if not WHATSAPP_PHONE_NUMBER_ID or not WHATSAPP_ACCESS_TOKEN:
        logger.error("confirm_template.missing_creds", user_id=user_id)
        return (
            "bhai, confirmation buttons bhejne mein issue aa gaya hai "
            "(credentials missing). aap simple 'haan' ya 'nahi' likh kar "
            "bhi reply kar sakte hain."
        )

    # 1) Get draft
    try:
        draft_data = _ensure_total_amount_field(get_cart(user_id) or {})
    except Exception as e:
        logger.error("confirm_template.draft_read_error", user_id=user_id, error=str(e))
        return (
            "mujhe aapka current cart read karne mein issue aa gaya hai. "
            "thori der baad phir try karein ya items dobara bhej dein."
        )

    if not draft_data:
        return (
            "bhai, abhi aapke cart mein koi items nahi hain. "
            "pehlay products add kar lein, phir main order place karne mein madad karungi."
        )

    try:
        draft = OrderDraft.model_validate(draft_data)
    except Exception as e:
        logger.error("confirm_template.draft_validation_error", user_id=user_id, error=str(e))
        return (
            "mujhe aapke cart ka data theek se samajh nahi aa raha hai. "
            "ek dafa items dobara share kar dein ya thora sa adjust kar ke bhej dein."
        )

    if not draft.items:
        return (
            "bhai, abhi aapke cart mein koi items nahi hain. "
            "pehlay kuch products add karein, phir main order place karungi."
        )

    summary_text = _render_order_draft_template_summary(draft.model_dump())

    body_text = (
        "theek hai, aapke cart mein yeh items hain:\n\n"
        f"{summary_text}\n\n"
        "kya yehi final order hai?"
    )

    url = f"https://graph.facebook.com/v23.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }
    data = {
        "messaging_product": "whatsapp",
        "to": user_id,
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {"text": body_text},
            "footer": {"text": "YES se confirm, NO se edit kar sakte hain."},
            "action": {
                "buttons": [
                    {
                        "type": "reply",
                        "reply": {
                            "id": "ORDER_CONFIRM_YES",
                            "title": "YES ✅",
                        },
                    },
                    {
                        "type": "reply",
                        "reply": {
                            "id": "ORDER_CONFIRM_NO",
                            "title": "NO ❌",
                        },
                    },
                ]
            },
        },
    }

    try:
        resp = requests.post(url, headers=headers, json=data, timeout=15)
        if resp.status_code in (200, 201):
            logger.info("confirm_template.sent", user_id=user_id)
            # IMPORTANT: return empty so model doesn’t send “buttons bhej diye…” text
            return ""
        else:
            logger.error(
                "confirm_template.send_failed",
                user_id=user_id,
                status=resp.status_code,
                body=resp.text[:300],
            )
            return (
                "confirmation buttons bhejne mein masla aa gaya hai bhai... "
                "aap simple 'haan' se confirm ya 'nahi' se edit bol dein."
            )
    except Exception as e:
        logger.error("confirm_template.exception", user_id=user_id, error=str(e))
        return (
            "confirmation buttons bhejne mein error aa gaya hai... "
            "aap 'haan' ya 'nahi' likh kar reply kar dein."
        )

def _digit_to_urdu(d: str) -> str:
    """Single digit to Urdu word (Roman Urdu)."""
    digits = {
        "0": "zero",
        "1": "ek",
        "2": "do",
        "3": "teen",
        "4": "char",
        "5": "panch",
        "6": "chhe",
        "7": "saat",
        "8": "aath",
        "9": "nau",
    }
    return digits.get(d, d)


def _int_to_urdu_words(num: int) -> str:
    """
    Convert integer to Roman Urdu number words.

    - Explicit mapping for 0–99 (to avoid 'tees teen' mistakes)
    - Supports 100+ using lakh / hazaar / sau composition
    """
    NUM_0_99 = {
        0: "zero",
        1: "ek",
        2: "do",
        3: "teen",
        4: "char",
        5: "panch",
        6: "chhe",
        7: "saat",
        8: "aath",
        9: "nau",
        10: "das",
        11: "gyarah",
        12: "barah",
        13: "terah",
        14: "chaudah",
        15: "pandrah",
        16: "solah",
        17: "satrah",
        18: "atharah",
        19: "unnees",
        20: "bees",
        21: "ikkees",
        22: "baees",
        23: "teis",
        24: "chaubees",
        25: "pachees",
        26: "chhabbees",
        27: "sattais",
        28: "atthaees",
        29: "untees",
        30: "tees",
        31: "iktees",
        32: "battis",
        33: "taintees",
        34: "chauntees",
        35: "paintees",
        36: "chhattis",
        37: "saintees",
        38: "arthees",
        39: "untaalees",
        40: "chalees",
        41: "iktaalees",
        42: "byaalees",
        43: "taintaalees",
        44: "chawalees",
        45: "pintaalees",
        46: "chhiyaalees",
        47: "saintaalees",
        48: "artaalees",
        49: "unchaas",
        50: "pachaas",
        51: "ikawan",
        52: "bawan",
        53: "tirpan",
        54: "chavan",
        55: "pachpan",
        56: "chhappan",
        57: "sattavan",
        58: "athavan",
        59: "unsath",
        60: "saath",
        61: "iksath",
        62: "basath",
        63: "tirsath",
        64: "chausath",
        65: "painsath",
        66: "chhiyaasath",
        67: "sadsath",
        68: "athasath",
        69: "unhattar",
        70: "sattar",
        71: "ikhattar",
        72: "bahattar",
        73: "tihattar",
        74: "chauhattar",
        75: "pachhattar",
        76: "chihattar",
        77: "sattattar",
        78: "athattar",
        79: "unasi",
        80: "assi",
        81: "ikiyasi",
        82: "biyasi",
        83: "tirasi",
        84: "churasi",
        85: "pachasi",
        86: "chiyasi",
        87: "sattasi",
        88: "athasi",
        89: "navvay",
        90: "nabbe",
        91: "ikyanve",
        92: "baanve",
        93: "tiryanve",
        94: "chouranve",
        95: "pachyanve",
        96: "chhiyanve",
        97: "sattayanve",
        98: "athyanve",
        99: "ninyanve",
    }

    if num == 0:
        return NUM_0_99[0]
    if num < 0:
        return "minus " + _int_to_urdu_words(-num)
    if num <= 99:
        return NUM_0_99.get(num, str(num))

    parts = []

    # Lakhs
    if num >= 100000:
        lakhs = num // 100000
        parts.append(_int_to_urdu_words(lakhs))
        parts.append("lakh")
        num %= 100000

    # Thousands
    if num >= 1000:
        thousands = num // 1000
        parts.append(_int_to_urdu_words(thousands))
        parts.append("hazaar")
        num %= 1000

    # Hundreds
    if num >= 100:
        hundreds = num // 100
        parts.append(_int_to_urdu_words(hundreds))
        parts.append("sau")
        num %= 100

    # Remaining 0–99
    if num > 0:
        parts.append(_int_to_urdu_words(num))

    return " ".join(parts).strip()

_DRAFT_REPLY_STRINGS = {
    "UR": {
        "format_error": "Maaf kijiye, cart ka data sahi tarah format nahi ho saka.",
        "currency": "Rs",
        "labels": {
            "price": "Rate",
            "item_total": "Item Total",
            "saving": "Bachat",
            "total_amount": "Total Amount",
            "total_saving": "Total Bachat",
        },
    },
    "EN": {
        "format_error": "Sorry, I couldn't format your cart due to an internal data error.",
        "currency": "Rs",
        "labels": {
            "price": "Price",
            "item_total": "Item Total",
            "saving": "Saving",
            "total_amount": "Total Amount",
            "total_saving": "Total Saving",
        },
    },
    "CN_MY": {
        "format_error": "抱歉，由于内部数据错误，无法格式化您的购物车。",
        "currency": "Ringet",
        "labels": {
            "price": "价格",
            "item_total": "商品小计",
            "saving": "节省",
            "total_amount": "总金额",
            "total_saving": "总节省",
        },
    },
    "BM": {
        "format_error": "Maaf, saya tidak dapat memformat troli anda kerana ralat data dalaman.",
        "currency": "Ringet",
        "labels": {
            "price": "Harga",
            "item_total": "Jumlah Item",
            "saving": "Penjimatan",
            "total_amount": "Jumlah Keseluruhan",
            "total_saving": "Jumlah Penjimatan",
        },
    },
    "AR": {
        "format_error": "عذرًا، لم أتمكن من تنسيق سلتك بسبب خطأ داخلي في البيانات.",
        "currency": "Riyal",
        "labels": {
            "price": "السعر",
            "item_total": "إجمالي العنصر",
            "saving": "التوفير",
            "total_amount": "الإجمالي",
            "total_saving": "إجمالي التوفير",
        },
    },
}

_PLACE_ORDER_STRINGS = {
    "UR": {
        "missing_store_code": "Error: Store code nahi mila. Order place nahi ho sakta.",
        "missing_endpoint": "Internal configuration error: SALESFLO_API_ENDPOINT set nahi hai.",
        "missing_company_code": "Internal configuration error: COMPANY_CODE set nahi hai.",
        "no_items": "Error: Aapke cart mein items nahi hain. Order se pehle items add karein.",
        "empty_cart": "Error: Aapka cart khali hai. Order se pehle items add karein.",
        "unexpected_response": "Order system se unexpected response aya. {diag}",
        "success": "Order place ho gaya! {msg}\n\nYeh aapka final order hai:\n{summary}",
        "delivery_notice_with_date": "Aapka order confirm ho gaya hai aur delivery ki tareekh yeh hai:\n*{delivery_date}*",
        "delivery_notice_followup": "Mazeed tafseelaat ke liye isi tareekh par humein message karein aur delivery driver ke baare mein poochein.",
        "failed": "Order place nahi ho saka. {reason}{diag}",
        "request_error": "Order system se rabta karte hue error aya. {diag}",
        "internal_error": "Order place karte hue internal error aa gaya: {error}",
        "reason_label": "Wajah",
    },
    "EN": {
        "missing_store_code": "Error: Store code was not provided. Cannot place order.",
        "missing_endpoint": "Internal configuration error: SALESFLO_API_ENDPOINT is not set.",
        "missing_company_code": "Internal configuration error: COMPANY_CODE is not set.",
        "no_items": "Error: You have no items in your cart. Please add items before placing an order.",
        "empty_cart": "Error: Your cart is empty. Please add items before placing an order.",
        "unexpected_response": "Order system returned an unexpected response. {diag}",
        "success": "Order placed successfully! {msg}\n\nHere is your final order:\n{summary}",
        "delivery_notice_with_date": "Your order has been confirmed and will be delivered to you on\n*{delivery_date}*",
        "delivery_notice_followup": "For further details, message us on this date to enquire about your delivery driver.",
        "failed": "Failed to place your order. {reason}{diag}",
        "request_error": "An error occurred while communicating with the order system. {diag}",
        "internal_error": "An unexpected internal error occurred while placing your order: {error}",
        "reason_label": "Reason",
    },
    "CN_MY": {
        "missing_store_code": "错误：未提供门店代码，无法下单。",
        "missing_endpoint": "内部配置错误：未设置 SALESFLO_API_ENDPOINT。",
        "missing_company_code": "内部配置错误：未设置 COMPANY_CODE。",
        "no_items": "错误：您的购物车没有商品，请先添加商品再下单。",
        "empty_cart": "错误：您的购物车为空，请先添加商品再下单。",
        "unexpected_response": "下单系统返回了异常响应。{diag}",
        "success": "订单已成功提交！{msg}\n\n以下是您的最终订单：\n{summary}",
        "delivery_notice_with_date": "您的订单已确认，预计送达日期为\n*{delivery_date}*",
        "delivery_notice_followup": "如需更多详情，请在该日期联系我们查询配送司机信息。",
        "failed": "下单失败。{reason}{diag}",
        "request_error": "与下单系统通信时出错。{diag}",
        "internal_error": "下单时发生内部错误：{error}",
        "reason_label": "原因",
    },
    "BM": {
        "missing_store_code": "Ralat: Kod kedai tidak diberikan. Tidak dapat buat pesanan.",
        "missing_endpoint": "Ralat konfigurasi dalaman: SALESFLO_API_ENDPOINT tidak ditetapkan.",
        "missing_company_code": "Ralat konfigurasi dalaman: COMPANY_CODE tidak ditetapkan.",
        "no_items": "Ralat: Tiada item dalam troli anda. Sila tambah item sebelum membuat pesanan.",
        "empty_cart": "Ralat: Troli anda kosong. Sila tambah item sebelum membuat pesanan.",
        "unexpected_response": "Sistem pesanan mengembalikan respons yang tidak dijangka. {diag}",
        "success": "Pesanan berjaya dibuat! {msg}\n\nIni pesanan akhir anda:\n{summary}",
        "delivery_notice_with_date": "Pesanan anda telah disahkan dan akan dihantar kepada anda pada\n*{delivery_date}*",
        "delivery_notice_followup": "Untuk maklumat lanjut, mesej kami pada tarikh ini untuk bertanya tentang pemandu penghantaran anda.",
        "failed": "Gagal membuat pesanan. {reason}{diag}",
        "request_error": "Ralat berlaku semasa berhubung dengan sistem pesanan. {diag}",
        "internal_error": "Ralat dalaman berlaku semasa membuat pesanan: {error}",
        "reason_label": "Sebab",
    },
    "AR": {
        "missing_store_code": "خطأ: لم يتم توفير رمز المتجر. لا يمكن تنفيذ الطلب.",
        "missing_endpoint": "خطأ في الإعدادات الداخلية: لم يتم ضبط SALESFLO_API_ENDPOINT.",
        "missing_company_code": "خطأ في الإعدادات الداخلية: لم يتم ضبط COMPANY_CODE.",
        "no_items": "خطأ: لا توجد عناصر في سلتك. يرجى إضافة عناصر قبل تنفيذ الطلب.",
        "empty_cart": "خطأ: سلتك فارغة. يرجى إضافة عناصر قبل تنفيذ الطلب.",
        "unexpected_response": "أعاد نظام الطلبات استجابة غير متوقعة. {diag}",
        "success": "تم تنفيذ الطلب بنجاح! {msg}\n\nهذا هو طلبك النهائي:\n{summary}",
        "delivery_notice_with_date": "تم تأكيد طلبك وسيتم توصيله إليك في\n*{delivery_date}*",
        "delivery_notice_followup": "لمزيد من التفاصيل، راسلنا في هذا التاريخ للاستفسار عن سائق التوصيل الخاص بك.",
        "failed": "تعذر تنفيذ الطلب. {reason}{diag}",
        "request_error": "حدث خطأ أثناء التواصل مع نظام الطلبات. {diag}",
        "internal_error": "حدث خطأ داخلي أثناء تنفيذ الطلب: {error}",
        "reason_label": "السبب",
    },
}

def _get_place_order_strings() -> dict:
    lang = _normalize_prompt_language(os.getenv("PROMPT_LANGUAGE"))
    return _PLACE_ORDER_STRINGS.get(lang, _PLACE_ORDER_STRINGS["UR"])

def _get_draft_reply_strings() -> dict:
    lang = _normalize_prompt_language(os.getenv("PROMPT_LANGUAGE"))
    return _DRAFT_REPLY_STRINGS.get(lang, _DRAFT_REPLY_STRINGS["UR"])

def _localize_draft_line(line: str, labels: dict) -> str:
    if line.startswith("Price:"):
        return line.replace("Price:", f"{labels['price']}:", 1)
    if line.startswith("Item Total:"):
        localized = line.replace("Item Total:", f"{labels['item_total']}:", 1)
        return localized.replace("Saving:", f"{labels['saving']}:", 1)
    return line

def _localize_draft_lines(lines: list[str], labels: dict) -> list[str]:
    return [_localize_draft_line(line, labels) for line in lines]


def _render_order_draft_template_summary(draft_data: dict) -> str:
    """
    Render cart summary through order_draft_template for uniform formatting.
    Returns only the first message segment when template output is multi-part.
    Falls back to legacy formatter if template rendering fails.
    """
    try:
        rendered = order_draft_template(cart=draft_data)
        if isinstance(rendered, str) and rendered.strip():
            if MULTI_MESSAGE_DELIMITER in rendered:
                parts = [
                    part.strip()
                    for part in rendered.split(MULTI_MESSAGE_DELIMITER)
                    if isinstance(part, str) and part.strip()
                ]
                if parts:
                    return parts[0]
            return rendered.strip()
    except Exception as e:
        logger.warning("order_draft.template_render_failed", error=str(e))
    return _format_draft_for_reply(draft_data)


def _format_draft_for_reply(draft_data: dict) -> str:
    """\
    Format the final order draft into a user-friendly, WhatsApp-ready string.

    - Uses SELL price for display (final_price → price → base_price)
    - Shows quantity both numerically and in Roman Urdu
    - Uses discount_value / discount_pct / line_total from the new schema
    """
    strings = _get_draft_reply_strings()
    labels = strings["labels"]
    currency_label = strings["currency"]
    totals_raw = draft_data.get("totals") if isinstance(draft_data, dict) and isinstance(draft_data.get("totals"), dict) else {}
    discount_total_fallback = None
    if isinstance(draft_data, dict):
        discount_candidates = [
            draft_data.get("discount_total"),
            draft_data.get("total_discount"),
            totals_raw.get("discount_total"),
            totals_raw.get("total_discount"),
            totals_raw.get("discount"),
        ]
        for candidate in discount_candidates:
            cand_val = _safe_float(candidate)
            if cand_val is not None:
                discount_total_fallback = cand_val
                break
    try:
        # Normalize dict -> OrderDraft (validates & gives .items objects)
        draft = OrderDraft.model_validate(_ensure_total_amount_field(draft_data))
    except Exception as e:
        logger.error(
            "format_draft.validation.error error=%s draft_type=%s",
            str(e),
            type(draft_data).__name__,
        )
        return strings["format_error"]

    lines: list[str] = []
    computed_total = 0.0
    total_savings = 0.0

    for idx, item in enumerate(draft.items, start=1):
        name = getattr(item, "name", None) or "Unknown Item"
        qty = getattr(item, "qty", 0) or 0

        base_price = getattr(item, "base_price", None)
        price = getattr(item, "price", None)
        final_price = getattr(item, "final_price", None)

        # Prefer explicit base, then fallback to price
        if base_price is None:
            base_price = price
        if final_price is None:
            final_price = price if price is not None else base_price

        line_total = getattr(item, "line_total", None)
        discount_value = getattr(item, "discount_value", None)

        sku_lines, meta = format_sku_price_block(
            name,
            qty,
            base_price,
            final_price,
            line_total=line_total,
            discount_value=discount_value,
            index=idx,
        )
        lines.extend(_localize_draft_lines(sku_lines, labels))
        lines.append("")

        meta_total = meta.get("line_total")
        if meta_total is not None:
            try:
                computed_total += float(meta_total)
            except (TypeError, ValueError):
                pass
        else:
            try:
                if final_price is not None:
                    computed_total += float(final_price) * float(qty)
            except (TypeError, ValueError):
                pass

        savings_meta = meta.get("savings_total")
        if savings_meta is not None:
            try:
                total_savings += float(savings_meta)
            except (TypeError, ValueError):
                pass

    if discount_total_fallback is not None and discount_total_fallback >= 0:
        total_savings = float(discount_total_fallback)

    # Prefer draft.total_amount if present; else use computed_total
    draft_total = getattr(draft, "total_amount", None)
    try:
        total_buy_final = float(draft_total) if draft_total is not None else float(computed_total)
    except (TypeError, ValueError):
        total_buy_final = float(computed_total)

    if lines and lines[-1] == "":
        lines.pop()

    lines.append("")
    lines.append(f"*{labels['total_amount']}: {currency_label} {total_buy_final:.2f}*")
    try:
        lines.append(f"{labels['total_saving']}: {currency_label} {float(total_savings):,.2f}")
    except (TypeError, ValueError):
        lines.append(f"{labels['total_saving']}: {currency_label} 0.00")

    return "\n".join(lines)

def build_invoice_payload_from_draft(user_id: str, store_id: Optional[str] = None) -> dict:
    """
    Build the payload expected by the invoice-generation Lambda.
    The Lambda now consumes:
        - store_name_en
        - store_id
        - timestamp (ISO-8601, UTC)
        - currency_type
        - items[{description_en, item_number, qty, unit_price, value, discount, total}]
        - item_totals{total_qty, total_value, total_discount}

    Older fields (customer_name, customer_phone, vat_code, etc.) are no longer
    required by the Lambda, so we keep the payload lean to avoid validation issues
    on the server side.
    """
    resolved_store = _resolve_store_id_for_cart(user_id, store_id)
    draft_data = get_cart(user_id, store_id=resolved_store) or {}

    # Currency is now explicit in the payload; default to PKR if not set in env.
    currency_type = (
        _clean_env_value(os.getenv("INVOICE_CURRENCY"))
        or _clean_env_value(os.getenv("CURRENCY_TYPE"))
        or "PKR"
    )

    # Lambda expects a single timestamp field instead of separate date/time.
    # Use Pakistan Standard Time (UTC+05:00) to match billing timezone.
    _pkt_offset = datetime.timedelta(hours=5)
    timestamp = datetime.datetime.now(datetime.timezone(_pkt_offset)).replace(microsecond=0).isoformat()

    items_src = draft_data.get("items") or draft_data.get("skus") or []
    invoice_items: list[dict] = []

    for raw in items_src:
        if isinstance(raw, dict):
            itm = raw
        else:
            try:
                itm = raw.model_dump()
            except Exception:
                logger.warning("invoice.item.unrecognized_type", user_id=user_id, type=str(type(raw)))
                continue

        sku = itm.get("sku_code") or itm.get("sku") or itm.get("item_number") or itm.get("id") or ""
        name = itm.get("name") or itm.get("description") or itm.get("description_en") or ""
        qty = _coerce_qty(itm.get("qty") or itm.get("quantity"))

        # Skip empty/zero-qty lines to avoid invoice noise
        if qty <= 0:
            continue

        base_price = _safe_float(itm.get("base_price") or itm.get("price"))
        final_price = _safe_float(itm.get("final_price") or itm.get("unit_price") or itm.get("price"))
        line_total = _safe_float(itm.get("line_total"))

        # Use preprovided line-level discount_value exactly as-is for invoice rows.
        discount_value_field = _safe_float(itm.get("discount_value"))

        unit_price = final_price if final_price is not None else (base_price if base_price is not None else 0.0)
        if line_total is None and unit_price is not None:
            line_total = round(float(unit_price) * float(qty), 2)

        # Pre-discount value: base_price * qty if available else unit_price * qty
        if base_price is not None:
            pre_discount_value = round(float(base_price) * float(qty), 2)
        else:
            pre_discount_value = round(float(unit_price) * float(qty), 2)

        discount_line = float(discount_value_field) if discount_value_field is not None else 0.0

        total = line_total if line_total is not None else round(float(pre_discount_value) - float(discount_line or 0.0), 2)

        invoice_items.append({
            "description_en": str(name) or "Item",
            "item_number": str(sku) or "UNKNOWN",
            "qty": qty,
            "unit_price": float(unit_price or 0.0),
            "value": float(pre_discount_value or 0.0),
            "discount": float(discount_line or 0.0),
            "total": float(total),
        })

    # Aggregate totals, preferring priced totals from cart when present
    cart_totals = draft_data.get("totals") if isinstance(draft_data, dict) else {}
    total_qty = int(sum(_coerce_qty(i.get("qty")) for i in invoice_items))
    total_value = float(sum(_safe_float(i.get("value")) or 0.0 for i in invoice_items))
    total_discount = float(sum(_safe_float(i.get("discount")) or 0.0 for i in invoice_items))

    if isinstance(cart_totals, dict):
        subtotal_override = _safe_float(cart_totals.get("subtotal"))
        discount_override = _safe_float(cart_totals.get("discount_total") or cart_totals.get("total_discount"))
        if subtotal_override is not None:
            total_value = float(subtotal_override)
        if discount_override is not None:
            total_discount = float(discount_override)

    item_totals = {
        "total_qty": total_qty,
        "total_value": round(total_value, 2),
        "total_discount": round(total_discount, 2),
    }

    cust = _extract_customer_store_from_api(user_id)
    payload_store_id = cust.get("store_id") or resolved_store

    payload: dict = {
        "store_name_en": cust["store_name_en"],
        "store_id": payload_store_id,
        "timestamp": timestamp,
        "currency_type": currency_type,
        "items": invoice_items,
        "item_totals": item_totals,
    }

    # Emit the exact payload used for invoice generation for debugging (non-redacted).
    try:
        logger.info(
            "invoice.payload.built",
            user_id=user_id,
            item_count=len(invoice_items),
            payload=payload,
        )
    except Exception as e:
        logger.warning("invoice.payload.log_failed", user_id=user_id, error=str(e))

    # Legacy schema validation is best-effort only (schema may lag behind Lambda)
    try:
        InvoiceDataAWS.model_validate(payload)
    except Exception as e:
        logger.warning("invoice.payload.validation_warning", user_id=user_id, error=str(e))
    return payload


def generate_invoice_for_current_draft(user_id: str, store_id: Optional[str] = None) -> str:
    """
    Convenience wrapper: builds a valid payload from Firestore draft + API data,
    then calls generate_invoice_lambda(). Use this after placing the order,
    *before* clearing the draft.
    """
    try:
        payload = build_invoice_payload_from_draft(user_id, store_id=store_id)
    except Exception as e:
        logger.error("invoice.payload.validation_failed", user_id=user_id, error=str(e))
        return f"Invoice payload validation failed: {e}"

    try:
        return generate_invoice_lambda(payload, user_id)
    except Exception as e:
        logger.error("invoice.lambda.call_failed", user_id=user_id, error=str(e))
        return f"Invoice generation failed: {e}"


def generate_invoice_lambda(order_json: dict, user_id: str) -> str:
    """
    Generate an invoice via Lambda using the new payload contract:
    {
        store_name_en, store_id, timestamp (ISO-8601 UTC), currency_type,
        items[{description_en,item_number,qty,unit_price,value,discount,total}],
        item_totals{total_qty,total_value,total_discount}
    }
    """
    endpoint_url = "https://pzwwtmedtmlxndutbi47sc73py0xjpac.lambda-url.us-east-1.on.aws/"

    try:
        if hasattr(order_json, "model_dump"):
            payload = order_json.model_dump()
        else:
            payload = dict(order_json or {})

        # Ensure required top-level fields exist
        payload.setdefault("currency_type", _clean_env_value(os.getenv("INVOICE_CURRENCY")) or "PKR")
        _pkt_offset = datetime.timedelta(hours=5)
        payload.setdefault("timestamp", datetime.datetime.now(datetime.timezone(_pkt_offset)).replace(microsecond=0).isoformat())

        # Normalize items to expected numeric formats
        normalized_items = []
        for item in payload.get("items", []) or []:
            if not isinstance(item, dict):
                continue
            try:
                qty = _coerce_qty(item.get("qty"))
            except Exception:
                qty = 0
            normalized_items.append(
                {
                    "description_en": str(item.get("description_en") or "Item"),
                    "item_number": str(item.get("item_number") or "UNKNOWN"),
                    "qty": qty,
                    "unit_price": _safe_float(item.get("unit_price")) or 0.0,
                    "value": _safe_float(item.get("value")) or 0.0,
                    "discount": _safe_float(item.get("discount")) or 0.0,
                    "total": _safe_float(item.get("total")) or 0.0,
                }
            )
        payload["items"] = normalized_items

        # Normalize totals
        totals = payload.get("item_totals") or {}
        payload["item_totals"] = {
            "total_qty": _coerce_qty(totals.get("total_qty")),
            "total_value": _safe_float(totals.get("total_value")) or 0.0,
            "total_discount": _safe_float(totals.get("total_discount")) or 0.0,
        }

        # Log the exact payload we are about to send to the Lambda
        try:
            logger.info(
                "invoice.payload.outgoing",
                user_id=user_id,
                item_count=len(payload.get("items") or []),
                payload=payload,
            )
        except Exception as e:
            logger.warning("invoice.payload.outgoing_log_failed", user_id=user_id, error=str(e))

        headers = {"Content-Type": "application/json"}

        payload_size = len(json.dumps(payload))
        logger.info("invoice.lambda.request", size_bytes=payload_size)

        response = _post_with_retries(
            endpoint_url,
            json_payload=payload,
            headers=headers,
            connect_timeout=5.0,
            read_timeout=90.0,
            max_attempts=3,
            base_backoff=0.8,
        )

        response_data = response.json()
        url = response_data.get("url", None)

        if url:
            logger.info("invoice.url.received", user_id=user_id, url=url)

            sent, wa_msg_id = send_image(user_id, url)
            if sent:
                try:
                    conv_id = _sessions.get_active_conversation_id(user_id) or user_id
                    if wa_msg_id:
                        billing_event = generate_billing_event_v2(
                            tenant_id=TENANT_ID,
                            conversation_id=conv_id,
                            msg_type="image",
                            message_id=wa_msg_id,
                            role="assistant",
                            channel="whatsapp",
                            conversation_text="[invoice_image]",
                            gemini_usage=None,
                            eleven_tts_usage=None,
                            s3_key=url,
                        )
                        send_billing_event_fire_and_forget(billing_event)
                    else:
                        logger.warning("invoice.billing.no_msg_id", user_id=user_id)
                except Exception as e:
                    logger.warning("invoice.billing.emit_failed", user_id=user_id, error=str(e))

                try:
                    _sessions.request_end(user_id, delay_sec=15 * 60, reason="post_invoice", combine="max")
                except Exception as e:
                    logger.warning("session.schedule_end.error", user_id=user_id, error=str(e))

                delete_order_draft(user_id)
                logger.info("invoice.sent.success", user_id=user_id)
                return "Successfully sent invoice."
            else:
                logger.error("invoice.image.send.failed", user_id=user_id)
                return "Failed to send invoice image."
        else:
            logger.error("invoice.generation.no_url", user_id=user_id)
            return "Invoice generation failed - no URL returned"

    except Exception as e:
        logger.error("invoice.generation.error", user_id=user_id, error=str(e))
        print(f"Error generating invoice: {e}")
        return f"Invoice generation and sending failed, error: {e}"

@llm_safe("order.place")
def place_order_and_clear_draft(store_code: str, user_id: str) -> str:
    """
    LLM tool: finalize and place the user's order after explicit confirmation.
    Uses the current cart/draft for the store, submits to the order API,
    generates an invoice, clears the draft, and returns a WhatsApp-ready
    confirmation or error message. Call only after the user says yes/confirm.

    Arguments the model should send:
    - store_code (required str): Store code from customer lookup; must come
      from search_customer_by_phone.
    - user_id (required str): Chat/user identifier (normalized phone already in session).

    Example calls:
    - place_order_and_clear_draft(store_code="<store_code>", user_id="923001234567")
    - place_order_and_clear_draft("<store_code>", user_id=session.user_id)

    Returns:
    - str: Final confirmation text with order summary, or a readable error/diagnostic.
    """
    def _po_print(stage: str, **fields: Any) -> None:
        parts = [f"user_id={user_id}", f"store_code={store_code}"]
        for key, value in fields.items():
            if isinstance(value, (dict, list, tuple)):
                try:
                    rendered = json.dumps(value, ensure_ascii=False, default=str)
                except Exception:
                    rendered = str(value)
            else:
                rendered = str(value)
            parts.append(f"{key}={rendered}")
        print(f"[place_order][{stage}] {' '.join(parts)}")

    if debug_enabled():
        logger.info(
            "tool.call",
            tool="placeOrderTool",
            user_id=user_id,
            store_code=store_code,
        )
    logger.info("place_order.started", user_id=user_id, store_code=store_code)
    _po_print("started")
    strings = _get_place_order_strings()

    # 1. Get StoreCode (it's now a function argument, so we just check it)
    if not store_code:
        _po_print("failed.missing_store_code")
        return strings["missing_store_code"]

    # 2. Get Endpoint configuration from environment
    endpoint_url = os.getenv("SALESFLO_API_ENDPOINT", SALESFLO_API_ENDPOINT)
    if not endpoint_url:
        error_msg = strings["missing_endpoint"]
        logger.error("place_order.env.missing", error=error_msg)
        _po_print("failed.missing_endpoint", error=error_msg)
        return error_msg

    # 2b. Get CompanyCode from environment (must be present)
    company_code = os.getenv("COMPANY_CODE")
    if not company_code:
        error_msg = strings["missing_company_code"]
        logger.error("place_order.company_code.missing", error=error_msg)
        _po_print("failed.missing_company_code", error=error_msg)
        return error_msg

    try:
        # 3. Fetch Order Draft data (new cart path, fallback to legacy)
        draft_data = {}
        try:
            draft_data = get_cart(user_id, store_id=store_code) or {}
            _po_print(
                "cart.loaded.primary",
                has_items=bool(draft_data.get("items")),
                item_count=len(draft_data.get("items") or []),
                keys=list(draft_data.keys()),
            )
        except Exception as e:
            logger.warning("place_order.get_cart.failed", user_id=user_id, error=str(e))
            _po_print("failed.get_cart_exception", error=str(e), error_type=type(e).__name__)

        if not draft_data.get("items"):
            try:
                user_data = get_user_data(user_id)
                draft_data = (user_data.get("order_drafts") or {}) if user_data else {}
                _po_print(
                    "cart.loaded.legacy",
                    has_items=bool(draft_data.get("items")),
                    item_count=len(draft_data.get("items") or []),
                    keys=list(draft_data.keys()),
                )
            except Exception as e:
                logger.warning("place_order.legacy_cart.failed", user_id=user_id, error=str(e))
                _po_print("failed.legacy_cart_exception", error=str(e), error_type=type(e).__name__)

        if not draft_data or not draft_data.get("items"):
            _po_print(
                "failed.no_items_in_cart",
                has_draft=bool(draft_data),
                keys=list(draft_data.keys()) if isinstance(draft_data, dict) else type(draft_data).__name__,
            )
            return strings["no_items"]

        # Normalize legacy shapes that might have "skus" or basket-only data
        if not draft_data.get("items"):
            if draft_data.get("skus"):
                draft_data["items"] = draft_data["skus"]
            elif (draft_data.get("basket") or {}).get("items"):
                draft_data["items"] = draft_data["basket"]["items"]

        # Normalize minimal fields expected by OrderDraftItem
        items = draft_data.get("items") or []
        normalized_items = []
        for itm in items:
            if not isinstance(itm, dict):
                normalized_items.append(itm)
                continue

            sku_code_raw = (
                itm.get("sku_code")
                or itm.get("sku")
                or itm.get("skucode")
                or itm.get("sku_id")
                or itm.get("item_number")
                or itm.get("id")
            )
            sku_code = _normalize_sku_code(sku_code_raw)
            name = (
                itm.get("name")
                or itm.get("official_name")
                or itm.get("product_name")
                or itm.get("sku_name")
                or itm.get("description")
                or itm.get("description_en")
                or itm.get("title")
                or "Item"
            )

            if sku_code:
                itm.setdefault("sku_code", sku_code)
            if name:
                itm.setdefault("name", name)

            if not sku_code:
                logger.warning(
                    "place_order.invalid_sku_dropped",
                    user_id=user_id,
                    raw_sku=str(sku_code_raw) if sku_code_raw is not None else None,
                    name=name,
                )
                continue

            normalized_items.append(itm)

        draft_data["items"] = normalized_items
        draft_data.pop("skus", None)

        # Validate draft with Pydantic model (will raise if still missing)
        draft = OrderDraft.model_validate(_ensure_total_amount_field(draft_data))
        if not draft.items:
            _po_print("failed.empty_cart_after_validation")
            return strings["empty_cart"]
        _po_print(
            "draft.validated",
            item_count=len(draft.items),
            total_amount=getattr(draft, "total_amount", None),
        )

        # YTL Cement: do NOT call any external order API.
        # For now we also skip PDF generation and just store the order
        # snapshot in Firestore and return a clear confirmation message.
        if (TENANT_ID or "").strip().lower() == "ytl":
            order_id = f"YTL-{int(time.time())}"
            lines: List[str] = [
                f"Order ID: {order_id}",
                f"Customer (store_code): {store_code}",
                "",
                "Items:",
            ]
            for item in draft.items:
                try:
                    name = item.name or item.sku_code
                    qty = item.qty
                    sku = item.sku_code
                    line = f"- {name} ({sku}) — qty: {qty}"
                    lines.append(line)
                except Exception:
                    continue

            try:
                total_amount = getattr(draft, "total_amount", None) or getattr(draft, "grand_total", None)
            except Exception:
                total_amount = None
            if total_amount is not None:
                lines.append("")
                lines.append(f"Approximate demo total: {total_amount}")

            # Store order snapshot in Firestore under the user's document (users/{user_id}/orders/{order_id})
            try:
                order_doc = _user_ref(user_id).collection("orders").document(order_id)
                order_payload = {
                    "order_id": order_id,
                    "user_id": user_id,
                    "store_code": store_code,
                    "created_at": datetime.datetime.utcnow().isoformat() + "Z",
                    "items": [
                        {
                            "sku_code": item.sku_code,
                            "name": item.name,
                            "qty": item.qty,
                        }
                        for item in draft.items
                    ],
                    "total_amount": getattr(draft, "total_amount", None) or getattr(draft, "grand_total", None),
                    "status": "confirmed",
                    "delivery_status": "scheduled_today",
                    "tracking_message": "Your order is scheduled for delivery today.",
                }
                order_doc.set(order_payload, merge=True)
                _po_print("ytl_demo.order_stored", order_path=order_doc.path)
            except Exception as e:
                logger.warning("ytl_demo.order_store_failed", user_id=user_id, error=str(e))

            # Best-effort: clear the draft so subsequent orders start clean
            try:
                clear_ok = delete_order_draft(user_id, store_id=store_code)
                _po_print("ytl_demo.draft.clear", success=clear_ok)
            except Exception as e:
                logger.warning("ytl_demo.order_draft_clear_failed", user_id=user_id, error=str(e))

            # Clear pending order-confirmation context so the next message (e.g. "hi") does not re-ask to confirm
            try:
                _user_ref(user_id).set(
                    {"pending_order_confirmation": firestore.DELETE_FIELD},
                    merge=True,
                )
                logger.info("ytl_demo.order_confirm.context.cleared", user_id=user_id)
            except Exception as e:
                logger.warning("ytl_demo.order_confirm.context.clear_failed", user_id=user_id, error=str(e))

            confirmation_text = (
                "Your order has been confirmed successfully. ✅\n\n"
                f"**Order ID: {order_id}** — please save this for tracking.\n\n"
                "You’ll now receive your order details here in chat. "
                "You can ask *Where's my order?* anytime and share this Order ID to get an update."
            )

            logger.info("place_order.completed_ytl", user_id=user_id, order_id=order_id)
            _po_print("completed_ytl_demo", confirmation_text=confirmation_text)
            return confirmation_text

        # 4. Format the 'CreateOrder' payload (USING YOUR SCHEMA)
        # New API: use SKUQty (not SKUQtyUnits)
        items_list = [
            {"SKUCode": item.sku_code, "SKUQty": item.qty}
            for item in draft.items
        ]
        logger.info(items_list)

        # QuantityType (Box/Carton/Units) – from env with default "Box"
        quantity_type = os.getenv("ORDER_QUANTITY_TYPE", "Box")

        order_date = datetime.datetime.now().strftime("%Y-%m-%d")
        logger.info(f"Date: {order_date}")

        create_order_payload = {
            "type": "CreateOrder",
            "CompanyCode": company_code,
            "StoreCode": store_code,
            "DistributorCode": "D0005",
            "AppUserCode": "D0005OB9",
            "QuantityType": quantity_type,  # <-- new top-level field per updated API
            "body": {
                "OrderDate": order_date,
                "Items": items_list,
            },
        }

        # --- DEBUG LOGGING BEFORE REQUEST ---
        print(f"[place_order] endpoint_url={endpoint_url}")
        print(f"[place_order] company_code={company_code} store_code={store_code}")
        print(f"[place_order] payload_outgoing={json.dumps(create_order_payload, ensure_ascii=False)}")
        _po_print(
            "request.outgoing",
            endpoint_url=endpoint_url,
            quantity_type=quantity_type,
            order_date=order_date,
            item_count=len(items_list),
            has_auth_header=bool(REQUEST_HEADERS.get("Authorization")),
        )
        logger.debug(
            "[place_order] outgoing",
            endpoint=endpoint_url,
            company_code=company_code,
            store_code=store_code,
            payload=create_order_payload,
        )

        # 5. Call HTTP Endpoint with JSON body
        logger.info("place_order.endpoint.request", user_id=user_id, endpoint=endpoint_url)

        response = requests.post(
            endpoint_url,
            headers=REQUEST_HEADERS,
            json=create_order_payload,
            timeout=20.0,
        )
        _po_print("response.received", status_code=response.status_code)

        # --- DEBUG DUMP RESPONSE ---
        _debug_dump_response(response)

        # Raise an exception for bad status codes (4xx, 5xx)
        response.raise_for_status()

        # 6. Handle API response
        payload, raw_text = _safe_json(response)
        print(
            f"[place_order] _safe_json payload_type="
            f"{type(payload).__name__ if payload is not None else None} "
            f"raw_len={len(raw_text) if raw_text else 0}"
        )
        logger.debug(
            "[place_order] _safe_json",
            payload_type=type(payload).__name__ if payload is not None else None,
            raw_len=len(raw_text) if raw_text else 0,
        )

        # Try to unwrap common Lambda/APIGW envelopes or JSON-as-string bodies
        payload = _unwrap_api_envelope(payload, response)
        print(
            f"[place_order] after unwrap payload_type="
            f"{type(payload).__name__ if payload is not None else None} "
            f"keys={(list(payload.keys()) if isinstance(payload, dict) else 'n/a')}"
        )
        logger.debug(
            "[place_order] after unwrap",
            payload_type=type(payload).__name__ if payload is not None else None,
            keys=list(payload.keys()) if isinstance(payload, dict) else [],
        )

        msg_probe = _best_message(payload or {}) if isinstance(payload, dict) else None
        succ_probe = _is_success_payload(payload) if isinstance(payload, dict) else False
        print(f"[place_order] msg_probe={msg_probe} succ_probe={succ_probe}")
        logger.debug("[place_order] probes", msg=msg_probe, success=succ_probe)

        if payload is None:
            # Non-JSON or empty response from API; surface useful diagnostics
            diag = _diagnostic_string(response, raw_text)
            logger.error("place_order.api.non_json_or_empty", user_id=user_id, diag=diag)
            print(f"[place_order] DIAG => {diag}")
            _po_print("failed.non_json_or_empty_response", diag=diag)
            return strings["unexpected_response"].format(diag=diag)

        # JSON parsed OK — check success forms
        if payload and _is_success_payload(payload):
            logger.info(
                "place_order.success",
                user_id=user_id,
                message=_best_message(payload),
            )
            _po_print("success.api_response", message=_best_message(payload), payload_keys=list(payload.keys()))
            # ✅ Generate invoice BEFORE clearing, but invoice must NOT clear cart internally
            invoice_result = generate_invoice_for_current_draft(user_id, store_id=store_code)
            _po_print("invoice.result", result=invoice_result)

            # Format concise confirmation message (no item list)
            def _format_money(val: Any) -> str:
                try:
                    return f"{float(val):,.0f}"
                except Exception:
                    return "N/A"

            order_total = _format_money(getattr(draft, "total_amount", None) or getattr(draft, "grand_total", None))
            delivery_raw = _extract_delivery_date(payload or {})
            delivery_date = _format_delivery_date_human(delivery_raw) or "Jald hi confirm karenge"

            confirmation_text = (
                "Aap ka order confirm ho gaya hai! ✅\n\n"
                "📦 Aap ki Order Details:\n\n"
                f"Total Bill: {order_total} PKR\n"
                f"Delivery Date: {delivery_date}\n\n"
                "Delivery wale din kisi bhi mazeed maloomat ke liye aap humein is number par message kar sakte hain.\n\n"
                "EBM chunne ka shukriya!"
            )

            # ✅ Clear the exact same cart (store_code)
            clear_ok = delete_order_draft(user_id, store_id=store_code)
            _po_print("draft.clear", success=clear_ok)

            # --- CLEAR BUTTON STATE ---
            try:
                for button_id in ["ORDER_CONFIRM_YES", "ORDER_CONFIRM_NO"]:
                    doc_ref = _user_ref(user_id).collection("button_clicks").document(button_id)
                    doc_ref.delete()
                logger.info("button_clicks.cleared", user_id=user_id)
                _po_print("buttons.clear", success=True)
            except Exception as e:
                logger.warning(
                "button_clicks.clear_failed",
                    user_id=user_id,
                    error=str(e),
                )
                _po_print("failed.buttons_clear", error=str(e), error_type=type(e).__name__)
            # --- END CLEAR BUTTON STATE ---

            # Clear pending order-confirmation context after successful placement.
            try:
                _user_ref(user_id).set(
                    {"pending_order_confirmation": firestore.DELETE_FIELD},
                    merge=True,
                )
                logger.info("order_confirm.context.cleared", user_id=user_id)
                _po_print("order_confirm_context.clear", success=True)
            except Exception as e:
                logger.warning("order_confirm.context.clear_failed", user_id=user_id, error=str(e))
                _po_print("failed.order_confirm_context_clear", error=str(e), error_type=type(e).__name__)

            # Schedule session end
            try:
                _sessions.request_end(user_id, delay_sec=15 * 60, reason="post_order")
                _po_print("session_end.scheduled", delay_sec=15 * 60)
            except Exception as e:
                logger.warning("session.schedule_end.error", user_id=user_id, error=str(e))
                _po_print("failed.session_end_schedule", error=str(e), error_type=type(e).__name__)

            _po_print("success.completed")
            return confirmation_text

        # Not explicitly marked success — still return details to diagnose
        msg = _best_message(payload)
        try:
            payload_preview = json.dumps(payload)[:1500]
        except Exception:
            payload_preview = str(payload)[:1500]
        diag = (
            f"HTTP {response.status_code}; Content-Type: {response.headers.get('Content-Type')}; "
            f"Payload: {payload_preview}"
        )
        logger.error("place_order.api.fail", user_id=user_id, diag=diag)
        print(f"[place_order] DIAG => {diag}")
        _po_print("failed.api_negative_response", message=msg, diag=diag)
        reason = f"{strings['reason_label']}: {msg}. " if msg else ""
        return strings["failed"].format(reason=reason, diag=diag)

    except requests.exceptions.RequestException as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        body = getattr(getattr(e, "response", None), "text", None)
        diag = f"HTTP {status}; Body: {_truncate(body)}"
        logger.error("place_order.request.error", user_id=user_id, error=str(e), diag=diag)
        print(f"[place_order] DIAG => {diag}")
        _po_print(
            "failed.request_exception",
            error=str(e),
            error_type=type(e).__name__,
            status_code=status,
            diag=diag,
        )
        return strings["request_error"].format(diag=diag)
    except Exception as e:
        logger.error("place_order.general.error", user_id=user_id, error=str(e))
        _po_print(
            "failed.unhandled_exception",
            error=str(e),
            error_type=type(e).__name__,
            traceback=traceback.format_exc(),
        )
        return strings["internal_error"].format(error=e)


def get_order_status(order_id: str, user_id: str) -> str:
    """
    LLM tool: look up an order by Order ID for the current user and return a
    short tracking/delivery status. Use when the user asks "where's my order",
    "track my order", or "order status" and has provided (or you have) their Order ID.

    Arguments:
    - order_id (required str): The order ID, e.g. YTL-1734567890 (from the confirmation message).
    - user_id (required str): Chat/user identifier (the current user).

    Returns:
    - str: A short status message, e.g. "Your order is scheduled for delivery today.";
      or "I couldn't find an order with that ID. Please check and try again."
    """
    logger.info("tool.call", tool="getOrderStatusTool", user_id=user_id, order_id=order_id)
    raw_order_id = (order_id or "").strip()
    if not raw_order_id:
        return "Please share your Order ID (e.g. YTL-1234567890) so I can check the status. You'll find it in the message we sent when your order was confirmed."
    try:
        # Normalize Order ID for lookup so that 'ytl-123', 'YTL-123' or 'Ytl-123'
        # all resolve to the same document key. Echo the user's original casing
        # back in messages for readability.
        order_id_key = raw_order_id.upper()
        order_ref = _user_ref(user_id).collection("orders").document(order_id_key)
        doc = order_ref.get()
        if not doc.exists:
            return (
                f"I couldn't find an order with ID *{raw_order_id}*. "
                "Please check the number and try again, or use the Order ID from your confirmation message."
            )
        data = doc.to_dict() or {}
        tracking_message = (data.get("tracking_message") or "").strip()
        delivery_status = (data.get("delivery_status") or "").strip()
        if tracking_message:
            return tracking_message
        status_map = {
            "scheduled_today": "Your order is scheduled for delivery today.",
            "in_transit": "Your order is on the way.",
            "delivered": "Your order has been delivered.",
            "confirmed": "Your order is confirmed and scheduled for delivery soon.",
        }
        return status_map.get(delivery_status) or status_map.get(
            (data.get("status") or "").strip().lower()
        ) or "Your order is confirmed. We'll update you on delivery timing shortly."
    except Exception as e:
        logger.warning("get_order_status.error", user_id=user_id, order_id=order_id, error=str(e))
        return "I had trouble looking up that order. Please try again or share your Order ID from the confirmation message."


# --- NEW FUNCTION ---
def get_last_orders(store_code: str, user_id: str) -> str:
    """
    LLM tool: fetch the customer's most recent orders for a store and return a
    WhatsApp-ready text summary. Use when the user asks for order history or to
    check their last order. Requires the store_code; 
    limit is 2 most recent orders.

    Arguments the model should send:
    - store_code (required str): Store code / storecode from customer lookup.
    - user_id (required str): Chat/user identifier (usually the normalized phone
      number already available in session).

    Example calls:
    - get_last_orders(store_code="<store_code>", user_id="923001234567")
    - get_last_orders("<store_code>", user_id=session.user_id)

    Returns:
    - str: Multi-line summary of recent orders including order number, date,
      delivery date, total, and item lines; or a readable error/diagnostic.
    """
    if debug_enabled():
        logger.info(
            "tool.call",
            tool="getLastOrdersTool",
            user_id=user_id,
            store_code=store_code,
        )
    logger.info("get_last_orders.started", user_id=user_id, store_code=store_code)

    if not store_code:
        return "Error: Store code was not provided. Cannot fetch orders."

    endpoint_url = os.getenv("SALESFLO_API_ENDPOINT", SALESFLO_API_ENDPOINT)
    if not endpoint_url:
        error_msg = "Internal configuration error: SALESFLO_API_ENDPOINT is not set."
        logger.error("get_last_orders.env.missing", error=error_msg)
        return error_msg

    company_code = os.getenv("COMPANY_CODE")
    if not company_code:
        error_msg = "Internal configuration error: COMPANY_CODE is not set."
        logger.error("get_last_orders.company_code.missing", error=error_msg)
        return error_msg

    try:
        # Format the 'GetLastOrders' payload
        get_orders_payload = {
            "type": "GetLastOrders",
            "CompanyCode": company_code,
            "StoreCode": store_code,
            "Limit": 2,  # optional; API supports it
            "DistributorCode": "D0005",
            "AppUserCode": "D0005OB9"
        }

        print(f"[get_last_orders] endpoint_url={endpoint_url}")
        print(f"[get_last_orders] company_code={company_code} store_code={store_code}")
        print(f"[get_last_orders] payload_outgoing={json.dumps(get_orders_payload, ensure_ascii=False)}")
        logger.debug(
            "[get_last_orders] outgoing",
            endpoint=endpoint_url,
            company_code=company_code,
            store_code=store_code,
            payload=get_orders_payload,
        )

        logger.info("get_last_orders.endpoint.request", user_id=user_id, endpoint=endpoint_url)

        response = requests.post(
            endpoint_url,
            headers=REQUEST_HEADERS,
            json=get_orders_payload,
            timeout=20.0
        )
        _debug_dump_response(response)
        response.raise_for_status()

        payload, raw_text = _safe_json(response)
        print(
            f"[get_last_orders] _safe_json payload_type="
            f"{type(payload).__name__ if payload is not None else None} "
            f"raw_len={len(raw_text) if raw_text else 0}"
        )
        logger.debug(
            "[get_last_orders] _safe_json",
            payload_type=type(payload).__name__ if payload is not None else None,
            raw_len=len(raw_text) if raw_text else 0,
        )

        # Try to unwrap common Lambda/APIGW envelopes or JSON-as-string bodies
        payload = _unwrap_api_envelope(payload, response)
        print(
            f"[get_last_orders] after unwrap payload_type="
            f"{type(payload).__name__ if payload is not None else None} "
            f"keys={(list(payload.keys()) if isinstance(payload, dict) else 'n/a')}"
        )
        logger.debug(
            "[get_last_orders] after unwrap",
            payload_type=type(payload).__name__ if payload is not None else None,
            keys=list(payload.keys()) if isinstance(payload, dict) else [],
        )

        msg_probe = _best_message(payload or {}) if isinstance(payload, dict) else None
        succ_probe = _is_success_payload(payload) if isinstance(payload, dict) else False
        has_list = isinstance(payload, dict) and isinstance(payload.get("LastOrders"), list)
        print(f"[get_last_orders] msg_probe={msg_probe} succ_probe={succ_probe} has_list={has_list}")
        logger.debug(
            "[get_last_orders] probes",
            msg=msg_probe,
            success=succ_probe,
            has_list=has_list,
        )

        if payload is None:
            diag = _diagnostic_string(response, raw_text)
            logger.error("get_last_orders.api.non_json_or_empty", user_id=user_id, diag=diag)
            print(f"[get_last_orders] DIAG => {diag}")
            return f"Order system returned an unexpected response. {diag}"

        # Success forms or presence of LastOrders data
        if (payload and _is_success_payload(payload)) or has_list:
            logger.info("get_last_orders.success", user_id=user_id)
            orders = payload.get("LastOrders") or []
            if not orders:
                return "You have no recent orders."

            reply_lines = ["Here are your last orders:"]
            for order in orders:
                order_num = order.get("OrderNumber") or order.get("OrderNo") or "N/A"
                order_date = order.get("OrderDate", "N/A")
                delivery_date = order.get("DeliveryDate", "N/A")

                # Handle total from multiple possible keys and format nicely
                raw_total = (
                    order.get("OrderTotal")
                    or order.get("TotalAmount")
                    or order.get("Total")
                    or order.get("Amount")
                )
                try:
                    order_total_fmt = (
                        f"{float(raw_total):,.2f}" if raw_total is not None else "N/A"
                    )
                except (TypeError, ValueError):
                    order_total_fmt = str(raw_total) if raw_total is not None else "N/A"

                reply_lines.append(
                    f"\nOrder {order_num} — Date: {order_date} — "
                    f"Delivery: {delivery_date} — Total: {order_total_fmt}"
                )
                reply_lines.append("Items:")

                items = order.get("Items") or []
                if items and isinstance(items, list):
                    for it in items:
                        desc = (
                            it.get("SKUDescription")
                            or it.get("Description")
                            or it.get("sku_desc")
                            or "Item"
                        )

                        # Prefer Box; if Box is zero or missing, fall back to Cartons,
                        # then to legacy quantity fields.
                        box_val = it.get("Box")
                        cartons_val = it.get("Cartons") or it.get("Carton")

                        def _is_nonzero(v):
                            try:
                                return float(v) != 0
                            except (TypeError, ValueError):
                                return bool(v)

                        unit_label = ""
                        if _is_nonzero(box_val):
                            qty = box_val
                            unit_label = "Box"
                        elif _is_nonzero(cartons_val):
                            qty = cartons_val
                            unit_label = "Carton"
                        else:
                            qty = (
                                it.get("SKUQty")
                                or it.get("SKUQtyUnits")
                                or it.get("Qty")
                                or it.get("qty")
                                or "N/A"
                            )

                        if unit_label:
                            reply_lines.append(
                                f"  • {desc} — Qty ({unit_label}): {qty}"
                            )
                        else:
                            reply_lines.append(
                                f"  • {desc} — Qty: {qty}"
                            )
                else:
                    reply_lines.append("  • No line items returned for this order.")

            return "\n".join(reply_lines)

        # Not explicitly marked success — show diagnostics
        msg = _best_message(payload)
        try:
            payload_preview = json.dumps(payload)[:1500]
        except Exception:
            payload_preview = str(payload)[:1500]
        diag = (
            f"HTTP {response.status_code}; Content-Type: "
            f"{response.headers.get('Content-Type')}; Payload: {payload_preview}"
        )
        logger.error("get_last_orders.api.fail", user_id=user_id, diag=diag)
        print(f"[get_last_orders] DIAG => {diag}")
        return (
            f"Failed to get your orders. "
            f"{('Reason: ' + msg + '. ') if msg else ''}{diag}"
        )

    except requests.exceptions.RequestException as e:
        status = getattr(getattr(e, "response", None), "status_code", None)
        body = getattr(getattr(e, "response", None), "text", None)
        diag = f"HTTP {status}; Body: {_truncate(body)}"
        logger.error("get_last_orders.request.error", user_id=user_id, error=str(e), diag=diag)
        print(f"[get_last_orders] DIAG => {diag}")
        return f"An error occurred while communicating with the order system. {diag}"
    except Exception as e:
        logger.error("get_last_orders.general.error", user_id=user_id, error=str(e))
        return f"An unexpected internal error occurred: {e}"
    
# ==============================================================================
# AGENT TOOL DEFINITIONS (CORRECTED)
# ==============================================================================

# --- Input schema for the place_order_tool ---
class PlaceOrderInput(BaseModel):
    """Input schema for the place_order_tool."""
    store_code: str = Field(..., description="The storecode of the customer, obtained from the 'search_customer_by_phone' tool.")

# --- Tool definition for placing an order ---
placeOrderTool = FunctionTool(
    func=place_order_and_clear_draft
)

getLastOrdersTool = FunctionTool(
    func=get_last_orders,
)

getOrderStatusTool = FunctionTool(
    func=get_order_status,
)

# --- Input schema for the get_last_orders tool ---
class GetLastOrdersInput(BaseModel):
    """Input schema for the get_last_orders tool."""
    store_code: str = Field(..., description="The storecode of the customer, obtained from the 'search_customer_by_phone' tool.")

placeOrderTool = FunctionTool(
    func=place_order_and_clear_draft
)

getLastOrdersTool = FunctionTool(
    func=get_last_orders,
)

class ConfirmOrderDraftInput(BaseModel):
    """Input schema for the confirm_order_draft tool."""
    pass
    # No extra fields; the runtime will inject user_id when calling the tool.
    # This exists mainly so the tool shows nicely in the schema.
# Update in order_draft_tools.py

def send_order_confirmation_flow(user_id: str) -> str:
    """
    Send dynamic WhatsApp Flow with editable product list
    
    The Flow will:
    1. Load products from Firestore order draft
    2. Show interactive quantity steppers
    3. Sync changes back to Firestore in real-time
    4. Show final confirmation screen
    """
    from dotenv import load_dotenv
    load_dotenv()

    WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
    WHATSAPP_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")
    FLOW_ID = os.getenv("WA_ORDER_CONFIRM_FLOW_ID")

    if not (WHATSAPP_PHONE_NUMBER_ID and WHATSAPP_ACCESS_TOKEN and FLOW_ID):
        logger.error("confirm_flow.missing_creds", user_id=user_id)
        return (
            "Order confirmation Flow settings are missing. Please confirm your order in chat instead."
        )

    # Verify draft exists (Flow will load actual data via data exchange endpoint)
    try:
        draft_data = _ensure_total_amount_field(get_cart(user_id) or {})
    except Exception as e:
        logger.error("confirm_flow.draft_read_error", user_id=user_id, error=str(e))
        return "Cart read karne mein issue aa gaya hai bhai."

    if not draft_data:
        return "Cart khali hai bhai. Pehle products add karein."

    try:
        draft = OrderDraft.model_validate(draft_data)
    except Exception as e:
        logger.error("confirm_flow.draft_validation_error", user_id=user_id, error=str(e))
        return "Cart data invalid hai. Please try again."

    if not draft.items:
        return "Cart mein items nahi hain. Pehle add karein."

    url = f"https://graph.facebook.com/v23.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_ACCESS_TOKEN}",
        "Content-Type": "application/json",
    }

    payload = {
        "messaging_product": "whatsapp",
        "to": user_id,
        "type": "interactive",
        "interactive": {
            "type": "flow",
            "header": {
                "type": "text",
                "text": "🛒 Your Order Cart"
            },
            "body": {
                "text": "Review and edit your concrete order below. You can change quantities or remove items."
            },
            "footer": {
                "text": "Powered by YTL Cement"
            },
            "action": {
                "name": "flow",
                "parameters": {
                    "flow_id": FLOW_ID,
                    "flow_message_version": "3",
                    "mode": "published",  # Use "draft" for testing
                    "flow_cta": "Review Order",
                    "flow_token": user_id,  # Pass user_id as token for data exchange
                    
                    # IMPORTANT: Don't pass initial data here
                    # Flow will call your data exchange endpoint to get products
                    # This keeps the flow and backend in perfect sync
                }
            }
        }
    }

    logger.info("confirm_flow.sending", user_id=user_id, flow_id=FLOW_ID)

    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=15)
        logger.info(
            "confirm_flow.response",
            user_id=user_id,
            status=resp.status_code,
            body=resp.text[:500],
        )
        resp.raise_for_status()
        logger.info("confirm_flow.sent", user_id=user_id)
        return ""  # Success - don't echo anything
    except Exception as e:
        logger.error("confirm_flow.send_error", user_id=user_id, error=str(e))
        return "Flow bhejne mein error aa gaya hai. Text mein 'haan' likh kar confirm karein."

from google.adk.tools.function_tool import FunctionTool

@llm_safe("order_draft.update")
def send_order_confirmation_flow_tool(user_id: str) -> str:
    return send_order_confirmation_flow(user_id)

sendOrderConfirmationFlowTool = FunctionTool(
    func=send_order_confirmation_flow_tool,
)

_CONFIRM_ORDER_DRAFT_MESSAGES = {
    "UR": {
        "read_error": (
            "Mujhe aapka current cart read karne mein issue aa gaya hai. "
            "Thori der baad phir try karein ya items dobara bhej dein."
        ),
        "no_draft": (
            "Bhai, abhi aapke cart mein koi items nahi hain. "
            "Pehlay products add kar lein, phir mein order place karne mein help karungi."
        ),
        "validation_error": "Mujhe aik dafa wapis batain",
        "empty_items": (
            "Bhai, abhi aapke cart mein koi items nahi hain. "
            "Pehlay kuch products add karein, phir mein order place karungi."
        ),
        "summary_template": (
            "Theek hai bhai, aapke cart mein yeh items hain:\n\n"
            "{summary}\n\n"
            "Confirm karte hi aapko *Hari Raya promotion* se *10% off* milega.\n\n"
            "Kya yeh final order hai? Agar aap confirm karein (haan / yes / confirm), "
            "toh main yehi order place kar dungi."
        ),
    },
    "EN": {
        "read_error": (
            "I had trouble reading your current cart. "
            "Please try again shortly or send the items again."
        ),
        "no_draft": (
            "There are no items in your cart right now. "
            "Please add products first, then I'll help place the order."
        ),
        "validation_error": "Please tell me again.",
        "empty_items": (
            "There are no items in your cart right now. "
            "Please add some products, then I'll place the order."
        ),
        "summary_template": (
            "Alright, here are the items in your cart:\n\n"
            "{summary}\n\n"
            "Confirm now and get *10% off* with our *Hari Raya promotion*.\n\n"
            "Is this the final order? If you confirm (yes / confirm), "
            "I'll place this order."
        ),
    },
    "CN_MY": {
        "read_error": "我读取您当前的购物车时遇到了问题。请稍后再试，或重新发送商品。",
        "no_draft": "目前您的购物车里没有商品。请先添加商品，然后我再帮您下单。",
        "validation_error": "请您再说一遍。",
        "empty_items": "目前您的购物车里没有商品。请先添加一些商品，然后我再为您下单。",
        "summary_template": (
            "好的，您购物车里有以下商品：\n\n"
            "{summary}\n\n"
            "确认即享*开斋节促销* *九折优惠*。\n\n"
            "这是最终订单吗？如果您确认（是 / yes / confirm），我就为您下单。"
        ),
    },
    "BM": {
        "read_error": (
            "Saya menghadapi masalah membaca troli anda sekarang. "
            "Sila cuba lagi sebentar lagi atau hantar semula item."
        ),
        "no_draft": (
            "Sekarang tiada item dalam troli anda. "
            "Sila tambah produk dahulu, kemudian saya akan bantu buat pesanan."
        ),
        "validation_error": "Sila beritahu saya sekali lagi.",
        "empty_items": (
            "Sekarang tiada item dalam troli anda. "
            "Sila tambah beberapa produk dahulu, kemudian saya akan buat pesanan."
        ),
        "summary_template": (
            "Baik, berikut adalah item dalam troli anda:\n\n"
            "{summary}\n\n"
            "Sahkan sekarang dan dapat *10% diskaun* dengan promosi *Hari Raya* kami.\n\n"
            "Adakah ini pesanan akhir? Jika anda sahkan (ya / yes / confirm), "
            "saya akan buat pesanan ini."
        ),
    },
    "AR": {
        "read_error": (
            "واجهت مشكلة أثناء قراءة سلة مشترياتك الحالية. "
            "يرجى المحاولة لاحقًا أو إرسال العناصر مرة أخرى."
        ),
        "no_draft": (
            "لا توجد عناصر في سلة مشترياتك الآن. "
            "يرجى إضافة المنتجات أولًا، ثم سأساعدك في إتمام الطلب."
        ),
        "validation_error": "يرجى إخباري مرة أخرى.",
        "empty_items": (
            "لا توجد عناصر في سلة مشترياتك الآن. "
            "يرجى إضافة بعض المنتجات أولًا، ثم سأقوم بإتمام الطلب."
        ),
        "summary_template": (
            "حسنًا، هذه هي العناصر في سلة مشترياتك:\n\n"
            "{summary}\n\n"
            "أكد الآن واحصل على *خصم 10٪* مع عرض *عيد الفطر*.\n\n"
            "هل هذا هو الطلب النهائي؟ إذا أكدت (نعم / yes / confirm)، "
            "سأقوم بإتمام هذا الطلب."
        ),
    },
}

def _normalize_prompt_language(raw: str) -> str:
    s = (raw or "").strip().upper()
    if s in ("ENGLISH", "EN"):
        return "EN"
    if s in ("URDU", "UR", "ROMAN URDU", "ROMAN_URDU", "ROMAN-URDU"):
        return "UR"
    if s in ("CN_MY", "ZH_MY", "MALAYSIAN CHINESE"):
        return "CN_MY"
    if s in ("BM", "BM_MY", "MALAY", "BAHASA"):
        return "BM"
    if s in ("ARABIC", "AR", "عربي", "العربية"):
        return "AR"
    return "UR"

def _confirm_order_draft_with_messages(user_id: str, messages: dict) -> str:
    """
    Common confirm_order_draft logic with language-specific messages.
    """
    try:
        draft_data = _ensure_total_amount_field(get_cart(user_id) or {})
    except Exception as e:
        logger.error("confirm_draft.read.error", user_id=user_id, error=str(e))
        return messages["read_error"]

    # No draft stored
    if not draft_data:
        return messages["no_draft"]

    # Validate with schema
    try:
        draft = OrderDraft.model_validate(draft_data)
    except Exception as e:
        logger.error("confirm_draft.validation.error", user_id=user_id, error=str(e))
        return messages["validation_error"]

    # Empty item list
    if not draft.items:
        return messages["empty_items"]

    # Non-empty cart → plain text summary
    summary_text = _render_order_draft_template_summary(draft.model_dump())
    return messages["summary_template"].format(summary=summary_text)


def confirm_order_draft_ur(user_id: str) -> str:
    return _confirm_order_draft_with_messages(user_id, _CONFIRM_ORDER_DRAFT_MESSAGES["UR"])


def confirm_order_draft_en(user_id: str) -> str:
    return _confirm_order_draft_with_messages(user_id, _CONFIRM_ORDER_DRAFT_MESSAGES["EN"])


def confirm_order_draft_cn_my(user_id: str) -> str:
    return _confirm_order_draft_with_messages(user_id, _CONFIRM_ORDER_DRAFT_MESSAGES["CN_MY"])


def confirm_order_draft_bm(user_id: str) -> str:
    return _confirm_order_draft_with_messages(user_id, _CONFIRM_ORDER_DRAFT_MESSAGES["BM"])

def confirm_order_draft_ar(user_id: str) -> str:
    return _confirm_order_draft_with_messages(user_id, _CONFIRM_ORDER_DRAFT_MESSAGES["AR"])


def confirm_order_draft(user_id: str) -> str:
    """
    LLM tool: generate a WhatsApp-friendly order confirmation message from the
    user's current cart/draft. Always Call when the user is ready to place the order before calling order placing tool so
    you can show the item summary and ask for explicit confirmation.

    Behavior:
    - Uses PROMPT_LANGUAGE env to pick language (UR default fallback).
    - Plain text only: no multi-product message, no buttons/interactive UI.
    - Returns friendly errors when cart/draft is missing or empty.
    - Agent must send the returned text to the user and wait for an explicit
      "yes/confirm" before calling placeOrderTool.

    Arguments:
    - user_id (required str): Identifier for the user whose draft/cart to read.

    Example calls:
    - confirm_order_draft(user_id=session.user_id)

    Returns:
    - str: Message ready to send to WhatsApp (summary + confirmation prompt).
    """
    logger.info("tool.call", tool="confirmOrderDraftTool", user_id=user_id)
    lang = _normalize_prompt_language(os.getenv("PROMPT_LANGUAGE"))
    if lang == "EN":
        return confirm_order_draft_en(user_id)
    if lang == "CN_MY":
        return confirm_order_draft_cn_my(user_id)
    if lang == "BM":
        return confirm_order_draft_bm(user_id)
    if lang == "AR":
        return confirm_order_draft_ar(user_id)
    return confirm_order_draft_ur(user_id)

confirmOrderDraftTool = FunctionTool(
    func=confirm_order_draft
    )
