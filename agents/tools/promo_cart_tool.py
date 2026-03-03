import json
from typing import List, Optional, Dict, Any

from utils.logging import logger, debug_enabled
from agents.tools.api_tools import search_products_by_sku, semantic_product_search, unwrap_tool_response
from agents.tools.order_draft_tools import get_cart
from agents.tools.templates import order_draft_template
from agents.tools.cart_tools import agentflo_cart_tool


def _safe_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _looks_like_sku_code(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    token = value.strip()
    if not token:
        return False
    upper = token.upper()
    return upper.startswith("SKU") and any(ch.isdigit() for ch in upper) and " " not in upper


def _display_name(name: Any, fallback: str = "item") -> str:
    if isinstance(name, str):
        cleaned = name.strip()
        if cleaned and not _looks_like_sku_code(cleaned):
            return cleaned
    return fallback


def _extract_products(payload: Any) -> List[Dict[str, Any]]:
    if isinstance(payload, list):
        return [p for p in payload if isinstance(p, dict)]

    if isinstance(payload, dict):
        for key in ("data", "products", "items", "result"):
            val = payload.get(key)
            if isinstance(val, list):
                return [p for p in val if isinstance(p, dict)]

    return []


def _parse_product_search(raw: Any) -> List[Dict[str, Any]]:
    ok, payload, err = unwrap_tool_response(raw, system_name="search_products_by_sku")
    if not ok:
        logger.warning("promo_cart.search_parse_failed", error=err)
        return []

    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            logger.warning("promo_cart.search_parse_failed", preview=str(payload)[:200])
            return []
    return _extract_products(payload)


def _pick_sku(product: Dict[str, Any]) -> Optional[str]:
    for key in ("sku_code", "skucode", "SKUCode", "sku", "SKU"):
        val = product.get(key)
        if val:
            return str(val)
    return None


def _pick_name(product: Dict[str, Any]) -> str:
    return (
        product.get("official_name")
        or product.get("name")
        or product.get("sku_name")
        or product.get("SKUDescription")
        or product.get("description")
        or product.get("title")
        or "Item"
    )


def _pick_price(product: Dict[str, Any]) -> Dict[str, Optional[float]]:
    pricing = product.get("pricing") or {}
    base = (
        _safe_float(pricing.get("total_buy_price_virtual_pack"))
        or _safe_float(pricing.get("total_buy_price"))
        or _safe_float(pricing.get("mrp"))
        or _safe_float(product.get("price"))
    )
    final = (
        _safe_float(pricing.get("final_price"))
        or _safe_float(pricing.get("discounted_price"))
        or _safe_float(pricing.get("total_sell_price_virtual_pack"))
        or base
    )

    # IMPORTANT CHANGE: main price is base (buy) not final (sell)
    return {
        "price": base if base is not None else 0.0,
        "base_price": base,
        "final_price": final,
    }

def _semantic_best_match(query: str) -> Optional[Dict[str, Any]]:
    try:
        raw = semantic_product_search(query)
        ok, payload, err = unwrap_tool_response(raw, system_name="semantic_product_search")
        if not ok:
            logger.warning("promo_cart.semantic_failed", query=query, error=err)
            return None

        # Handle ADK-style tool response: may return a JSON string in "result"
        # or nested under "response": {"result": "..."}.
        def _coerce_payload(obj: Any) -> Any:
            if isinstance(obj, str):
                try:
                    return json.loads(obj)
                except json.JSONDecodeError:
                    return obj
            if hasattr(obj, "model_dump"):
                return obj.model_dump()
            if hasattr(obj, "dict"):
                return obj.dict()
            return obj

        payload = _coerce_payload(payload)
        if isinstance(payload, dict) and "response" in payload and isinstance(payload["response"], dict):
            payload = _coerce_payload(payload["response"])
        if isinstance(payload, dict) and "result" in payload and isinstance(payload["result"], str):
            payload = _coerce_payload(payload["result"])

        # Final normalized payload should have "data": [...]
        data = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(data, list):
            logger.warning("promo_cart.semantic_no_data", query=query, payload_preview=str(payload)[:200])
            return None

        candidates = [c for c in data if isinstance(c, dict)]
        if not candidates:
            return None

        def _score(candidate: Dict[str, Any]) -> float:
            # Prefer semantic similarity first; fall back to profit if similarity missing.
            similarity = _safe_float(candidate.get("similarity"))
            if similarity is not None:
                return similarity

            product = candidate.get("product") or candidate
            pricing = product.get("pricing") or {}
            profit = pricing.get("retailer_profit_margin") or product.get("profit")
            profit_val = _safe_float(profit)
            if profit_val is not None:
                return profit_val
            return 0.0

        best = max(candidates, key=_score)
        
        # --- FIX: Ensure the match is actually a strong match ---
        if _score(best) < 0.85: # Require at least 85% confidence
            logger.warning(f"promo_cart.semantic_weak_match", query=query, top_score=_score(best))
            return None
        # --------------------------------------------------------
            
        product = best.get("product") or best
        return product if isinstance(product, dict) else None
    except Exception as e:
        logger.warning("promo_cart.semantic_exception", query=query, error=str(e))
        return None


def add_promo_items_to_cart(
    user_id: str,
    sku_codes: Optional[List[str]] = None,
    sku_names: Optional[List[str]] = None,
    quantity: int = 1,
) -> str:
    """
    Add items tied to promotions or profit asks into the order draft.
    - First tries search_products_by_sku using the provided codes (codes can be promo codes).
    - Then runs semantic_product_search on each provided SKU name (one-by-one) to resolve missing SKUs.
    - Updates the order draft with resolved SKU codes and a default quantity.
    """
    if debug_enabled():
        logger.info(
            "tool.call",
            tool="add_promo_items_to_cart",
            user_id=user_id,
            sku_code_count=len(sku_codes or []),
            sku_name_count=len(sku_names or []),
            quantity=quantity,
        )
    resolved_items: Dict[str, Dict[str, Any]] = {}
    skipped_existing: List[str] = []
    draft_snapshot: Dict[str, Any] = {}

    try:
        draft_snapshot = get_cart(user_id) or {}
    except Exception as e:
        logger.warning("promo_cart.read_draft_failed", user_id=user_id, error=str(e))
        draft_snapshot = {}

    existing_skus = {
        str(item.get("sku_code") or item.get("sku") or "").strip()
        for item in (draft_snapshot.get("skus") or draft_snapshot.get("items") or [])
        if isinstance(item, dict) and (item.get("sku_code") or item.get("sku"))
    }

    qty = quantity if isinstance(quantity, int) and quantity > 0 else 1

    # 1) Direct SKU/promotion codes lookup
    codes = [c for c in (sku_codes or []) if c]
    if codes:
        raw_resp = search_products_by_sku(codes)
        products = _parse_product_search(raw_resp)
        for prod in products:
            sku = _pick_sku(prod)
            if not sku:
                continue
            if sku in existing_skus:
                skipped_existing.append(sku)
                continue
            name = _pick_name(prod)
            price_info = _pick_price(prod)
            resolved_items[sku] = {
                "sku_code": sku,
                "name": _display_name(name),
                "qty": qty,
                "adjust_qty_by": qty,
                "price": price_info["price"],
                "base_price": price_info["base_price"],
                "final_price": price_info["final_price"],
                "line_total": None,
            }

    # 2) Semantic search by names (one-by-one)
    for name in sku_names or []:
        if not name:
            continue
        product = _semantic_best_match(name)
        if not product:
            continue
        sku = _pick_sku(product)
        if not sku:
            continue
        if sku in existing_skus:
            skipped_existing.append(sku)
            continue
        price_info = _pick_price(product)
        resolved_items[sku] = {
            "sku_code": sku,
            "name": _display_name(_pick_name(product)),
            "qty": qty,
            "adjust_qty_by": qty,
            "price": price_info["price"],
            "base_price": price_info["base_price"],
            "final_price": price_info["final_price"],
            "line_total": None,
        }

    if not resolved_items:
        return (
            "Bhai, promotion code ya naam se koi clear SKU resolve nahi hua "
            "ya phir jo mila wo already cart mein tha. "
            "Ek baar promo code ya item ka sahi naam bhej dein, phir add kar deta hoon."
        )

    store_id = draft_snapshot.get("store_id") or user_id
    operations = []
    for item in resolved_items.values():
        op = {
            "op": "ADD_ITEM",
            "sku_code": item["sku_code"],
            "qty": item["qty"],
            "merge_strategy": "INCREMENT",
        }
        if item.get("name"):
            op["name"] = item["name"]
        if item.get("product_retailer_id"):
            op["product_retailer_id"] = item["product_retailer_id"]
        operations.append(op)

    try:
        resp = agentflo_cart_tool(
            {
                "user_id": user_id,
                "store_id": store_id,
                "operations": operations,
            }
        ) or {}
        ok = bool(isinstance(resp, dict) and resp.get("ok"))
        if not ok:
            logger.warning(
                "promo_cart.update_failed",
                user_id=user_id,
                errors=resp.get("errors") if isinstance(resp, dict) else None,
                warnings=resp.get("warnings") if isinstance(resp, dict) else None,
            )
            return "System issue: the cart could not be updated. Please try again shortly."
    except Exception as e:
        logger.error("promo_cart.update_failed", error=str(e))
        return "System issue: the cart could not be updated. Please try again shortly."

    added_list = ", ".join(
        f"{_display_name(item.get('name'))} ×{item['qty']}"
        for item in resolved_items.values()
    )
    skipped_count = len(set(skipped_existing))

    response_lines = [f"Done, these items have been added to the cart: {added_list}."]
    if skipped_count:
        response_lines.append(
            f"{skipped_count} selected item(s) were already in the cart, so I skipped them."
        )

    try:
        refreshed = get_cart(user_id)
        if refreshed:
            response_lines.append("")
            response_lines.append(order_draft_template({"draft": refreshed}))
    except Exception as e:
        logger.warning("promo_cart.refresh_failed", user_id=user_id, error=str(e))

    return "\n".join(line for line in response_lines if line).strip()
