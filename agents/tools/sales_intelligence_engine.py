import datetime
import os
import requests
import json
import time
from typing import List, Optional, Dict, Any
from dotenv import load_dotenv
from utils.logging import logger, debug_enabled
from agents.tools.cart_tools import agentflo_cart_tool
from agents.tools.api_tools import semantic_product_search, unwrap_tool_response
from agents.tools.templates import format_sku_price_block
from agents.tools.dynamic_strings import t
from agents.helpers.firestore_utils import user_root

try:
    from google.cloud import firestore  # type: ignore
except Exception:
    firestore = None  # type: ignore

try:
    # Lightweight PDF generator; fail softly if not installed.
    from fpdf import FPDF  # type: ignore
except Exception:
    FPDF = None  # type: ignore

load_dotenv()

# --- Configuration ---
DEFAULT_V2_ENDPOINT = "https://portal.agentflo.com/api/v2/basket/optimised"


def _ensure_v2_endpoint(raw_url: Optional[str]) -> str:
    """
    Force the orchestrator endpoint to v2 even if an older URL is configured.
    """
    if not raw_url:
        return DEFAULT_V2_ENDPOINT
    if "/api/v2/" in raw_url:
        return raw_url
    return DEFAULT_V2_ENDPOINT


ORCHESTRATOR_API_URL = _ensure_v2_endpoint(
    os.getenv("SALES_INTELLIGENCE_ENDPOINT")
)

TENANT_ID = os.getenv("TENANT_ID", "ebm")
SALES_INTEL_TOKEN = os.getenv("API_JWT_TOKEN")
REORDER_ENABLED = os.getenv("SALES_INTEL_REORDER_ENABLED", "").lower() in {"1", "true", "yes"}
AGENT_ID=os.getenv("AGENT_ID")

OBJECTIVE_RECOMMENDATION = "RECOMMENDATION"
OBJECTIVE_BUDGET = "BUDGET_RECOMMENDATION"
OBJECTIVE_PROMOS_ONLY = "PROMOTIONS_ONLY"
OBJECTIVE_CART_ITEMS = "CART_ITEMS"
OBJECTIVE_REORDER = "REORDER_INTELLIGENCE"
OBJECTIVE_TOP_PRODUCTS = "TOP_PRODUCTS"
OBJECTIVE_DEFAULT = OBJECTIVE_RECOMMENDATION

OBJECTIVE_ALIASES = {
    "BALANCED": OBJECTIVE_RECOMMENDATION,
    "DEFAULT": OBJECTIVE_RECOMMENDATION,
    "RECOMMEND": OBJECTIVE_RECOMMENDATION,
    "MAX_SAVINGS": OBJECTIVE_BUDGET,
    "MAX SAVINGS": OBJECTIVE_BUDGET,
    "MAX_SAVING": OBJECTIVE_BUDGET,
    "MAX_SAVINGS_UNDER_BUDGET": OBJECTIVE_BUDGET,
    "BUDGET": OBJECTIVE_BUDGET,
    "PROMOS": OBJECTIVE_PROMOS_ONLY,
    "PROMO": OBJECTIVE_PROMOS_ONLY,
    "PROMOTIONS": OBJECTIVE_PROMOS_ONLY,
    "PROMOTION_ONLY": OBJECTIVE_PROMOS_ONLY,
    "OFFERS": OBJECTIVE_PROMOS_ONLY,
    "OFFER": OBJECTIVE_PROMOS_ONLY,
    "SCHEME": OBJECTIVE_PROMOS_ONLY,
    "SCHEMES": OBJECTIVE_PROMOS_ONLY,
    "DEAL": OBJECTIVE_PROMOS_ONLY,
    "DEALS": OBJECTIVE_PROMOS_ONLY,
    "DISCOUNT": OBJECTIVE_PROMOS_ONLY,
    "DISCOUNTS": OBJECTIVE_PROMOS_ONLY,
    "CART": OBJECTIVE_CART_ITEMS,
    "CART_ITEM": OBJECTIVE_CART_ITEMS,
    "CART_ITEMS": OBJECTIVE_CART_ITEMS,
    "LOYALTY": OBJECTIVE_RECOMMENDATION,
    "LOYALTY_REPLENISH": OBJECTIVE_RECOMMENDATION,
    "REORDER": OBJECTIVE_REORDER,
    "REORDER_INTELLIGENCE": OBJECTIVE_REORDER,
}

OBJECTIVE_SET = {
    OBJECTIVE_RECOMMENDATION,
    OBJECTIVE_BUDGET,
    OBJECTIVE_PROMOS_ONLY,
    OBJECTIVE_CART_ITEMS,
    OBJECTIVE_REORDER,
    OBJECTIVE_TOP_PRODUCTS,
}

KNOWN_ERRORS = {
    "2001": "Insufficient forecast data — fallback template used",
    "2002": "No applicable promotions",
    "3001": "Budget parameters invalid",
    "4001": "No order history found",
    "4002": "Invalid date range",
}

CUSTOMER_SAFE_FALLBACK_TEXT = "Samajh nahi aaya, kya aap dobara keh sakte hain?"

PROMOTIONS_INLINE_MAX = 6
PENDING_RECOMMENDATIONS_FIELD = "pending_recommendations"
PENDING_RECOMMENDATIONS_MAX_ITEMS = 30


def _customer_error_fallback(log_event: str, **kwargs: Any) -> str:
    """
    Keep backend diagnostics in logs but never surface raw error internals to users.
    """
    try:
        logger.warning(log_event, **kwargs)
    except Exception:
        pass
    return CUSTOMER_SAFE_FALLBACK_TEXT


def _safe_float(val: Any) -> Optional[float]:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


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


def _normalize_user_id(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        token = value.strip()
    elif isinstance(value, (int, float)):
        token = str(value).strip()
    else:
        return None
    return token or None


def _utc_now_iso() -> str:
    return datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()


def _resolve_user_doc_ref(user_id: str):
    if not firestore:
        return None
    try:
        db = firestore.Client()
        kwargs: Dict[str, Any] = {"tenant_id": TENANT_ID}
        if AGENT_ID:
            kwargs["agent_id"] = AGENT_ID
        return user_root(db, user_id, **kwargs)
    except Exception as e:
        logger.warning("sales_intel.pending_recommendations.ref_failed", user_id=user_id, error=str(e))
        return None


def _compact_recommendation_items(normalized_items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    compact: List[Dict[str, Any]] = []
    seen: set[str] = set()
    for itm in normalized_items:
        if not isinstance(itm, dict):
            continue
        sku = str(itm.get("sku_code") or itm.get("sku") or "").strip()
        if not sku:
            continue
        sku_key = sku.lower()
        if sku_key in seen:
            continue
        qty = _coerce_qty(itm.get("qty"))
        if qty <= 0:
            continue
        compact.append(
            {
                "sku_code": sku,
                "name": _clean_display_name(itm.get("name"), fallback="Item"),
                "qty": qty,
                "product_retailer_id": itm.get("product_retailer_id"),
                "base_price": itm.get("base_price"),
                "final_price": itm.get("final_price"),
                "line_total": itm.get("line_total"),
                "discount_value": itm.get("discount_value"),
                "discount_pct": itm.get("discount_pct"),
            }
        )
        seen.add(sku_key)
        if len(compact) >= PENDING_RECOMMENDATIONS_MAX_ITEMS:
            break
    return compact


def _clear_pending_recommendations(user_id: Optional[str]) -> None:
    uid = _normalize_user_id(user_id)
    if not uid:
        return
    if not firestore:
        return
    user_ref = _resolve_user_doc_ref(uid)
    if user_ref is None:
        return
    try:
        user_ref.set({PENDING_RECOMMENDATIONS_FIELD: firestore.DELETE_FIELD}, merge=True)
    except Exception as e:
        logger.warning("sales_intel.pending_recommendations.clear_failed", user_id=uid, error=str(e))


def _persist_pending_recommendations(
    user_id: Optional[str],
    *,
    store_id: Optional[str],
    objective: Optional[str],
    normalized_items: List[Dict[str, Any]],
) -> None:
    uid = _normalize_user_id(user_id)
    if not uid:
        return
    if not firestore:
        return

    compact_items = _compact_recommendation_items(normalized_items)
    if not compact_items:
        _clear_pending_recommendations(uid)
        return

    user_ref = _resolve_user_doc_ref(uid)
    if user_ref is None:
        return

    payload = {
        "source": "sales_intelligence_engine",
        "objective": str(objective or OBJECTIVE_RECOMMENDATION),
        "store_id": str(store_id or "").strip() or None,
        "created_at": _utc_now_iso(),
        "created_epoch": int(time.time()),
        "items": compact_items,
    }
    try:
        user_ref.set({PENDING_RECOMMENDATIONS_FIELD: payload}, merge=True)
    except Exception as e:
        logger.warning("sales_intel.pending_recommendations.persist_failed", user_id=uid, error=str(e))


def _looks_like_sku_code(value: Any) -> bool:
    """
    Best-effort detector for SKU-like tokens (e.g., SKU00909).
    """
    if not isinstance(value, str):
        return False
    token = value.strip()
    if not token:
        return False
    upper = token.upper()
    return upper.startswith("SKU") and any(ch.isdigit() for ch in upper) and " " not in upper


def _clean_display_name(name: Any, *, fallback: str = "Item") -> str:
    """
    Keep names user-friendly; never show raw SKU codes as product names.
    """
    if isinstance(name, str):
        candidate = name.strip()
        if candidate and not _looks_like_sku_code(candidate):
            return candidate
    return fallback


def _normalize_objective(raw: Optional[str]) -> Optional[str]:
    """
    Convert free-text objective into a valid v2 enum if possible.
    """
    if not raw:
        return None
    candidate = str(raw).strip().upper()
    candidate = OBJECTIVE_ALIASES.get(candidate, candidate)
    if candidate in OBJECTIVE_SET:
        return candidate
    return None


def _objective_from_intent(intent_text: Optional[str]) -> Optional[str]:
    """
    Lightweight intent router to keep the tool resilient even if the agent
    sends only a hint instead of an explicit objective.
    """
    if not intent_text:
        return None
    text = intent_text.lower()

    if any(keyword in text for keyword in ["promo", "promotion", "offer", "offers", "scheme", "deal", "discount"]):
        return OBJECTIVE_PROMOS_ONLY

    if any(keyword in text for keyword in ["budget", "under", "cap", "limit", "save", "savings"]):
        return OBJECTIVE_BUDGET

    if any(keyword in text for keyword in ["cart", "already selected", "existing cart"]):
        return OBJECTIVE_CART_ITEMS

    if any(keyword in text for keyword in ["reorder", "last order", "repeat order", "order history"]):
        return OBJECTIVE_REORDER

    if "recommend" in text or "suggest" in text:
        return OBJECTIVE_RECOMMENDATION

    return None


def _infer_objective(
    requested_objective: Optional[str],
    max_budget: Optional[float],
    intent_hint: Optional[str],
    has_items: bool,
) -> str:
    """
    Final objective selection with explicit override -> intent -> constraints/items -> default.
    """
    normalized_request = _normalize_objective(requested_objective)
    if normalized_request:
        return normalized_request

    intent_based = _objective_from_intent(intent_hint)
    if intent_based:
        return intent_based

    if max_budget is not None and max_budget > 0:
        return OBJECTIVE_BUDGET

    if has_items:
        return OBJECTIVE_CART_ITEMS

    if intent_hint and "reorder" in intent_hint.lower():
        return OBJECTIVE_REORDER

    return OBJECTIVE_DEFAULT


def _compute_budget_tolerance(max_budget: Optional[float]) -> Optional[float]:
    """
    Fixed tolerance of 500 for budget mode.
    """
    if max_budget is None:
        return None
    try:
        budget_val = float(max_budget)
        if budget_val <= 0:
            return None
        return 500.0
    except Exception:
        return None


def _coerce_qty(val: Any) -> int:
    try:
        qty_val = int(val)
        return max(qty_val, 0)
    except (TypeError, ValueError):
        return 0


def _coerce_limit(val: Any) -> Optional[int]:
    try:
        limit_val = int(val)
        if limit_val <= 0:
            return None
        return limit_val
    except (TypeError, ValueError):
        return None


def _coerce_item_selector(value: Any) -> Optional[str]:
    """
    Normalize a selector token used in `items[].sku`.
    Supports SKU codes as well as brand/category text.
    """
    if value is None:
        return None

    token: Optional[str]
    if isinstance(value, str):
        token = " ".join(value.split())
    elif isinstance(value, (int, float)):
        token = str(value).strip()
    else:
        token = None

    if not token:
        return None

    token = token.strip()
    if _looks_like_sku_code(token):
        return token.upper()
    # Backend matching can be strict on selector casing for brand/category tokens.
    return token.upper()


def _extract_item_selector_and_qty(raw_item: Any) -> tuple[Optional[str], Optional[int]]:
    """
    Parse a single item selector from mixed item payloads.
    Accepts:
    - {"sku": "..."} where sku can be SKU code OR brand OR category
    - {"brand": "..."} / {"category": "..."} (normalized to {"sku": value})
    - plain string selector like "SOOPER" or "NUTS"
    """
    if isinstance(raw_item, dict):
        selector = None
        for key in (
            "sku",
            "sku_code",
            "skucode",
            "sku_id",
            "brand",
            "brand_name",
            "category",
            "category_name",
            "query",
            "name",
            "value",
        ):
            selector = _coerce_item_selector(raw_item.get(key))
            if selector:
                break

        qty_val = _coerce_qty(raw_item.get("qty") or raw_item.get("quantity"))
        qty = qty_val if qty_val > 0 else None
        return selector, qty

    selector = _coerce_item_selector(raw_item)
    return selector, None


def _normalize_items_input(items: Any) -> List[Dict[str, Any]]:
    """
    Normalize incoming `items` into API contract:
      [{"sku": "<selector>", "qty": <optional int>}]

    Selector may be:
    - exact SKU code (e.g., SKU00812)
    - brand (e.g., SOOPER, RIO)
    - category (e.g., NUTS)
    """
    if items is None:
        return []

    raw_entries: List[Any]
    if isinstance(items, list):
        raw_entries = items
    else:
        raw_entries = [items]

    cleaned: List[Dict[str, Any]] = []
    seen: Dict[str, int] = {}

    for raw_item in raw_entries:
        selector, qty = _extract_item_selector_and_qty(raw_item)
        if not selector:
            continue

        dedup_key = selector.strip().lower()
        existing_idx = seen.get(dedup_key)
        if existing_idx is None:
            entry: Dict[str, Any] = {"sku": selector}
            if qty is not None:
                entry["qty"] = qty
            cleaned.append(entry)
            seen[dedup_key] = len(cleaned) - 1
            continue

        # Keep first selector occurrence; only backfill qty if missing.
        if qty is not None and "qty" not in cleaned[existing_idx]:
            cleaned[existing_idx]["qty"] = qty

    return cleaned


def _resolve_prices(item: Dict[str, Any], qty: int) -> Dict[str, Optional[float]]:
    """
    Normalize price fields for an item. Returns a dict with base_price, final_price,
    discount_value, discount_pct, and line_total.
    """
    pricing = item.get("pricing") if isinstance(item.get("pricing"), dict) else {}

    def _pick_float(keys: list[str]) -> Optional[float]:
        for key in keys:
            val = _safe_float(item.get(key))
            if val is None and isinstance(pricing, dict):
                val = _safe_float(pricing.get(key))
            if val is not None:
                return val
        return None

    base_price = _pick_float(
        [
            "base_price",
            "consumer_price",
            "list_price",
            "mrp",
            "unit_price",
            "price",
        ]
    )

    final_price = _pick_float(
        [
            "final_price",
            "discounted_price",
            "unit_price_final",
            "unit_price",
            "price",
            "total_buy_price_virtual_pack",
        ]
    )

    unit_discount = _pick_float(["discount_value", "unit_discount"])
    line_discount = _pick_float(["discount_value_line", "line_discount"])
    if unit_discount is None:
        unit_discount = _pick_float(["discount"])
    if unit_discount is None and line_discount is not None and qty > 0:
        unit_discount = round(line_discount / max(qty, 1), 2)
    if line_discount is None and unit_discount is not None and qty > 0:
        line_discount = round(unit_discount * qty, 2)
    discount_pct = _pick_float(["discount_pct", "discountvalue", "discount_percentage"])

    if final_price is None and base_price is not None and discount_pct is not None:
        final_price = round(base_price * (1 - discount_pct / 100.0), 2)

    if unit_discount is None and base_price is not None and final_price is not None:
        unit_discount = round(base_price - final_price, 2)
    if line_discount is None and unit_discount is not None and qty > 0:
        line_discount = round(unit_discount * qty, 2)

    if discount_pct is None and unit_discount is not None and base_price:
        discount_pct = round((unit_discount / base_price) * 100.0, 2)

    line_total = _pick_float(["line_total", "linetotal", "lineamount", "line_total_amount"])
    if line_total is None and final_price is not None:
        line_total = round(final_price * qty, 2)

    return {
        "base_price": base_price,
        "final_price": final_price,
        "discount_value": unit_discount,
        "discount_value_line": line_discount,
        "discount_pct": discount_pct,
        "line_total": line_total,
    }


def _coerce_item_fields(item: Dict[str, Any]) -> Dict[str, Any]:
    """
    Build a normalized item structure used for formatting and order-draft persistence.
    """
    sku = (
        item.get("sku_code")
        or item.get("sku_id")
        or item.get("skucode")
        or item.get("sku")
        or item.get("item_number")
        or item.get("variant_code")
    )
    name = _clean_display_name(
        item.get("official_name")
        or item.get("name")
        or item.get("product_name")
        or item.get("sku_name")
        or item.get("description_en")
        or item.get("sku_desc")
        or item.get("description")
        or item.get("title"),
        fallback="Item",
    )

    qty = _coerce_qty(
        item.get("qty")
        or item.get("quantity")
        or item.get("forecast_qty")
        or item.get("recommended_qty")
    )
    pricing = _resolve_prices(item, qty)

    applied_promotions_raw = item.get("applied_promotions") or []
    promo_descriptions = []
    for promo in applied_promotions_raw:
        if not isinstance(promo, dict):
            continue
        desc = promo.get("description") or promo.get("promotion_description")
        code = promo.get("promotion_id") or promo.get("promotioncode") or promo.get("code")
        if desc:
            promo_descriptions.append(str(desc))
        elif code:
            promo_descriptions.append("Promotion applied")
    if not promo_descriptions and item.get("promotioncode"):
        promo_descriptions.append("Promotion applied")

    # profit = _safe_float(item.get("profit") or item.get("line_profit"))
    # profit_margin = _safe_float(
    #     item.get("profit_margin")
    #     or item.get("profit_margin_pct")
    #     or item.get("margin_pct")
    # )
    # if profit is None and profit_margin is not None and qty:
    #     profit = profit_margin * qty

    reason = item.get("primary_reason") or item.get("source") or item.get("block_reason")
    tags = item.get("tags") or item.get("recommendation_tags") or []

    return {
        "sku_code": str(sku) if sku else None,
        "name": str(name),
        "qty": qty,
        "product_retailer_id": item.get("product_retailer_id")
        or item.get("productid")
        or item.get("product_id")
        or item.get("retailer_id"),
        **pricing,
        # "profit": profit,
        # "profit_margin": profit_margin,
        "applied_promotions": promo_descriptions,
        "reason": reason,
        "tags": tags if isinstance(tags, list) else [tags],
    }


def _extract_payload_sections(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize the response into items/totals/summary regardless of nesting.
    """
    basket = api_response.get("basket") if isinstance(api_response, dict) else {}
    items = []
    totals = {}
    summary = {}
    objective_used = None
    customer_id = api_response.get("customer_id") if isinstance(api_response, dict) else None

    if isinstance(basket, dict):
        items = basket.get("items") or items
        totals = basket.get("totals") or totals
        summary = basket.get("summary") or summary
        objective_used = basket.get("objective_used") or objective_used
        customer_id = basket.get("customer_id") or customer_id

    if isinstance(api_response, dict):
        items = api_response.get("items") or items
        totals = api_response.get("totals") or totals
        summary = api_response.get("summary") or summary
        objective_used = api_response.get("objective_used") or api_response.get("objective") or objective_used
        customer_id = api_response.get("store_id") or api_response.get("customer_id") or customer_id

    return {
        "items": items or [],
        "totals": totals or {},
        "summary": summary or {},
        "objective_used": objective_used,
        "customer_id": customer_id,
    }


def _compute_totals(
    normalized_items: List[Dict[str, Any]],
    totals_raw: Dict[str, Any],
    summary_raw: Dict[str, Any],
) -> Dict[str, Optional[float]]:
    """
    Derive basket totals with sensible fallbacks when the API omits some fields.
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
        or summary_raw.get("discount_total")
        or summary_raw.get("total_discount")
    )
    # total_profit = _safe_float(summary_raw.get("total_profit") or totals_raw.get("total_profit"))
    # profit_margin_pct = _safe_float(summary_raw.get("profit_margin") or summary_raw.get("profit_margin_pct"))

    if subtotal is None:
        subtotal = 0.0
        for itm in normalized_items:
            qty = itm.get("qty") or 0
            base_price = _safe_float(itm.get("base_price"))
            if base_price is not None:
                subtotal += base_price * qty

    if grand_total is None:
        line_total_sum = 0.0
        for itm in normalized_items:
            lt = itm.get("line_total")
            if lt is not None:
                line_total_sum += float(lt)
            else:
                final_price = itm.get("final_price")
                if final_price is not None:
                    line_total_sum += float(final_price) * (itm.get("qty") or 0)
        grand_total = line_total_sum

    if discount_total is None and subtotal is not None and grand_total is not None:
        discount_total = max(subtotal - grand_total, 0.0)

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

    # if profit_margin_pct is None and total_profit is not None and subtotal:
    #     try:
    #         profit_margin_pct = round((float(total_profit) / float(subtotal)) * 100.0, 2)
    #     except Exception:
    #         profit_margin_pct = None

    return {
        "subtotal": subtotal,
        "grand_total": grand_total,
        "discount_total": discount_total,
        # "total_profit": total_profit,
        # "profit_margin_pct": profit_margin_pct,
    }


def _format_error_payload(err_obj: Dict[str, Any], prefix: str) -> str:
    code = err_obj.get("code") or err_obj.get("error_code")
    message = err_obj.get("message") or err_obj.get("reason") or t("unknown_error")
    details = err_obj.get("details")
    known = KNOWN_ERRORS.get(str(code)) if code else None
    public_hint = known or prefix
    return _customer_error_fallback(
        "sales_intel.error.redacted",
        code=code,
        message=str(message)[:500],
        details=_redact_sensitive_for_log(details) if isinstance(details, (dict, list)) else details,
        public_hint=public_hint,
    )


def _normalize_promotions(
    promos: List[Dict[str, Any]],
    promo_details: Any,
) -> List[Dict[str, Any]]:
    """
    Normalize promotions with unified fields for sorting/formatting.

    Supports new response shape:
    - promotions: [{promotioncode, skus_list: [{sku, skucode, base_price, final_price}, ...]}]
    - promo_details: {promotioncode: {promotion_description, discount_pct, ...}}
    """

    def _coerce_detail_map(raw: Any) -> Dict[str, Dict[str, Any]]:
        if isinstance(raw, dict):
            detail_map: Dict[str, Dict[str, Any]] = {}
            for key, val in raw.items():
                if not key or not isinstance(val, dict):
                    continue
                code_str = str(key)
                entry = dict(val)
                entry.setdefault("promotioncode", code_str)
                detail_map[code_str] = entry
            return detail_map

        if isinstance(raw, list):
            detail_map = {}
            for pd in raw:
                if not isinstance(pd, dict):
                    continue
                code = (
                    pd.get("promotioncode")
                    or pd.get("promotion_code")
                    or pd.get("code")
                )
                if code:
                    detail_map[str(code)] = pd
            return detail_map

        return {}

    details_map = _coerce_detail_map(promo_details)

    def _build_entry(
        code: str,
        sku_entry: Optional[Dict[str, Any]],
        promo: Dict[str, Any],
        detail: Dict[str, Any],
    ) -> Dict[str, Any]:
        sku_code = None
        sku_name = None
        sku_base_price = None
        sku_final_price = None
        sku_discount_value = None
        sku_discount_pct = None

        if isinstance(sku_entry, dict):
            sku_code = (
                sku_entry.get("skucode")
                or sku_entry.get("sku_code")
                or sku_entry.get("sku")
            )
            sku_name = (
                sku_entry.get("sku")
                or sku_entry.get("name")
                or sku_entry.get("description")
            )
            sku_base_price = _safe_float(sku_entry.get("base_price"))
            sku_final_price = _safe_float(sku_entry.get("final_price"))
            sku_discount_value = _safe_float(sku_entry.get("discount_value"))
            sku_discount_pct = _safe_float(sku_entry.get("discount_pct"))

        sku_code = sku_code or promo.get("skucode") or promo.get("sku_code") or promo.get("sku")
        sku_name_raw = (
            sku_name
            or promo.get("sku_name")
            or promo.get("name")
            or promo.get("description")
        )
        sku_name_clean = _clean_display_name(sku_name_raw, fallback=t("promotion_item_fallback"))

        description = (
            detail.get("promotion_description")
            or promo.get("promotion_description")
            or promo.get("description")
            or t("promotions_special_offer")
        )

        base_price = sku_base_price
        if base_price is None:
            base_price = _safe_float(detail.get("base_price") or promo.get("base_price"))
        final_price = sku_final_price
        if final_price is None:
            final_price = _safe_float(
                detail.get("final_price")
                or promo.get("final_price")
                or promo.get("price")
                or detail.get("price")
            )
        discount_value = sku_discount_value
        if discount_value is None:
            discount_value = _safe_float(detail.get("discount_value") or promo.get("discount_value"))
        discount_pct = sku_discount_pct
        if discount_pct is None:
            discount_pct = _safe_float(detail.get("discount_pct") or promo.get("discount_pct"))

        # Back-compute missing discount metrics when possible.
        if discount_pct is None and base_price and final_price and base_price > 0:
            delta = base_price - final_price
            if delta > 0:
                discount_pct = round((delta / base_price) * 100, 2)
                discount_value = discount_value or delta
        if discount_value is None and discount_pct is not None and base_price:
            discount_value = (discount_pct / 100.0) * base_price

        sort_score = (
            discount_pct
            if discount_pct is not None
            else (
                ((discount_value / base_price) * 100)
                if (discount_value and base_price)
                else (discount_value or 0.0)
            )
        )

        return {
            "code": code,
            "promotion_code": code,
            "sku": sku_code or sku_name_raw or code,
            "sku_code": sku_code,
            "name": sku_name_clean,
            "description": description,
            "offer_text": description,
            "base_price": base_price,
            "final_price": final_price,
            "discount_value": discount_value,
            "discount_pct": discount_pct,
            "sort_score": sort_score if sort_score is not None else 0.0,
        }

    normalized: List[Dict[str, Any]] = []
    promo_list = promos if isinstance(promos, list) else []

    for promo in promo_list:
        if not isinstance(promo, dict):
            continue
        promo_code_raw = (
            promo.get("promotioncode")
            or promo.get("promotion_code")
            or promo.get("code")
        )
        code = str(promo_code_raw) if promo_code_raw else "N/A"
        detail = details_map.get(code, {})

        sku_entries = promo.get("skus_list") or promo.get("skus") or promo.get("items") or []
        if not sku_entries:
            normalized.append(_build_entry(code, None, promo, detail))
            continue

        for sku_entry in sku_entries:
            normalized.append(_build_entry(code, sku_entry, promo, detail))

    return normalized


def _aggregate_promotions_by_code(promos: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Collapse per-SKU promotions into promotion-code buckets for concise previews.
    """
    buckets: Dict[str, Dict[str, Any]] = {}

    for promo in promos:
        if not isinstance(promo, dict):
            continue

        code = promo.get("code") or promo.get("promotion_code") or "N/A"
        code_str = str(code)
        bucket = buckets.setdefault(
            code_str,
            {
                "code": code_str,
                "description": promo.get("description") or promo.get("offer_text") or t("promotions_special_offer"),
                "discount_pct": promo.get("discount_pct"),
                "discount_value": promo.get("discount_value"),
                "sort_score": float(promo.get("sort_score") or 0.0),
                "base_price": promo.get("base_price"),
                "final_price": promo.get("final_price"),
                "skus": [],
            },
        )

        # Prefer richer values if they arrive later
        if bucket.get("discount_pct") is None and promo.get("discount_pct") is not None:
            bucket["discount_pct"] = promo.get("discount_pct")
        if bucket.get("discount_value") is None and promo.get("discount_value") is not None:
            bucket["discount_value"] = promo.get("discount_value")
        if promo.get("description"):
            bucket["description"] = promo.get("description")
        if promo.get("base_price") and not bucket.get("base_price"):
            bucket["base_price"] = promo.get("base_price")
        if promo.get("final_price") and not bucket.get("final_price"):
            bucket["final_price"] = promo.get("final_price")

        bucket["sort_score"] = max(bucket.get("sort_score") or 0.0, float(promo.get("sort_score") or 0.0))

        sku_label = _clean_display_name(promo.get("name") or promo.get("sku"), fallback="")
        sku_code = promo.get("sku_code") or promo.get("sku")
        if sku_label or sku_code:
            seen = bucket.setdefault("_sku_seen", set())
            dedup_key = (sku_label or "", sku_code or "")
            if dedup_key not in seen:
                seen.add(dedup_key)
                bucket.setdefault("skus", []).append(
                    {
                        "name": sku_label,
                        "sku_code": sku_code,
                    }
                )

    # Strip helper keys before returning
    for bucket in buckets.values():
        bucket.pop("_sku_seen", None)

    return list(buckets.values())


def _fetch_price_for_sku(query: str, sku_code: Optional[str], cache: Dict[str, Optional[float]]) -> Optional[float]:
    """
    Use semantic search to retrieve pricing (total_buy_price_virtual_pack) for a given SKU.
    Caches results to avoid repeated network calls.
    """
    key = (sku_code or query or "").strip().lower()
    if not key:
        return None
    if key in cache:
        return cache[key]

    try:
        resp = semantic_product_search(query)
        ok, payload, err = unwrap_tool_response(resp, system_name="semantic_product_search")
        if not ok or not payload:
            cache[key] = None
            return None
        if isinstance(payload, str):
            payload = json.loads(payload)

        price: Optional[float] = None
        fallback_price: Optional[float] = None
        items = []
        if isinstance(payload, dict):
            items = payload.get("data") or []
        elif isinstance(payload, list):
            items = payload
        for item in items or []:
            product = item.get("product") or {}
            code = (
                product.get("skucode")
                or item.get("skucode")
                or item.get("sku_code")
                or item.get("sku")
            )
            pricing = product.get("pricing") or {}
            candidate_price = _safe_float(pricing.get("total_buy_price_virtual_pack"))
            if fallback_price is None and candidate_price is not None:
                fallback_price = candidate_price
            if sku_code and code and str(code).lower() != str(sku_code).lower():
                continue
            price = candidate_price
            if price is not None:
                break
        if price is None:
            price = fallback_price

        cache[key] = price
        return price
    except Exception as e:
        logger.warning("promotions.price_lookup.exception", query=query, error=str(e))
        cache[key] = None
        return None


def _enrich_promotions_with_prices(promos: List[Dict[str, Any]]) -> None:
    """
    Populate base_price/final_price for promotions using semantic search pricing.
    Mutates the list in-place.
    """
    cache: Dict[str, Optional[float]] = {}
    for promo in promos:
        if not isinstance(promo, dict):
            continue
        # Skip if already have price
        base_price = _safe_float(promo.get("base_price"))
        if base_price is None:
            # Try SKU code first, then name
            q_primary = promo.get("name") or promo.get("sku") or ""
            q_fallback = promo.get("sku_code") or promo.get("sku") or ""
            base_price = _fetch_price_for_sku(q_primary, promo.get("sku_code"), cache)
            if base_price is None and q_fallback:
                base_price = _fetch_price_for_sku(q_fallback, promo.get("sku_code"), cache)
            promo["base_price"] = base_price

        discount_pct = _safe_float(promo.get("discount_pct"))
        if base_price is not None and discount_pct is not None:
            final_price = _safe_float(promo.get("final_price"))
            if final_price is None:
                final_price = round(base_price * (1 - discount_pct / 100.0), 2)
                promo["final_price"] = final_price
            if final_price is not None:
                promo.setdefault("discount_value", round(base_price - final_price, 2))


def _generate_promotions_pdf(promos_sorted: List[Dict[str, Any]]) -> Optional[bytes]:
    """
    Build a clean PDF (tabular) sorted by discount strength.
    """
    if not FPDF or not promos_sorted:
        return None

    PACK_SUFFIXES = [
        "Snack Pack PRJ X",
        "Family Pack",
        "Munch Pack",
        "Ticky Pack",
        "Snack Pack",
        "Half Roll",
        "Half Pack",
        "Packet",
        "Pack",
    ]

    def _split_brand_variant_pack(name: str) -> tuple[str, str, str]:
        clean = (name or "").strip()
        if not clean:
            return "", "", ""
        base = clean
        pack = ""
        lower = clean.lower()
        for pk in PACK_SUFFIXES:
            if lower.endswith(pk.lower()):
                pack = pk
                base = clean[: -len(pk)].strip(" -_/")
                break
        tokens = base.split()
        if not tokens:
            return "", "", pack
        if len(tokens) <= 2:
            brand = tokens[0]
            variant = " ".join(tokens[1:]) if len(tokens) > 1 else ""
        else:
            brand = " ".join(tokens[:2])
            variant = " ".join(tokens[2:])
        return brand, variant, pack

    def _fmt_discount(p: Dict[str, Any]) -> str:
        if p.get("discount_pct") is not None:
            pct = float(p["discount_pct"])
            return f"{pct:.1f}% off"
        if p.get("discount_value") is not None:
            return f"Rs {float(p['discount_value']):,.0f} off"
        return t("promotions_special_price")

    def _fmt_price(p: Dict[str, Any], key: str) -> str:
        val = p.get(key)
        if val is None:
            return "-"
        try:
            return f"{float(val):,.2f}"
        except Exception:
            return "-"

    def _trim(text: str, max_len: int = 70) -> str:
        if len(text) <= max_len:
            return text
        return text[: max_len - 3] + "..."

    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    pdf.set_fill_color(255, 255, 255)
    pdf.rect(0, 0, pdf.w, pdf.h, "F")  # force white page background (avoids dark-mode inversion)
    pdf.set_text_color(0, 0, 0)

    headers = ["#", t("pdf_column_item_name"), t("offer_label"), t("price_label"), t("discounted_price_label")]
    col_widths = [10, 80, 65, 15, 20]  # total ~190mm (wider last col to keep header on one line)
    line_h = 6
    page_margin_bottom = pdf.b_margin if hasattr(pdf, "b_margin") else 15

    def _draw_title():
        pdf.set_font("Arial", "B", 16)
        pdf.cell(0, 10, t("promotions_pdf_title"), ln=1)
        pdf.set_font("Arial", "", 11)
        pdf.cell(0, 8, t("promotions_pdf_subtitle"), ln=1)
        pdf.ln(2)

    def _nb_lines(text: str, width: float) -> int:
        if not text:
            return 1
        lines = 1
        cur_len = 0.0
        space = pdf.get_string_width(" ")
        for part in str(text).split("\n"):
            for word in part.split():
                w = pdf.get_string_width(word)
                if cur_len == 0:
                    cur_len = w
                elif cur_len + space + w <= width:
                    cur_len += space + w
                else:
                    lines += 1
                    cur_len = w
            cur_len = 0.0
        return max(1, lines)

    def _calc_height(text: str, width: float) -> float:
        return _nb_lines(text, width) * line_h

    def _render_table_row(values: List[str], *, check_page_break: bool = True) -> None:
        nonlocal pdf
        x_row = pdf.l_margin
        y_row = pdf.get_y()
        heights = [_calc_height(val, w) for val, w in zip(values, col_widths)]
        row_height = max(heights)

        # Manual page break
        if check_page_break and y_row + row_height > pdf.h - page_margin_bottom:
            pdf.add_page()
            _render_table_header()
            x_row = pdf.l_margin
            y_row = pdf.get_y()

        col_x = x_row
        for value, width in zip(values, col_widths):
            # Draw bounding box with uniform height
            pdf.rect(col_x, y_row, width, row_height)
            pdf.set_xy(col_x, y_row)
            pdf.multi_cell(width, line_h, value, border=0, align="L")
            col_x += width
            pdf.set_xy(col_x, y_row)
        pdf.set_xy(x_row, y_row + row_height)

    def _render_table_header():
        pdf.set_font("Arial", "B", 9)
        _render_table_row(headers, check_page_break=False)
        pdf.set_font("Arial", "", 9)

    _draw_title()
    _render_table_header()

    for idx, promo in enumerate(promos_sorted, 1):
        offer_text = promo.get("offer_text") or promo.get("description") or ""
        if promo.get("code"):
            offer_text = f"{offer_text}".strip()
        row = [
            str(idx),
            _trim(_clean_display_name(promo.get("name"), fallback=t("promotion_item_fallback")), 90),
            _trim(offer_text, 120),
            _fmt_price(promo, "base_price"),
            _fmt_price(promo, "final_price"),
        ]
        _render_table_row(row)

    try:
        raw = pdf.output(dest="S")
        if isinstance(raw, (bytes, bytearray)):
            return bytes(raw)
        # fpdf2 may return str; encode exactly once to preserve binary
        return str(raw).encode("latin-1", errors="ignore")
    except Exception as e:
        logger.warning("sales_intel.promos.pdf_encode_failed", error=str(e))
        return None


def _send_promotions_pdf_to_whatsapp(user_id: str, pdf_bytes: bytes, filename: str = "promotions.pdf") -> bool:
    """
    Upload and send the promotions PDF via WhatsApp document message.
    """
    transport = (os.getenv("WHATSAPP_TRANSPORT", "meta") or "meta").strip().lower()
    if transport == "twilio":
        if not user_id:
            logger.warning(
                "sales_intel.promos.pdf_skip",
                have_user=bool(user_id),
                transport="twilio",
            )
            return False
        try:
            from agents.helpers.adk_helper import ADKHelper  

            helper = ADKHelper()
            media_url = helper._twilio_upload_media_bytes(
                pdf_bytes,
                "application/pdf",
            )
            if not media_url:
                logger.warning("sales_intel.promos.pdf_upload_failed", transport="twilio")
                return False

            sent = helper._twilio_send_media(
                user_id,
                media_url,
                body=t("promotions_pdf_caption"),
                content_type="application/pdf",
            )
            if sent:
                logger.info("sales_intel.promos.pdf_sent", user_id=user_id)
                return True

            logger.warning("sales_intel.promos.pdf_send_failed", transport="twilio")
            return False
        except Exception as e:
            logger.warning("sales_intel.promos.pdf_exception", error=str(e), transport="twilio")
            return False

    elif transport=="meta":
        phone_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
        token = os.getenv("WHATSAPP_ACCESS_TOKEN")

        if not (phone_id and token and user_id):
            logger.warning(
                "sales_intel.promos.pdf_skip",
                have_phone=bool(phone_id),
                have_token=bool(token),
                have_user=bool(user_id),
            )
            return False

        media_url = f"https://graph.facebook.com/v23.0/{phone_id}/media"
        messages_url = f"https://graph.facebook.com/v23.0/{phone_id}/messages"
        headers = {"Authorization": f"Bearer {token}"}

        try:
            upload_resp = requests.post(
                media_url,
                headers=headers,
                data={"messaging_product": "whatsapp"},
                files={"file": (filename, pdf_bytes, "application/pdf")},
                timeout=20,
            )
            if upload_resp.status_code not in (200, 201):
                logger.warning(
                    "sales_intel.promos.pdf_upload_failed",
                    status=upload_resp.status_code,
                    body=upload_resp.text[:300],
                )
                return False

            media_id = (upload_resp.json() or {}).get("id")
            if not media_id:
                logger.warning("sales_intel.promos.pdf_upload_no_id")
                return False

            payload = {
                "messaging_product": "whatsapp",
                "to": user_id,
                "type": "document",
                "document": {
                    "id": media_id,
                    "filename": filename,
                    "caption": t("promotions_pdf_caption"),
                },
            }
            send_resp = requests.post(messages_url, headers=headers, json=payload, timeout=20)
            if send_resp.status_code in (200, 201):
                logger.info("sales_intel.promos.pdf_sent", user_id=user_id)
                return True

            logger.warning(
                "sales_intel.promos.pdf_send_failed",
                status=send_resp.status_code,
                body=send_resp.text[:300],
            )
            return False
        except Exception as e:
            logger.warning("sales_intel.promos.pdf_exception", error=str(e))
            return False


def _map_basket_to_order_draft(api_response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert orchestrator basket payload into order_draft structure for Firestore.
    """
    sections = _extract_payload_sections(api_response)
    normalized_items: List[Dict[str, Any]] = []
    for itm in sections.get("items") or []:
        if isinstance(itm, dict):
            normalized_items.append(_coerce_item_fields(itm))

    totals = _compute_totals(normalized_items, sections.get("totals") or {}, sections.get("summary") or {})
    skus = []

    for itm in normalized_items:
        try:
            sku_code = itm.get("sku_code")
            if not sku_code:
                continue
            qty = _coerce_qty(itm.get("qty"))
            price = itm.get("final_price")
            if price is None:
                price = itm.get("base_price")
            line_total = itm.get("line_total")
            if line_total is None and price is not None:
                line_total = float(price) * qty

            skus.append(
                {
                    "sku_code": str(sku_code),
                    "name": _clean_display_name(itm.get("name"), fallback="Item"),
                    "qty": qty,
                    "price": price,
                    "product_retailer_id": itm.get("product_retailer_id"),
                    "base_price": itm.get("base_price"),
                    "final_price": itm.get("final_price"),
                    "discount_value": itm.get("discount_value"),
                    "discount_pct": itm.get("discount_pct"),
                    "line_total": line_total,
                    # "profit": itm.get("profit"),
                    # "profit_margin": itm.get("profit_margin"),
                }
            )
        except Exception as e:
            logger.warning("sales_intel.draft_item_skip", error=str(e), item_preview=str(itm)[:200])
            continue

    return {
        "store_id": sections.get("customer_id") or api_response.get("customer_id") or "",
        "skus": skus,
        "total_amount": totals.get("grand_total") or 0.0,
    }


def _cart_is_empty(user_id: Optional[str], store_id: Optional[str]) -> bool:
    """
    Best-effort check to see if the user's cart has items.
    """
    if not user_id or not store_id:
        return False
    try:
        result = agentflo_cart_tool(
            {
                "user_id": user_id,
                "store_id": store_id,
                "operations": [{"op": "GET_CART"}],
            }
        )
        cart = (result or {}).get("cart") or {}
        items = cart.get("items") or []
        return len(items) == 0
    except Exception as e:
        logger.warning("sales_intel.cart_check_failed", user_id=user_id, store_id=store_id, error=str(e))
        return False


def _persist_order_draft_from_basket(
    user_id: Optional[str],
    api_response: Dict[str, Any],
    *,
    store_id_override: Optional[str] = None,
) -> tuple[bool, bool]:
    """
    Best-effort persistence of recommended basket into the order draft.
    Keeps cart available for follow-up edits (e.g., remove item #2).
    """
    if not user_id:
        return False, False
    try:
        draft_payload = _map_basket_to_order_draft(api_response)
        skus = draft_payload.get("skus") or []
        store_id = store_id_override or draft_payload.get("store_id")
        if not store_id or not skus:
            return False, False

        cart_empty = _cart_is_empty(user_id, store_id)
        if not cart_empty:
            return False, False

        operations = []
        for item in skus:
            sku = item.get("sku_code") or item.get("sku")
            qty = item.get("qty")
            if not sku or qty is None:
                continue
            operations.append(
                {
                    "op": "ADD_ITEM",
                    "sku_code": sku,
                    "qty": qty,
                    "name": item.get("name"),
                    "product_retailer_id": item.get("product_retailer_id"),
                    "price": item.get("price"),
                    "base_price": item.get("base_price"),
                    "final_price": item.get("final_price"),
                    "discount_value": item.get("discount_value"),
                    "discount_pct": item.get("discount_pct"),
                    "line_total": item.get("line_total"),
                    # "profit": item.get("profit"),
                    # "profit_margin": item.get("profit_margin"),
                }
            )

        if operations:
            result = agentflo_cart_tool(
                {
                    "user_id": user_id,
                    "store_id": store_id,
                    "operations": operations,
                }
            )
            return bool((result or {}).get("ok", True)), True
        return False, True
    except Exception as e:
        logger.warning("sales_intel.draft.persist_failed", user_id=user_id, error=str(e))
        return False, False


def _format_promotions_only(api_response: Dict[str, Any], user_id: Optional[str] = None) -> str:
    """
    Handler for objective='PROMOTIONS_ONLY'.
    For small result sets (<= PROMOTIONS_INLINE_MAX), return inline text only.
    For larger sets, attempt PDF delivery and include highlights in chat.
    """
    promos = api_response.get("promotions") or []
    promo_details = api_response.get("promo_details") or {}
    if not isinstance(promos, list) or not promos:
        return t("promotions_none")

    normalized = _normalize_promotions(promos, promo_details)
    if not normalized:
        return t("promotions_unavailable")

    # Enrich with prices via semantic search (best-effort)
    try:
        _enrich_promotions_with_prices(normalized)
    except Exception as e:
        logger.warning("sales_intel.promos.price_enrichment_failed", error=str(e))

    sorted_promos = sorted(
        normalized,
        key=lambda p: (-float(p.get("sort_score") or 0.0), str(p.get("name") or "")),
    )
    promo_groups = _aggregate_promotions_by_code(normalized)
    grouped_sorted = sorted(
        promo_groups,
        key=lambda p: (-float(p.get("sort_score") or 0.0), str(p.get("code") or "")),
    )
    preview_source = grouped_sorted if grouped_sorted else sorted_promos
    total_offers = len(preview_source)
    should_send_pdf = total_offers > PROMOTIONS_INLINE_MAX

    pdf_attempted = False
    pdf_sent = False
    if should_send_pdf:
        if user_id and FPDF:
            pdf_bytes = _generate_promotions_pdf(sorted_promos)
            pdf_attempted = True
            if pdf_bytes:
                pdf_sent = _send_promotions_pdf_to_whatsapp(user_id, pdf_bytes)
            else:
                logger.warning("sales_intel.promos.pdf_generation_failed")
        elif user_id and not FPDF:
            logger.warning("sales_intel.promos.pdf_missing_library")

    def _discount_label(p: Dict[str, Any]) -> str:
        if p.get("discount_pct") is not None:
            pct = float(p["discount_pct"])
            return f"{pct:.1f}% off"
        if p.get("discount_value") is not None:
            return f"Rs {float(p['discount_value']):,.0f} off"
        return t("promotions_special_price")

    def _price_label(p: Dict[str, Any]) -> str:
        base = p.get("base_price")
        final = p.get("final_price")
        if base and final and base != final:
            return f"Rs {final:,.2f} (was {base:,.2f})"
        if final:
            return f"Rs {final:,.2f}"
        if base:
            return f"Rs {base:,.2f}"
        return "-"

    preview = preview_source if not should_send_pdf else preview_source[:PROMOTIONS_INLINE_MAX]
    lines = []
    if should_send_pdf and pdf_sent:
        lines.append(t("promotions_intro_pdf_sent"))
    elif should_send_pdf and pdf_attempted:
        lines.append(t("promotions_intro_pdf_failed"))
    elif should_send_pdf and user_id:
        lines.append(t("promotions_intro_pdf_generation_failed"))
    elif should_send_pdf:
        lines.append(t("promotions_intro_top_with_pdf_hint"))
    else:
        lines.append(t("promotions_intro_active"))
    lines.append("")

    for idx, promo in enumerate(preview, 1):
        sku_list = promo.get("skus") or []
        sku_bits = []
        for sku in sku_list[:3]:
            label = _clean_display_name(sku.get("name"), fallback="")
            if label:
                sku_bits.append(label)
        if len(sku_list) > 3:
            sku_bits.append(t("promotions_more_skus", count=(len(sku_list) - 3)))

        sku_label = "; ".join(sku_bits) if sku_bits else _clean_display_name(
            promo.get("name"),
            fallback=t("promotion_item_fallback"),
        )
        lines.append(f"*{idx})* {sku_label}")
        lines.append(f"   • {t('offer_label')}: {promo.get('description')}")
        lines.append(f"   • {t('discount_label')}: {_discount_label(promo)}")
        price_str = _price_label(promo)
        if price_str and price_str != "-":
            lines.append("   • Discounted Price (incl. GST):")
            lines.append(f"     {price_str}")
        lines.append("")

    remaining = len(preview_source) - len(preview)
    if remaining > 0:
        if should_send_pdf and pdf_sent:
            lines.append(t("promotions_more_offers_pdf", count=remaining))
        else:
            lines.append(t("promotions_more_offers_active", count=remaining))

    return "\n".join(lines)


def _format_top_products_response(api_response: Dict[str, Any]) -> str:
    """
    Handler for objective='TOP_PRODUCTS'.
    Returns a concise list of top-selling product names.
    """
    products_raw = []
    if isinstance(api_response, dict):
        products_raw = api_response.get("products") or api_response.get("items") or []

    if not isinstance(products_raw, list) or not products_raw:
        return t("top_sellers_unavailable")

    normalized = []
    for item in products_raw:
        if not isinstance(item, dict):
            continue
        sku = item.get("sku") or item.get("sku_code") or item.get("skucode")
        name = _clean_display_name(
            item.get("name") or item.get("product_name") or item.get("description"),
            fallback="",
        )
        if not sku and not name:
            continue
        normalized.append({"sku": sku, "name": name})

    if not normalized:
        return t("top_sellers_unavailable")

    limit = api_response.get("limit")
    header = t("top_sellers_intro")
    if limit:
        header = t("top_sellers_intro_with_limit", limit=limit)

    lines = [header, ""]
    for idx, item in enumerate(normalized, 1):
        name = item.get("name")
        if name:
            lines.append(f"*{idx})* {name}")
        else:
            lines.append(f"*{idx})* {t('item_fallback_index', index=idx)}")
    return "\n".join(lines)


def _format_basket_response(
    api_response: Dict[str, Any],
    user_id: Optional[str] = None,
    *,
    added_to_cart: bool = False,
    cart_was_empty: bool = False,
) -> str:
    """
    always use to Format v2 basket responses (RECOMMENDATION, BUDGET_RECOMMENDATION, CART_ITEMS).
    Delegates to promotions/top-products handlers when relevant.
    """
    sections = _extract_payload_sections(api_response)
    objective_used = _normalize_objective(sections.get("objective_used"))
    if objective_used == OBJECTIVE_TOP_PRODUCTS or api_response.get("products"):
        return _format_top_products_response(api_response)
    if objective_used == OBJECTIVE_PROMOS_ONLY or api_response.get("promotions"):
        return _format_promotions_only(api_response, user_id=user_id)

    raw_items = sections.get("items") or []
    normalized_items = [_coerce_item_fields(itm) for itm in raw_items if isinstance(itm, dict)]

    if not normalized_items:
        err_obj = api_response.get("error")
        if isinstance(err_obj, dict):
            return _format_error_payload(err_obj, t("basket_no_suitable_prefix"))
        return (
            f"{t('basket_no_suitable_prefix')}. "
            f"{t('basket_no_suitable_message')}"
        )

    totals = _compute_totals(normalized_items, sections.get("totals") or {}, sections.get("summary") or {})
    summary = sections.get("summary") or {}
    mode = summary.get("mode")
    header_objective = objective_used or OBJECTIVE_DEFAULT

    lines = []
    if header_objective == OBJECTIVE_BUDGET:
        lines.append(t("basket_header_budget"))
    elif header_objective == OBJECTIVE_CART_ITEMS:
        lines.append(t("basket_header_cart"))
    elif header_objective == OBJECTIVE_REORDER:
        lines.append(t("basket_header_reorder"))
    else:
        lines.append(t("basket_header_recommendation"))

    if mode:
        lines.append(f"{t('mode_label')}: {mode}")
    lines.append("")


    for idx, itm in enumerate(normalized_items, 1):
        name = _clean_display_name(itm.get("name"), fallback=t("item_fallback_index", index=idx))
        qty = itm.get("qty") or 0
        base_price = _safe_float(itm.get("base_price"))
        final_price = _safe_float(itm.get("final_price"))
        discount_val = _safe_float(itm.get("discount_value"))
        line_total = _safe_float(itm.get("line_total"))
        if line_total is None and final_price is not None:
            line_total = float(final_price) * qty

        sku_lines, _ = format_sku_price_block(
            name,
            qty,
            base_price,
            final_price,
            line_total=line_total,
            discount_value=discount_val,
            index=idx,
        )
        lines.extend(sku_lines)

        if itm.get("applied_promotions"):
            lines.append(f"   • {t('promo_label')}: {'; '.join(itm['applied_promotions'])}")
        lines.append("")

    subtotal = totals.get("subtotal") if totals.get("subtotal") is not None else 0.0
    grand_total = totals.get("grand_total") if totals.get("grand_total") is not None else subtotal
    discount_total = totals.get("discount_total") if totals.get("discount_total") is not None else max(subtotal - grand_total, 0.0)
    # total_profit = totals.get("total_profit")
    # profit_margin_pct = totals.get("profit_margin_pct")

    lines.append("-----------------------------")
    if discount_total:
        lines.append(f"{t('subtotal_label')}: Rs {float(subtotal):,.2f}")
        lines.append(f"{t('total_discount_label')}: Rs {float(discount_total):,.2f}")
    lines.append(f"*{t('grand_total_label')} (incl. GST):*")
    lines.append(f"*Rs {float(grand_total):,.2f}*")
    # if total_profit is not None:
    #     lines.append(f"{t('profit_label')}: Rs {float(total_profit):,.2f}")
    # if profit_margin_pct is not None:
    #     lines.append(f"{t('profit_margin_label')}: {float(profit_margin_pct):.1f}%")
    lines.append("-----------------------------")
    lines.append("")
    if added_to_cart:
        lines.append(t("cta_added_to_cart"))
    elif cart_was_empty:
        lines.append(t("cta_cart_empty_add_recommendations"))
    elif cart_was_empty is False:
        lines.append(t("cta_recommendations_choose_items"))
    else:
        lines.append(t("cta_finalize"))

    return "\n".join(lines)


def sales_intelligence_engine(
    store_code: str,
    objective: Optional[str] = None,
    max_budget: Optional[float] = None,
    limit: Optional[int] = None,
    min_margin_pct: Optional[float] = None,
    items: Optional[List[Dict[str, Any]]] = None,
    note: Optional[str] = None,
    user_id: Optional[str] = None,
    intent: Optional[str] = None,
) -> str:
    """
    LLM tool: orchestrate Sales Intelligence v2 (Optimised Basket) to recommend,
    promo-apply, or reorder products for a customer/store. Use this to generate
    the basket summary text shown to the user.

    Cart persistence guidance:
    - The tool never auto-adds recommendations to the cart; it only presents the
      priced basket and asks the user what to add.
    - On follow-up, call cart tools explicitly when the user says to add or
      modify items.

    Supported objectives:
    - RECOMMENDATION (default), BUDGET_RECOMMENDATION (requires max_budget),
      PROMOTIONS_ONLY, CART_ITEMS (apply promos to provided selectors),
      REORDER_INTELLIGENCE (if enabled), TOP_PRODUCTS (top sellers list; supports
      constraints.limit).

    If objective is omitted, it is inferred from intent + constraints/items.
    Profit margin is informational only; min_margin_pct is ignored in v2.

    Arguments the model should send:
    - store_code (required str): Customer/store id (from search_customer_by_phone).
    - objective (optional str): One of the objectives above; otherwise inferred.
    - max_budget (optional float): Required for BUDGET_RECOMMENDATION.
    - limit (optional int): Max items for TOP_PRODUCTS.
    - items (optional list): Selector filters for objectives that support item scoping.
      Each selector is sent as {"sku": "<selector>"} where selector can be:
        - SKU code (e.g., "SKU00059")
        - brand (e.g., "SOOPER", "RIO")
        - category (e.g., "NUTS")
      Optional qty is supported mainly for exact SKU constraints.
    - user_id (required str): Chat/user id. Always required for auth/session-safe
      execution and follow-up flows.
    - intent (optional str) and note (optional str): Free-text hints (e.g., "promos for tea",
      "stay under 15k") to help objective selection.

    Cart persistence guidance:
    - The tool never auto-adds recommendations to the cart; it only presents the
      priced basket and asks the user what to add.
    - On follow-up, call cart tools explicitly when the user says to add or
      modify items.

    Example calls:
    - sales_intelligence_engine(store_code="<store_code>", user_id="923001234567")
    - sales_intelligence_engine(store_code="<store_code>", objective="BUDGET_RECOMMENDATION",
      max_budget=15000, user_id=session.user_id, intent="stay under budget")
    - sales_intelligence_engine(store_code="<store_code>", objective="PROMOTIONS_ONLY",
      items=[{"sku": "RIO"}], user_id=session.user_id)
    - sales_intelligence_engine(store_code="<store_code>", objective="PROMOTIONS_ONLY",
      items=[{"category": "NUTS"}], user_id=session.user_id)
    - sales_intelligence_engine(store_code="<store_code>", objective="CART_ITEMS",
      items=[{"sku": "SKU00059", "qty": 2}, {"sku": "SKU00310"}], user_id=session.user_id)
    - sales_intelligence_engine(store_code="<store_code>", objective="TOP_PRODUCTS", limit=5, user_id=session.user_id)

    Returns:
    - str: WhatsApp-ready summary of recommended/promoted basket (or top products)
      with totals and promos, or a readable error/diagnostic string.
    """
    if debug_enabled():
        logger.info(
            "tool.call",
            tool="sales_intelligence_engine",
            store_code=store_code,
            objective=objective,
            max_budget=max_budget,
            item_count=len(items) if isinstance(items, list) else (1 if items else 0),
            user_id=user_id,
        )

    user_id = _normalize_user_id(user_id)
    if not user_id:
        return _customer_error_fallback("sales_intel.error.auth_missing_user_id", user_id=user_id)

    # --- GUARD: Reject placeholder/fake store codes before hitting the API ---
    # The LLM sometimes passes "UNKNOWN_STORECODE" or a phone number when it lacks a real store code.
    # A 422 from the API in this case is noisy and misleading; fail fast with a clear message.
    def _is_bad_store_code(sc: str) -> bool:
        t = str(sc or "").strip().lower()
        if not t or t in {"unknown", "none", "null", "n/a", "na", "-", "0"}:
            return True
        if t.startswith("unknown"):   # catches "unknown_storecode", "unknown_store", etc.
            return True
        import re as _re
        if _re.fullmatch(r"\+?\d{10,15}", t):  # phone number used as fallback
            return True
        return False

    if _is_bad_store_code(store_code):
        logger.warning(
            "sales_intel.error.invalid_store_code",
            store_code=store_code,
            user_id=user_id,
        )
        return (
            "Aapka store code abhi available nahi hai. "
            "Pehle 'search_customer_by_phone' tool se store code hasil karein, "
            "phir dobara koshish karein."
        )
    # --- END GUARD ---

    if not SALES_INTEL_TOKEN:
        return _customer_error_fallback("sales_intel.error.missing_api_token", user_id=user_id)
    if not TENANT_ID:
        return _customer_error_fallback("sales_intel.error.missing_tenant_id", user_id=user_id)

    # Retained for backward compatibility; v2 ignores min_margin_pct.
    _ = min_margin_pct

    max_budget_val = _safe_float(max_budget)
    limit_val = _coerce_limit(limit)
    intent_tokens = [str(txt) for txt in (intent, note) if txt]
    intent_hint = " ".join(intent_tokens).strip() or None

    cleaned_items = _normalize_items_input(items)

    final_objective = _infer_objective(objective, max_budget_val, intent_hint, bool(cleaned_items))

    if final_objective == OBJECTIVE_BUDGET and (max_budget_val is None or max_budget_val <= 0):
        return _customer_error_fallback(
            "sales_intel.error.invalid_budget",
            user_id=user_id,
            max_budget=max_budget,
        )
    if final_objective == OBJECTIVE_REORDER and not REORDER_ENABLED:
        return _customer_error_fallback(
            "sales_intel.error.reorder_not_enabled",
            user_id=user_id,
        )

    budget_tolerance = _compute_budget_tolerance(max_budget_val) if final_objective == OBJECTIVE_BUDGET else None

    payload: Dict[str, Any] = {
        "tenant_id": TENANT_ID,
        "customer_id": store_code,
        "sales_intel_token": SALES_INTEL_TOKEN,
        "objective": final_objective,
        "agent_id": AGENT_ID,
    }

    constraints: Dict[str, Any] = {}
    if final_objective == OBJECTIVE_BUDGET and max_budget_val is not None:
        constraints["max_budget"] = max_budget_val
        if budget_tolerance is not None:
            constraints["tolerance"] = budget_tolerance
    if final_objective == OBJECTIVE_TOP_PRODUCTS and limit_val is not None:
        constraints["limit"] = limit_val
    if constraints:
        payload["constraints"] = constraints

    if final_objective == OBJECTIVE_BUDGET and max_budget_val is not None:
        payload["budget"] = max_budget_val
        if budget_tolerance is not None:
            payload["tolerance"] = budget_tolerance
    elif budget_tolerance is not None:
        # Only budget objective should carry tolerance; ensure it is not sent otherwise.
        budget_tolerance = None

    if cleaned_items:
        payload["items"] = cleaned_items

    if note:
        payload["note"] = note

    headers = {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
    }

    try:
        print(f"Sales Engine: {final_objective} @ {ORCHESTRATOR_API_URL}")
        logger.info(
            "sales_intel.orchestrator.request",
            endpoint=ORCHESTRATOR_API_URL,
            payload=_redact_sensitive_for_log(payload),
        )

        response = requests.post(
            ORCHESTRATOR_API_URL,
            json=payload,
            headers=headers,
            timeout=25,
        )
        response_payload = None
        response_preview = None
        try:
            response_payload = response.json()
        except Exception:
            response_preview = response.text[:2000] if response.text else None
        logger.info(
            "sales_intel.orchestrator.response",
            endpoint=ORCHESTRATOR_API_URL,
            status_code=response.status_code,
            payload=_redact_sensitive_for_log(response_payload) if isinstance(response_payload, (dict, list)) else None,
            body_preview=response_preview,
        )

        if response.status_code in (400, 422):
            err_payload = response_payload if isinstance(response_payload, dict) else {}

            err_obj: Dict[str, Any] = {}
            if isinstance(err_payload, dict):
                if isinstance(err_payload.get("error"), dict):
                    err_obj = err_payload.get("error") or {}
                else:
                    err_obj = err_payload
            return _format_error_payload(
                err_obj,
                t("invalid_request_verify_inputs"),
            )

        if response.status_code not in (200, 201):
            return _customer_error_fallback(
                "sales_intel.orchestrator.http_error.redacted",
                status_code=response.status_code,
                body_preview=(response.text[:500] if response.text else None),
            )

        data = response_payload if isinstance(response_payload, dict) else response.json()
        if isinstance(data, dict) and data.get("error"):
            err_obj = data.get("error")
            err_dict = err_obj if isinstance(err_obj, dict) else {"message": str(err_obj)}
            return _format_error_payload(err_dict, t("invalid_request"))

        sections = _extract_payload_sections(data if isinstance(data, dict) else {})
        normalized_items = [
            _coerce_item_fields(itm)
            for itm in (sections.get("items") or [])
            if isinstance(itm, dict)
        ]

        if final_objective in {
            OBJECTIVE_RECOMMENDATION,
            OBJECTIVE_BUDGET,
            OBJECTIVE_REORDER,
        }:
            _persist_pending_recommendations(
                user_id,
                store_id=store_code,
                objective=final_objective,
                normalized_items=normalized_items,
            )
        elif final_objective in {
            OBJECTIVE_TOP_PRODUCTS,
            OBJECTIVE_PROMOS_ONLY,
        }:
            _clear_pending_recommendations(user_id)

        if final_objective == OBJECTIVE_TOP_PRODUCTS:
            return _format_top_products_response(data)

        added_to_cart = False
        cart_was_empty = False
        if final_objective != OBJECTIVE_PROMOS_ONLY:
            cart_was_empty = _cart_is_empty(user_id, store_code)
        return _format_basket_response(
            data,
            user_id=user_id,
            added_to_cart=added_to_cart,
            cart_was_empty=cart_was_empty,
        )

    except Exception as e:
        return _customer_error_fallback(
            "sales_intel.orchestrator.exception.redacted",
            error_type=type(e).__name__,
            error=str(e),
        )
