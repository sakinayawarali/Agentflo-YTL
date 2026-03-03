# engro_templates_module.py
# All functions return WhatsApp-ready text blocks.

import random
import re
from datetime import datetime
import json
from typing import Any, Dict, List, Optional, Tuple
import importlib
import os
from functools import lru_cache
from dotenv import load_dotenv
from utils.logging import logger, debug_enabled
from agents.tools.dynamic_strings import t
load_dotenv()

MULTI_MESSAGE_DELIMITER = "\n\n<<NEXT_MESSAGE>>\n\n"


def _display_price(item):
    # Try specific keys first, then fall back to standard price fields
    return (
        item.get("total_buy_price_virtual_pack") or 
        item.get("final_price") or 
        item.get("price") or 
        item.get("base_price") or
        item.get("unit_price")
    )

# Helper: Compute profit/margin fields for templates
def _compute_profit_fields(container: dict, total_key: str = "total"):
    """
    Small helper to compute profit / margin values for templates.
    """
    if not isinstance(container, dict):
        return None, None, None

    total_buy = container.get(total_key)
    total_sell = container.get("total_sell")
    profit = container.get("profit")
    margin_pct = container.get("margin_pct")
    retailer_profit_margin = container.get("retailer_profit_margin")

    # If we don't have a sell total, we can't compute anything meaningful
    if total_sell is None:
        return None, None, None

    # Derive profit if missing
    if profit is None:
        if retailer_profit_margin is not None:
            qty = (
                container.get("qty")
                or container.get("quantity")
                or container.get("total_qty")
                or container.get("total_quantity")
            )
            try:
                rpm_val = float(retailer_profit_margin)
                if qty is not None:
                    profit = rpm_val * float(qty)
                else:
                    profit = rpm_val
            except (TypeError, ValueError):
                profit = None

    # Fallback: derive from total_sell and total_buy
    if profit is None and total_buy is not None and total_sell is not None:
        try:
            profit = float(total_sell) - float(total_buy)
        except (TypeError, ValueError):
            profit = None

    # Derive margin percentage if missing
    if margin_pct is None and profit is not None and total_buy not in (None, 0):
        try:
            margin_pct = (float(profit) / float(total_buy)) * 100.0
        except (TypeError, ValueError, ZeroDivisionError):
            margin_pct = None

    return total_sell, profit, margin_pct


def _coerce_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        if isinstance(val, str):
            cleaned = val.replace(",", "").strip()
            if not cleaned:
                return None
            return float(cleaned)
        return float(val)
    except (TypeError, ValueError):
        return None


def _format_currency(val: Any) -> Optional[str]:
    num = _coerce_float(val)
    if num is None:
        return None
    # Note: Currency symbol is often handled by the specific pack strings or config,
    # but this helper defaults to Rs. If needed, packs can override or formatting
    # logic can be made pack-aware. For now, we keep as is or let packs handle it.
    # ideally, currency prefix should come from config, but this is a low-level helper.
    return f"{num:,.2f}"


def _format_qty_display(qty: float) -> str:
    try:
        q = float(qty)
    except (TypeError, ValueError):
        return "1"
    if q.is_integer():
        return str(int(q))
    return f"{q:.2f}".rstrip("0").rstrip(".")


def format_sku_price_block(
    name: str,
    qty: Any,
    base_price: Any,
    final_price: Any,
    *,
    line_total: Any = None,
    discount_value: Any = None,
    index: Optional[int] = None,
) -> Tuple[List[str], Dict[str, Optional[float]]]:
    """
    Unified SKU display:
    1) _Name_
    Price: ~Base~ Final x qty
    Item Total: Total (Saving: Z)

    Robust fallback:
    - If unit price is missing but line_total exists, derive unit price from line_total/qty.
    - If line_total is missing but unit price exists, derive line_total from unit*qty.
    - If base price missing, fall back to final (to avoid showing Price: - when any money exists).
    - Saving always uses discount_value as the final line-level discount amount.
    """
    # Note: Currency symbol handling is simplified here.
    # Real localization often happens inside the pack functions using this helper.

    qty_val = _coerce_float(qty)
    if qty_val is None:
        qty_val = 1.0
    elif qty_val < 0:
        qty_val = 0.0
    qty_label = _format_qty_display(qty_val)

    base_val = _coerce_float(base_price)
    final_val = _coerce_float(final_price)
    line_total_val = _coerce_float(line_total)

    # --- FALLBACK LOGIC START ---
    # 1) If we have Line Total but no Unit Price, derive Unit Price
    if final_val is None and line_total_val is not None and qty_val > 0:
        try:
            final_val = round(float(line_total_val) / float(qty_val), 2)
        except Exception:
            final_val = None

    # 2) If we have Unit Price but no Line Total, derive Line Total
    effective_unit_tmp = final_val if final_val is not None else base_val
    if line_total_val is None and effective_unit_tmp is not None:
        try:
            line_total_val = round(float(effective_unit_tmp) * float(qty_val), 2)
        except Exception:
            line_total_val = None

    # 3) If base_val is missing, assume it's same as final (no discount formatting)
    if base_val is None:
        base_val = final_val
    # --- FALLBACK LOGIC END ---

    effective_unit = final_val if final_val is not None else base_val

    savings_total = _coerce_float(discount_value)
    if savings_total is not None:
        savings_total = round(float(savings_total), 2)
        if savings_total <= 0:
            savings_total = None

    prefix = f"{index}) " if index is not None else ""
    header = f"{prefix}_{name or t('item_fallback_generic')}_"

    # We use a neutral formatted string here; packs often prepend currency symbols
    if base_val is not None and final_val is not None and base_val > final_val:
        base_str = _format_currency(base_val) or "-"
        final_str = _format_currency(final_val) or "-"
        price_line = f"{t('price_label')}: ~{base_str}~ {final_str} x {qty_label}"
    elif effective_unit is not None:
        unit_str = _format_currency(effective_unit) or "-"
        price_line = f"{t('price_label')}: {unit_str} x {qty_label}"
    else:
        # Final fallback: if absolutely no price data, show placeholder
        price_line = f"{t('price_label')}: - x {qty_label}"

    item_total_line = f"{t('item_total_label')}: -"
    if line_total_val is not None:
        total_str = _format_currency(line_total_val) or "-"
        item_total_line = f"{t('item_total_label')}: {total_str}"
    if savings_total is not None and savings_total > 0:
        item_total_line += f" ({t('saving_label')}: {savings_total:,.2f})"

    return [header, price_line, item_total_line], {
        "line_total": line_total_val,
        "savings_total": savings_total,
        "unit_price": effective_unit,
        "qty": qty_val,
    }

def _classify_user_greeting(user_message: Optional[str]) -> Optional[str]:
    """
    Lightweight detector for common greeting intents in Latin + Arabic script.
    Returns a category string such as "salam" or "hello", else None.
    """
    if not isinstance(user_message, str):
        return None
    text = user_message.strip().lower()
    if not text:
        return None

    normalized = re.sub(r"[^0-9a-z\u0600-\u06FF\s]+", " ", text)
    normalized = re.sub(r"\s+", " ", normalized).strip()

    salam_hints = (
        "assalam o alaikum",
        "assalamualaikum",
        "assalamu alaikum",
        "asalam o alaikum",
        "salam alaikum",
        "salam",
        "salaam",
        "aoa",
        "slm",
        "السلام",
        "سلام",
    )
    hello_hints = (
        "hello",
        "hi",
        "hey",
        "helo",
        "hie",
        "hy",
        "hye",
        "heya",
        "hei",
        "hai",
        "hola",
        "مرحبا",
        "اهلا",
        "أهلا",
        "اهلاً",
    )

    for hint in salam_hints:
        if hint in normalized:
            return "salam"
    for hint in hello_hints:
        if hint in normalized:
            return "hello"
    if "good morning" in normalized or "good evening" in normalized or "good afternoon" in normalized:
        return "hello"
    return None


def _smart_greeting_line(
    user_message: Optional[str],
    salam_lines: List[str],
    hello_lines: List[str],
    default_lines: List[str],
) -> str:
    """
    Pick a greeting line that mirrors the user's opener when possible.
    """
    detected = _classify_user_greeting(user_message)
    if detected == "salam" and salam_lines:
        return random.choice(salam_lines)
    if detected == "hello" and hello_lines:
        return random.choice(hello_lines)
    return random.choice(default_lines or hello_lines or salam_lines or ["Hello"])

def extract_first_name(customer_name: Optional[str]) -> Optional[str]:
    """
    Best-effort first-name extractor for casual personalization.
    """
    if not isinstance(customer_name, str):
        return None
    cleaned = re.sub(r"\s+", " ", customer_name).strip()
    if not cleaned:
        return None
    first = cleaned.split(" ", 1)[0].strip(" ,.;:!?\"'()[]{}")
    if not first:
        return None
    if re.match(r"^[A-Za-z]", first):
        first = first[0].upper() + first[1:]
    return first


# ==========================
# Language pack dispatcher
# ==========================


_DEFAULT_PACK = "PK_Retail_RomanUrdu_v1"

# Map PROMPT_LANGUAGE -> pack module name
LANG_TO_PACK = {
    "UR": "PK_Retail_RomanUrdu_v1",
    "EN": "EN_GCC_Default_v1",
    "AR": "SA_Retail_Arabic_v1",
    "CN": "CN_Retail_Standard_v1",        # Mainland China
    "CN_MY": "CN_MY_Retail_Standard_v1",  # Malaysian Chinese
    "BM": "BM_MY_Retail_Standard_v1",     # Bahasa Melayu
    "BM_MY": "BM_MY_Retail_Standard_v1",  # Alias
}

def _normalize_lang(x: str) -> str:
    s = (x or "").strip().upper()
    
    # Standard
    if s in ("URDU", "UR"): return "UR"
    if s in ("ENGLISH", "EN"): return "EN"
    if s in ("ARABIC", "AR"): return "AR"
    
    # Chinese variants
    if s in ("CN", "ZH", "CHINESE"): return "CN"
    if s in ("CN_MY", "ZH_MY", "MALAYSIAN CHINESE"): return "CN_MY"
    
    # Malay variants
    if s in ("BM", "BM_MY", "MALAY", "BAHASA"): return "BM"
    
    # Return raw if 2-letter code matches directly
    return s or "UR"

@lru_cache(maxsize=32)
def _load_pack(pack_name: str):
    try:
        return importlib.import_module(f"agents.tools.packs.{pack_name}")
    except Exception as e:
        print(f"Warning: Failed to load language pack '{pack_name}': {e}. Fallback to default.")
        # fallback to default pack
        return importlib.import_module(f"agents.tools.packs.{_DEFAULT_PACK}")

def _resolve_pack_name() -> str:
    """
    Priority:
      1) REGION_CULTURE_PACK_NAME (explicit override if set in config/env, not implemented here but possible)
      2) PROMPT_LANGUAGE -> mapped pack name
      3) default pack
    """
    lang = _normalize_lang(os.getenv("PROMPT_LANGUAGE", "UR"))
    return LANG_TO_PACK.get(lang, _DEFAULT_PACK)

def _pack():
    return _load_pack(_resolve_pack_name())

def _dispatch(func_name: str, *args, **kwargs):
    mod = _pack()
    fn = getattr(mod, func_name, None)
    if debug_enabled():
        try:
            logger.info(
                "tool.call",
                tool=func_name,
                pack=mod.__name__,
                arg_types=[type(a).__name__ for a in args],
                kw_keys=list(kwargs.keys())[:12],
            )
        except Exception:
            pass
    if callable(fn):
        return fn(*args, **kwargs)

    # fallback to default pack if function missing
    default_mod = _load_pack(_DEFAULT_PACK)
    default_fn = getattr(default_mod, func_name, None)
    if callable(default_fn):
        return default_fn(*args, **kwargs)

    raise AttributeError(
        f"Template function '{func_name}' not found in '{mod.__name__}' or default pack."
    )

# ---- Templates: dispatch to selected pack ----
def greeting_template(
    user_message: Optional[str] = None,
    customer_name: Optional[str] = None,
) -> str:
    """
    LLM tool: unified greeting template. Use at the start of a chat to
    send the standard welcome message for the current language pack, ensuring a
    consistent first-touch experience across markets.

    Behavior:
    - Picks the pack using PROMPT_LANGUAGE env (falls back to default pack).
    - Returns WhatsApp-ready text only; no buttons or media.
    - Safe to call multiple times but typically used once per session.

    Arguments:
    - user_message (optional str): Last inbound user text to mirror their greeting
    - customer_name (optional str): Customer full name from metadata for personalization

    Example calls:
    - greeting_template()
    - greeting_template(user_message="salam")

    Returns:
    - str: Greeting text to send to the user.
    """
    return _dispatch(
        "greeting_template",
        user_message=user_message,
        customer_name=customer_name,
    )

def order_draft_template(
    cart: Optional[Dict[str, Any]] = None,
    draft: Optional[Dict[str, Any]] = None,
    *,
    ok: Optional[bool] = None,
    errors: Optional[List[Any]] = None,
    warnings: Optional[List[Any]] = None,
) -> str:
    """
    LLM tool: unified cart/draft listing template. Call this after cart
    operations (add, set qty, remove, undo) and when user asks to view cart/draft,
    to show the latest cart state in a consistent, WhatsApp-ready format.
    For explicit clear-cart intent, prefer a direct clear confirmation line.

    Behavior:
    - Uses PROMPT_LANGUAGE to pick the pack; falls back to default.
    - Renders items, totals, errors/warnings if present.
    - Text-only; no buttons/media. Safe to call repeatedly as cart mutates.

    Arguments:
    - cart (optional dict): Firestore cart object.
    - draft (optional dict): Draft/order snapshot (alternative shape).
    - ok (optional bool): Indicates operation success/failure.
    - errors (optional list): Fatal issues to display.
    - warnings (optional list): Non-fatal notices to display.

    Example calls:
    - order_draft_template(cart=cart_state, ok=True, warnings=["Promo applied"])
    - order_draft_template(draft=draft_state, ok=False, errors=["Missing store"])

    Returns:
    - str: WhatsApp-ready cart summary text.
    """
    # Forward named params so ADK tool schema includes them
    return _dispatch(
        "order_draft_template",
        cart=cart,
        draft=draft,
        ok=ok,
        errors=errors,
        warnings=warnings,
    )
def vn_order_draft_template(args: Dict[str, Any]) -> str:
    """
    LLM tool: voice-note friendly cart/draft listing template. Use after every
    cart operation when generating audio responses so users hear the latest cart
    summary. Mirrors order_draft_template but optimized for voice output.

    Behavior:
    - Uses PROMPT_LANGUAGE pack selection; falls back to default.
    - Formats items/totals/errors/warnings for spoken delivery.
    - Safe to call repeatedly as cart changes; text-only output for TTS.

    Arguments:
    - args (dict): Should include cart/draft data and optional ok/errors/warnings
      similar to order_draft_template input.

    Example calls:
    - vn_order_draft_template({"cart": cart_state, "ok": True})
    - vn_order_draft_template({"draft": draft_state, "ok": False, "errors": ["Missing store"]})

    Returns:
    - str: Voice-ready cart summary text to feed into TTS/voice note generation.
    """
    return _dispatch("vn_order_draft_template", args)
