from __future__ import annotations

import ast
import contextvars
import inspect
import json
import logging
import os
import re
import time
from functools import wraps
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)

# --- simple detectors ---
PRICE_RE = re.compile(r"\b(rs\.?|pkr|₨)\s*\d+\b|\b\d+\s*(rs\.?|pkr)\b", re.I)
DEFAULT_API_RE = re.compile(r"\bdefault_api\.", re.I)
PRINT_CALL_RE = re.compile(r"\bprint\s*\(", re.I)
OUTPUT_PRIVACY_RE = re.compile(
    r"\b(system prompt|developer message|internal (config|tool)|function ?call|api response|backend error|traceback|stack trace|exception|llm|language model)\b",
    re.I,
)
INJECTED_CONTEXT_BLOCK_RE = re.compile(
    r"\[(CUSTOMER_CONTEXT|SESSION_SUMMARY)\].*?\[/\1\]\s*",
    re.I | re.S,
)
INJECTED_CONTEXT_TAG_RE = re.compile(r"\[CONTEXT:[^\]]+\]\s*", re.I)

OPTOUT_PHRASES = (
    "stop", "unsubscribe", "opt out", "optout", "mat bhejo", "band karo", "no more", "cancel"
)

CONFIRM_YES_HINTS = (
    "yes",
    "jee",
    "ji",
    "haan",
    "han",
    "confirm",
    "kar do",
    "krdo",
    "theek hai",
    "thek hai",
    "ok place",
    "place order",
    "place karo",
    "order place",
    "confirm order",
    "go ahead",
    "ok proceed",
)

CONFIRM_NO_HINTS = (
    "no",
    "nahi",
    "nah",
    "cancel order",
    "stop order",
    "do not place",
    "dont place",
    "don't place",
)

# Words that suggest the user is editing/searching instead of purely confirming checkout.
CONFIRMATION_DETOUR_HINTS = (
    "add",
    "remove",
    "delete",
    "replace",
    "change",
    "update",
    "set",
    "qty",
    "quantity",
    "box",
    "boxes",
    "carton",
    "cartons",
    "item",
    "items",
    "sku",
    "product",
    "products",
    "promo",
    "promotion",
    "offer",
    "offers",
    "recommend",
    "recommendation",
    "top product",
    "top products",
    "search",
    "find",
    "price",
    "discount",
    "brand",
    "category",
    "aur",
    "but",
    "instead",
)

INJECTION_HINTS = (
    "ignore previous", "reveal system prompt", "show system prompt", "developer message",
    "print your instructions", "tool list", "use different user_id", "change user_id"
)

BLACKLISTED_OUTPUT_PHRASES = (
    "system is processing",
    "backend issue",
    "api response",
    "technical error",
    "loading data",
    "perfect!",
    "absolutely!",
    "amazing!",
    "let me assist you",
    "bot",
    " llm",
    " ai ",
    " agent",
)

# Guardrail language resources (keyed by normalized PROMPT_LANGUAGE)
_GUARDRAIL_LANG = {
    "UR": {
        "optout_phrases": (
            "stop",
            "unsubscribe",
            "opt out",
            "optout",
            "mat bhejo",
            "band karo",
            "no more",
            "cancel",
        ),
        "confirm_yes_hints": (
            "yes",
            "jee",
            "ji",
            "haan",
            "han",
            "confirm",
            "kar do",
            "krdo",
            "theek hai",
            "thek hai",
            "ok place",
            "place order",
            "place karo",
            "order place",
            "confirm order",
            "go ahead",
            "ok proceed",
        ),
        "confirm_no_hints": (
            "no",
            "nahi",
            "nah",
            "cancel order",
            "stop order",
            "do not place",
            "dont place",
            "don't place",
        ),
        "optout_ack": "Theek hai jee, aapko ab aur messages nahi aayenge.",
        "injection_block": (
            "Maaf kijiye, main is request mein madad nahi kar sakti. "
            "Aap product ka naam/SKU aur quantity bata dein jee."
        ),
        "privacy_block": (
            "Maaf kijiye, technical details share nahi kar sakti. "
            "Product ka naam/SKU aur quantity bata dein jee."
        ),
        "default_api_block": (
            "Maaf kijiye, system mein issue aa gaya. "
            "Aap apni request dobara bhej dein jee."
        ),
        "blacklist_block": (
            "Theek hai jee, main check kar ke bataati hoon. "
            "Aap item ka naam/SKU aur quantity share kar dein."
        ),
        "raw_json_block": (
            "Maaf kijiye, raw data share nahi kar sakti. "
            "Aap item ka naam/SKU aur quantity bata dein jee."
        ),
        "price_guard_block": (
            "Jee abhi live price confirm nahi ho raha. "
            "Aap item add kar dein—main draft pe confirm kar dungi jee."
        ),
        "semantic_options_template": (
            "yeh options mili hain:\n{options}\nKonsa add karun? Quantity bata dein."
        ),
        "semantic_no_results": "(matches nahi mile ya format missing)",
    },
    "EN": {
        "optout_phrases": ("stop", "unsubscribe", "opt out", "optout", "no more", "cancel"),
        "confirm_yes_hints": (
            "yes",
            "yeah",
            "confirm",
            "confirm it",
            "sure",
            "ok place",
            "place order",
            "place it",
            "go ahead",
            "ok proceed",
            "confirm order",
            "yes please",
        ),
        "confirm_no_hints": (
            "no",
            "cancel",
            "dont place",
            "don't place",
            "do not place",
            "stop order",
        ),
        "optout_ack": "Got it—you won't receive more messages.",
        "injection_block": (
            "Sorry, I can't help with that request. "
            "Tell me the product name/SKU and quantity."
        ),
        "privacy_block": (
            "Sorry, I can't share technical details. "
            "Please tell me the product name/SKU and quantity."
        ),
        "default_api_block": (
            "Sorry, something went wrong. Please send your request again."
        ),
        "blacklist_block": (
            "Alright, let me check. Share the product name/SKU and quantity."
        ),
        "raw_json_block": (
            "Sorry, I can't share raw data. Please tell me the product name/SKU and quantity."
        ),
        "price_guard_block": (
            "I can't confirm live prices yet. Add the item and I'll confirm on the draft."
        ),
        "semantic_options_template": (
            "Here are the options:\n{options}\nWhich one should I add? Please share the quantity."
        ),
        "semantic_no_results": "(no matches found or format missing)",
    },
    "AR": {
        "optout_phrases": ("توقف", "الغاء", "إلغاء", "لا ترسل", "قف", "لا مزيد"),
        "confirm_yes_hints": (
            "نعم",
            "ايه",
            "تمام",
            "أكد الطلب",
            "أرسل الطلب",
            "تابع",
            "استمر",
            "امضي قدماً",
        ),
        "confirm_no_hints": (
            "لا",
            "مش عايز",
            "الغاء الطلب",
            "لا ترسل",
            "لا تكمل",
        ),
        "optout_ack": "حاضر، لن يصلك مزيد من الرسائل.",
        "injection_block": (
            "عذرًا، لا أستطيع المساعدة في هذا الطلب. "
            "من فضلك أرسل اسم المنتج/الكود والكمية."
        ),
        "privacy_block": (
            "عذرًا، لا يمكنني مشاركة التفاصيل التقنية. "
            "أرسل اسم المنتج/الكود والكمية."
        ),
        "default_api_block": "عذرًا، حدث خطأ بالنظام. أعد إرسال طلبك لو سمحت.",
        "blacklist_block": "حاضر، سأتحقق. أرسل اسم المنتج/الكود والكمية.",
        "raw_json_block": "عذرًا، لا يمكنني مشاركة البيانات الخام. أرسل اسم المنتج/الكود والكمية.",
        "price_guard_block": (
            "حاليًا لا أستطيع تأكيد السعر. أضف الصنف وسأؤكد السعر في المسودة."
        ),
        "semantic_options_template": (
            "هذه الخيارات المتاحة:\n{options}\nأي واحد أضيف؟ وما الكمية؟"
        ),
        "semantic_no_results": "(لم يتم العثور على نتائج)",
    },
    "CN": {
        "optout_phrases": ("停止", "取消订阅", "退订", "不要发", "停止消息"),
        "confirm_yes_hints": ("是", "好的", "下单", "确认下单", "继续", "可以", "好吧"),
        "confirm_no_hints": ("不", "不要", "取消订单", "别下单", "不要下单"),
        "optout_ack": "好的，不再给您发送消息。",
        "injection_block": "抱歉，这个请求无法处理。请告诉我商品名称/SKU和数量。",
        "privacy_block": "抱歉，不能分享技术细节。请提供商品名称/SKU和数量。",
        "default_api_block": "抱歉，系统出错了，请再发一次。",
        "blacklist_block": "好的，我查一下。请告诉我商品名称/SKU和数量。",
        "raw_json_block": "抱歉，不能直接分享原始数据。请提供商品名称/SKU和数量。",
        "price_guard_block": "暂时无法确认实时价格。请先加到购物车，我会在草稿里确认。",
        "semantic_options_template": (
            "找到这些选项：\n{options}\n要添加哪一个？请告诉我数量。"
        ),
        "semantic_no_results": "(没有找到匹配项)",
    },
    "CN_MY": {
        "optout_phrases": ("停止", "取消订阅", "退订", "不要发", "停止消息"),
        "confirm_yes_hints": ("是", "好的", "下单", "确认下单", "继续", "可以", "好吧"),
        "confirm_no_hints": ("不", "不要", "取消订单", "别下单", "不要下单"),
        "optout_ack": "好的，不再给您发送消息。",
        "injection_block": "抱歉，这个请求无法处理。请告诉我商品名称/SKU和数量。",
        "privacy_block": "抱歉，不能分享技术细节。请提供商品名称/SKU和数量。",
        "default_api_block": "抱歉，系统出错了，请再发一次。",
        "blacklist_block": "好的，我查一下。请告诉我商品名称/SKU和数量。",
        "raw_json_block": "抱歉，不能直接分享原始数据。请提供商品名称/SKU和数量。",
        "price_guard_block": "暂时无法确认实时价格。请先加到购物车，我会在草稿里确认。",
        "semantic_options_template": (
            "找到这些选项：\n{options}\n要添加哪一个？请告诉我数量。"
        ),
        "semantic_no_results": "(没有找到匹配项)",
    },
    "BM": {
        "optout_phrases": ("henti", "berhenti", "unsubscribe", "stop", "jangan hantar", "batal"),
        "confirm_yes_hints": (
            "ya",
            "okay teruskan",
            "teruskan",
            "sahkan order",
            "ok proceed",
            "ya sila",
        ),
        "confirm_no_hints": ("tidak", "jangan", "batal order", "jangan hantar", "stop order"),
        "optout_ack": "Baik, saya tidak akan hantar mesej lagi.",
        "injection_block": (
            "Maaf, saya tak boleh bantu untuk permintaan itu. "
            "Beritahu nama/SKU produk dan kuantiti."
        ),
        "privacy_block": (
            "Maaf, saya tak boleh kongsi butiran teknikal. "
            "Sila beri nama/SKU produk dan kuantiti."
        ),
        "default_api_block": "Maaf, ada ralat sistem. Sila hantar semula permintaan.",
        "blacklist_block": (
            "Baik, saya semak dulu. Beri nama/SKU produk dan kuantiti."
        ),
        "raw_json_block": (
            "Maaf, saya tak boleh kongsi data mentah. "
            "Sila beri nama/SKU produk dan kuantiti."
        ),
        "price_guard_block": (
            "Saya belum boleh sahkan harga langsung. "
            "Tambah item dulu, nanti saya sahkan dalam draf."
        ),
        "semantic_options_template": (
            "Ini pilihan yang ada:\n{options}\nYang mana perlu saya tambah? Nyatakan kuantiti."
        ),
        "semantic_no_results": "(tiada padanan ditemui)",
    },
}


def _normalize_prompt_language(raw: str) -> str:
    s = (raw or "").strip().upper()
    if s in ("ENGLISH", "EN", "EN_GCC", "EN-GB", "EN_US"):
        return "EN"
    if s in ("URDU", "UR", "ROMAN URDU", "ROMAN_URDU", "ROMAN-URDU"):
        return "UR"
    if s in ("ARABIC", "AR", "AR-SA", "AR_EG", "AR_EGYPT"):
        return "AR"
    if s in ("CN", "ZH", "ZH_CN", "ZH-CN", "CHINESE"):
        return "CN"
    if s in ("CN_MY", "ZH_MY", "MALAYSIAN CHINESE"):
        return "CN_MY"
    if s in ("BM", "BM_MY", "MALAY", "BAHASA", "MS"):
        return "BM"
    return "UR"


def _guardrail_lang() -> Dict[str, Any]:
    lang = _normalize_prompt_language(os.getenv("PROMPT_LANGUAGE"))
    return _GUARDRAIL_LANG.get(lang) or _GUARDRAIL_LANG["UR"]


def _phrases(key: str) -> tuple:
    cfg = _guardrail_lang()
    return tuple(cfg.get(key) or ())


def _msg(key: str) -> str:
    cfg = _guardrail_lang()
    fallback = _GUARDRAIL_LANG["UR"].get(key, "")
    return cfg.get(key) or fallback or ""

# Template tool names in YOUR agent config
TEMPLATE_TOOL_NAMES = {
    "greeting_template",  # Initiates the conversation
    "order_draft_template",  # Cart update summary
    "vn_order_draft_template",  # VN-friendly draft summary
}

# Tools that return JSON envelopes or JSON-like strings.
TOOLS_EXPECT_JSON = {
    "semantic_product_search",
    "search_products_by_sku",
    "search_customer_by_phone",
    "retrieve_knowledge_base",
    # "product_info_csv_tool",  # CSV tool decoupled from runtime tool list.
}

# Tools that should NOT be auto-retried because they can mutate state or place orders.
NON_RETRYABLE_TOOL_NAMES = {
    "agentflo_cart_tool",
    "add_promo_items_to_cart",
    "placeOrderTool",
    "confirmOrderDraftTool",
}

CART_TOOL_NAME = "agentflo_cart_tool"
PROMO_CART_TOOL_NAME = "add_promo_items_to_cart"
SALES_INTEL_TOOL_NAME = "sales_intelligence_engine"

# Tools that should always carry user_id for auth/session safety.
USER_ID_REQUIRED_TOOL_NAMES = {
    CART_TOOL_NAME,
    PROMO_CART_TOOL_NAME,
    SALES_INTEL_TOOL_NAME,
    "placeOrderTool",
    "getLastOrdersTool",
    "confirmOrderDraftTool",
    "sendOrderConfirmationFlowTool",
}

CART_UPDATING_OBJECTIVES = {
    "CART_ITEMS",
    "MAX_SAVINGS_UNDER_BUDGET",
    "LOYALTY_REPLENISH",
    "BUDGET_RECOMMENDATION",
}

DEFAULT_TOOL_RETRIES = int(os.getenv("ADK_TOOL_RETRIES", "0") or 0)

# Keep the current callback context available for tool wrappers (best-effort).
_CALLBACK_CONTEXT = contextvars.ContextVar("adk_callback_context", default=None)
_GUARDED_TOOL_IDS: set[int] = set()
_LAST_FORCED_REPLY: Optional[str] = None
_LAST_FORCED_REPLY_BY_USER: Dict[str, str] = {}


def set_callback_context(callback_context) -> None:
    try:
        _CALLBACK_CONTEXT.set(callback_context)
    except Exception:
        pass


def get_callback_context():
    try:
        return _CALLBACK_CONTEXT.get()
    except Exception:
        return None


def clear_callback_context() -> None:
    try:
        _CALLBACK_CONTEXT.set(None)
    except Exception:
        pass


def _current_forced_reply_user_id() -> Optional[str]:
    try:
        callback_context = get_callback_context()
    except Exception:
        callback_context = None
    if callback_context is None:
        return None
    try:
        state = getattr(callback_context, "state", {}) or {}
        return _resolve_user_id_from_context(state, callback_context)
    except Exception:
        return None


def remember_forced_reply(text: Optional[str], user_id: Optional[str] = None) -> None:
    try:
        val = (text or "").strip() or None
        uid = _normalize_user_id_token(user_id) or _current_forced_reply_user_id()
        if uid:
            cache = globals().setdefault("_LAST_FORCED_REPLY_BY_USER", {})
            if val:
                cache[uid] = val
            else:
                cache.pop(uid, None)
            return
        globals()["_LAST_FORCED_REPLY"] = val
    except Exception:
        pass


def pop_forced_reply(user_id: Optional[str] = None) -> Optional[str]:
    try:
        uid = _normalize_user_id_token(user_id) or _current_forced_reply_user_id()
        if uid:
            cache = globals().setdefault("_LAST_FORCED_REPLY_BY_USER", {})
            return cache.pop(uid, None)
        val = globals().get("_LAST_FORCED_REPLY")
        globals()["_LAST_FORCED_REPLY"] = None
        return val
    except Exception:
        return None

# --- helpers ---
def _now_ms() -> int:
    return int(time.time() * 1000)

def _short(text: str, n: int = 260) -> str:
    t = (text or "").strip()
    return t if len(t) <= n else t[:n] + "…"

def _strip_injected_context(text: str) -> str:
    t = text or ""
    if not t:
        return ""
    t = INJECTED_CONTEXT_BLOCK_RE.sub("", t)
    t = INJECTED_CONTEXT_TAG_RE.sub("", t)
    return t.strip()

def _contains_any(text: str, phrases: Tuple[str, ...]) -> bool:
    t = (text or "").lower()
    return any(p in t for p in phrases)

def _contains_any_word(text: str, phrases: Tuple[str, ...]) -> bool:
    t = (text or "").lower()
    for phrase in phrases:
        p = (phrase or "").strip().lower()
        if not p:
            continue
        try:
            if re.search(rf"\b{re.escape(p)}\b", t):
                return True
        except re.error:
            # Fallback to substring if regex compilation fails
            if p in t:
                return True
    return False


def _has_any_phrase(text: str, phrases: Tuple[str, ...]) -> bool:
    return _contains_any_word(text, phrases) or _contains_any(text, phrases)


def _is_plain_order_confirmation(
    text: str,
    *,
    confirm_yes_hints: Tuple[str, ...],
    confirm_no_hints: Tuple[str, ...],
) -> bool:
    t = (text or "").strip().lower()
    if not t:
        return False
    if _has_any_phrase(t, confirm_no_hints):
        return False
    if not _has_any_phrase(t, confirm_yes_hints):
        return False
    if _has_any_phrase(t, CONFIRMATION_DETOUR_HINTS):
        return False
    if re.search(r"\b\d+\b", t):
        return False
    return True

def _read_latest_user_text(callback_context: Any, state: Dict[str, Any], *, prefer_state: bool = False) -> str:
    from_state = ""
    if isinstance(state, dict):
        from_state = (
            state.get("last_user_text")
            or state.get("user_message")
            or state.get("message_text")
            or state.get("text")
            or ""
        )

    from_content = ""
    try:
        content = getattr(callback_context, "user_content", None)
        if content and getattr(content, "parts", None):
            for part in content.parts:
                txt = getattr(part, "text", None)
                if isinstance(txt, str) and txt.strip():
                    from_content = txt
                    break
    except Exception:
        from_content = ""

    if prefer_state:
        chosen = from_state or from_content or ""
    else:
        chosen = from_content or from_state or ""
    return _strip_injected_context(chosen)

def _normalize_user_id_token(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        token = value.strip()
    elif isinstance(value, (int, float)):
        token = str(value).strip()
    else:
        return None
    return token or None


def _resolve_user_id_from_context(state: Dict[str, Any], callback_context: Any) -> Optional[str]:
    """
    Best-effort user_id lookup from callback/session context.
    """
    if isinstance(state, dict):
        for key in ("wa_user_id", "user_id", "whatsapp_user_id"):
            token = _normalize_user_id_token(state.get(key))
            if token:
                return token

    for attr in ("wa_user_id", "user_id"):
        token = _normalize_user_id_token(getattr(callback_context, attr, None))
        if token:
            return token

    return None


def _recover_user_id_in_args(tool_name: str, args: Dict[str, Any], callback_context: Any) -> Optional[str]:
    """
    Recover missing user_id from callback/session context and inject into args.
    Prefers wa_user_id since this is the canonical conversation identity.
    """
    if not isinstance(args, dict):
        return None

    state = getattr(callback_context, "state", {}) or {}
    recovered = _resolve_user_id_from_context(state, callback_context)
    if not recovered:
        return None

    if tool_name == CART_TOOL_NAME:
        payload = args.get("payload")
        if isinstance(payload, dict):
            payload_user = _normalize_user_id_token(payload.get("user_id"))
            if payload_user is None:
                payload["user_id"] = recovered

    existing = _normalize_user_id_token(args.get("user_id"))
    if existing is None:
        args["user_id"] = recovered

    return recovered


def _inject_user_id_into_invocation(
    func: Any,
    *,
    tool_name: str,
    args: Tuple[Any, ...],
    kwargs: Dict[str, Any],
    context_user_id: Optional[str],
) -> Tuple[Tuple[Any, ...], Dict[str, Any]]:
    """
    Inject recovered user_id directly into invocation args/kwargs so the real tool call
    executes with user identity even when the model omitted it.
    """
    if not context_user_id:
        return args, kwargs

    args_list = list(args)
    kwargs_copy = dict(kwargs)

    try:
        sig = inspect.signature(func)
        param_names = list(sig.parameters.keys())
    except Exception:
        sig = None
        param_names = []

    if tool_name == CART_TOOL_NAME:
        payload = kwargs_copy.get("payload")
        payload_idx: Optional[int] = None

        if payload is None and param_names and "payload" in param_names:
            payload_idx = param_names.index("payload")
            if payload_idx < len(args_list):
                payload = args_list[payload_idx]

        if isinstance(payload, dict):
            payload_user_id = _normalize_user_id_token(payload.get("user_id"))
            if payload_user_id is None:
                payload_copy = dict(payload)
                payload_copy["user_id"] = context_user_id
                if payload_idx is not None and payload_idx < len(args_list):
                    args_list[payload_idx] = payload_copy
                else:
                    kwargs_copy["payload"] = payload_copy
        return tuple(args_list), kwargs_copy

    if tool_name in USER_ID_REQUIRED_TOOL_NAMES:
        kw_user = _normalize_user_id_token(kwargs_copy.get("user_id"))
        if kw_user:
            return tuple(args_list), kwargs_copy

        if param_names and "user_id" in param_names:
            user_idx = param_names.index("user_id")
            if user_idx < len(args_list):
                pos_user = _normalize_user_id_token(args_list[user_idx])
                if pos_user is None:
                    args_list[user_idx] = context_user_id
            else:
                kwargs_copy["user_id"] = context_user_id
        else:
            kwargs_copy["user_id"] = context_user_id

    return tuple(args_list), kwargs_copy

def _coerce_selector_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, str):
        token = " ".join(value.split())
    elif isinstance(value, (int, float)):
        token = str(value).strip()
    else:
        return None
    return token or None


def _sales_intel_item_selector(raw_item: Any) -> Optional[str]:
    """
    Extract a valid selector token for sales_intelligence_engine items.
    Supports SKU code, brand, or category selectors.
    """
    if isinstance(raw_item, dict):
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
            token = _coerce_selector_text(raw_item.get(key))
            if token:
                return token
        return None

    return _coerce_selector_text(raw_item)


def _sales_intel_items_valid(raw_items: Any) -> bool:
    """
    Validate that sales_intelligence_engine items carry at least one usable selector
    when provided. Empty items are allowed.
    """
    if raw_items is None:
        return True

    if isinstance(raw_items, list):
        if not raw_items:
            return True
        return any(_sales_intel_item_selector(item) for item in raw_items)

    return _sales_intel_item_selector(raw_items) is not None

def _is_error_string(s: str) -> bool:
    t = (s or "").strip().lower()
    return t.startswith("error:") or "traceback" in t or "exception" in t

def _maybe_json_parse(s: str) -> Tuple[bool, Any]:
    try:
        return True, json.loads(s)
    except Exception:
        return False, None

def _looks_like_tool_json(text: str) -> bool:
    t = (text or "").strip()
    if not t:
        return False
    if t.startswith("```"):
        t = re.sub(r"^```[a-zA-Z0-9_-]*\\n", "", t).strip()
        t = re.sub(r"```$", "", t).strip()
    if (t.startswith("{") and t.endswith("}")) or (t.startswith("[") and t.endswith("]")):
        ok, parsed = _maybe_json_parse(t)
        if ok and isinstance(parsed, (dict, list)):
            return True
    if t.startswith("{") and ("\"success\"" in t or "\"data\"" in t):
        return True
    return False


def _extract_semantic_items(payload: Any) -> list:
    """
    Normalize semantic_product_search payloads into a list of item dicts.
    """
    if isinstance(payload, list):
        return payload
    if isinstance(payload, dict):
        for key in ("data", "items", "products", "results"):
            val = payload.get(key)
            if isinstance(val, list):
                return val
    return []


def _format_semantic_items(items: list, *, limit: int = 3) -> list[str]:
    """
    Build short WhatsApp-friendly option lines from semantic search results.
    """
    lines: list[str] = []

    def _pick_name(it: dict) -> str:
        product = it.get("product") if isinstance(it.get("product"), dict) else None

        def _first_text(container: dict, keys: tuple) -> str:
            for key in keys:
                val = container.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
            return ""

        name = _first_text(
            it,
            (
                "product_name",
                "name",
                "title",
                "sku_name",
                "display_name",
                "product_title",
                "product_label",
            ),
        )
        if not name and product:
            name = _first_text(
                product,
                (
                    "official_name",
                    "product_name",
                    "name",
                    "display_name",
                    "title",
                ),
            )
        if name:
            return name

        for key in (
            "product_name",
            "name",
            "title",
            "sku_name",
            "display_name",
            "product_title",
            "product_label",
        ):
            val = it.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        # Compose from brand + variant + flavor
        brand = (product or {}).get("brand") or it.get("brand") or ""
        variant = (product or {}).get("variant") or (product or {}).get("flavor") or it.get("variant") or it.get("flavor") or ""
        pieces = [p.strip() for p in (brand, variant) if isinstance(p, str) and p.strip()]
        if pieces:
            return " ".join(pieces)
        # Fallback to IDs
        for key in ("product_retailer_id", "sku", "sku_code", "product_id", "id"):
            val = it.get(key)
            if isinstance(val, (str, int)) and str(val).strip():
                return f"Item {val}"
        return ""

    def _pick_pack(it: dict) -> str:
        for key in ("pack_size", "size", "packaging", "unit_size", "unit", "uom"):
            val = it.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
        product = it.get("product") if isinstance(it.get("product"), dict) else None
        if product:
            for key in ("pack_size", "size", "packaging", "unit_size", "unit", "uom"):
                val = product.get(key)
                if isinstance(val, str) and val.strip():
                    return val.strip()
        return ""

    def _pick_price(it: dict) -> str:
        product = it.get("product") if isinstance(it.get("product"), dict) else None
        pricing = product.get("pricing") if isinstance(product, dict) else {}
        for key in (
            "price",
            "sell_price",
            "retail_price",
            "mrp",
            "unit_price",
            "list_price",
            "item_price",
            "total_buy_price_virtual_pack",
        ):
            val = it.get(key)
            if isinstance(val, (int, float)):
                return f"Rs {val:0.2f}"
            if isinstance(val, str) and val.strip():
                return f"Rs {val.strip()}"
        if isinstance(pricing, dict):
            for key in ("total_buy_price_virtual_pack", "unit_price", "price"):
                val = pricing.get(key)
                if isinstance(val, (int, float)):
                    return f"Rs {val:0.2f}"
                if isinstance(val, str) and val.strip():
                    return f"Rs {val.strip()}"
        return ""

    for idx, item in enumerate(items[:max(1, limit)], start=1):
        if not isinstance(item, dict):
            continue
        name = _pick_name(item) or "Item"
        pack = _pick_pack(item)
        price = _pick_price(item)
        parts = [f"{idx}) {name}"]
        if pack:
            parts.append(f"({pack})")
        if price:
            parts.append(f"- {price}")
        lines.append(" ".join(parts))

    return lines

def _cart_ops_mutate(payload: Dict[str, Any]) -> bool:
    ops = payload.get("operations")
    if not isinstance(ops, list) or not ops:
        return False
    op_names = {
        str(op.get("op")).upper()
        for op in ops
        if isinstance(op, dict) and op.get("op")
    }
    return bool(op_names and not op_names.issubset({"GET_CART"}))

def _cart_ops_get_only(payload: Dict[str, Any]) -> bool:
    ops = payload.get("operations")
    if not isinstance(ops, list) or not ops:
        return False
    op_names = {
        str(op.get("op")).upper()
        for op in ops
        if isinstance(op, dict) and op.get("op")
    }
    return bool(op_names and op_names.issubset({"GET_CART"}))

def _extract_cart(raw_result: Any) -> Optional[Dict[str, Any]]:
    if isinstance(raw_result, dict):
        if isinstance(raw_result.get("cart"), dict):
            return raw_result.get("cart")
        # Sometimes cart shape is returned directly
        if "items" in raw_result or "basket" in raw_result:
            return raw_result
    return None

def _recover_default_api_template_call(text: str) -> Optional[str]:
    """
    If the model returns a print(default_api.tool(...)) style string, extract the
    cart/draft payload and render the template with the real tool name.
    """
    if not text:
        return None

    target = None
    for name in ("vn_order_draft_template", "order_draft_template"):
        if name in text:
            target = name
            break
    if not target:
        return None

    def _extract_braced(start_at: int) -> Optional[str]:
        if start_at < 0:
            return None
        depth = 0
        start_idx = None
        for pos in range(start_at, len(text)):
            ch = text[pos]
            if ch == "{":
                depth += 1
                if depth == 1:
                    start_idx = pos
            elif ch == "}":
                if depth:
                    depth -= 1
                    if depth == 0 and start_idx is not None:
                        return text[start_idx : pos + 1]
        return None

    payload_key = None
    payload_str = None
    for key in ("cart", "draft"):
        match = re.search(rf"{key}\s*=", text)
        if not match:
            continue
        payload_str = _extract_braced(match.end())
        if payload_str:
            payload_key = key
            break

    if not payload_str:
        payload_str = _extract_braced(text.find("{"))

    if not payload_str:
        return None

    payload = None
    try:
        payload = json.loads(payload_str)
    except Exception:
        try:
            payload = ast.literal_eval(payload_str)
        except Exception:
            return None

    if not isinstance(payload, dict):
        return None

    data = payload.get("cart") or payload.get("draft") or payload
    try:
        from agents.tools.templates import order_draft_template, vn_order_draft_template

        if target == "vn_order_draft_template":
            vn_args = {
                "ok": payload.get("ok"),
                "errors": payload.get("errors"),
                "warnings": payload.get("warnings"),
            }
            if payload_key == "draft":
                vn_args["draft"] = data
            else:
                vn_args["cart"] = data
            return vn_order_draft_template(vn_args)

        if payload_key == "draft":
            return order_draft_template(
                draft=data,
                ok=payload.get("ok"),
                errors=payload.get("errors"),
                warnings=payload.get("warnings"),
            )
        return order_draft_template(
            cart=data,
            ok=payload.get("ok"),
            errors=payload.get("errors"),
            warnings=payload.get("warnings"),
        )
    except Exception:
        return None

def _safe_get_cart(
    user_id: Optional[str],
    store_id: Optional[str],
    *,
    state: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """
    Best-effort GET_CART using available hints. Avoids recursion by setting _cart_auto_fetch.
    """
    if not user_id:
        # fall back to state if available
        if isinstance(state, dict):
            user_id = state.get("user_id") or state.get("wa_user_id")
    if not user_id:
        return None

    # prefer store hints from state if not provided
    if not store_id and isinstance(state, dict):
        store_id = (
            state.get("store_id")
            or state.get("storeid")
            or state.get("store_code")
            or state.get("storecode")
        )

    if isinstance(state, dict):
        state["_cart_auto_fetch"] = True
    try:
        from agents.tools.order_draft_tools import get_cart
        cart = get_cart(user_id=user_id, store_id=store_id)
        if isinstance(cart, dict) and cart:
            return cart
    except Exception:
        pass
    finally:
        if isinstance(state, dict):
            state["_cart_auto_fetch"] = False
    return None

def _apply_order_draft_template(
    state: Dict[str, Any],
    cart: Optional[Dict[str, Any]],
    *,
    ok: Optional[bool] = None,
    errors: Optional[Any] = None,
    warnings: Optional[Any] = None,
) -> None:
    if not isinstance(cart, dict):
        return
    try:
        from agents.tools.templates import order_draft_template
        text = order_draft_template(cart=cart, ok=ok, errors=errors, warnings=warnings)
        if isinstance(text, str) and text.strip():
            state["forced_reply"] = text.strip()
    except Exception:
        return

def normalize_tool_result(tool_name: str, result: Any) -> Dict[str, Any]:
    """
    Normalize ANY tool output into a stable envelope:
      { success: bool, data: Any|None, error: {code,message,retryable}|None, source:{tool,timestamp_ms} }
    """
    src = {"tool": tool_name, "timestamp_ms": _now_ms()}

    # If tool already returns an envelope-ish dict, keep it
    if isinstance(result, dict) and "success" in result and ("data" in result or "error" in result):
        out = dict(result)
        out.setdefault("source", src)
        return out

    # String outputs (common in your repo)
    if isinstance(result, str):
        if tool_name in TEMPLATE_TOOL_NAMES:
            if _is_error_string(result):
                return {
                    "success": False,
                    "data": None,
                    "error": {"code": "TEMPLATE_ERROR", "message": _short(result), "retryable": True},
                    "source": src,
                }
            if result.strip():
                return {"success": True, "data": result, "error": None, "source": src}
            return {
                "success": False,
                "data": None,
                "error": {"code": "EMPTY_TEMPLATE", "message": "Empty template output", "retryable": True},
                "source": src,
            }

        if _is_error_string(result):
            return {
                "success": False,
                "data": None,
                "error": {"code": "TOOL_ERROR", "message": _short(result), "retryable": True},
                "source": src,
            }

        if tool_name in TOOLS_EXPECT_JSON:
            ok, parsed = _maybe_json_parse(result)
            if ok:
                return {"success": True, "data": parsed, "error": None, "source": src}
            return {
                "success": False,
                "data": None,
                "error": {"code": "BAD_TOOL_OUTPUT", "message": "Unparseable tool output", "retryable": True},
                "source": src,
            }

        return {"success": True, "data": result, "error": None, "source": src}

    # Other types
    return {"success": True, "data": result, "error": None, "source": src}


# =============================================================================
# Agent-level guardrails (compose with your existing before/after callbacks)
# =============================================================================

def before_agent_guard(callback_context) -> None:
    """
    Pre-turn guardrails:
    - opt-out detection
    - injection detection
    - store last_user_text for later checks
    Sets state['forced_reply'] when short-circuiting response text.
    """
    state = callback_context.state
    optout_phrases = _phrases("optout_phrases") or OPTOUT_PHRASES
    confirm_yes_hints = _phrases("confirm_yes_hints") or CONFIRM_YES_HINTS
    confirm_no_hints = _phrases("confirm_no_hints") or CONFIRM_NO_HINTS

    # Read latest user utterance, preferring raw state text over merged prompt.
    user_text = _read_latest_user_text(callback_context, state, prefer_state=True)

    if user_text:
        state["last_user_text"] = user_text

    # Reset/derive confirmation gate per turn
    if "awaiting_user_confirmation" not in state:
        state["awaiting_user_confirmation"] = False
    state["pending_confirmation"] = False
    if _contains_any_word(user_text, confirm_no_hints):
        state["pending_confirmation"] = False
        state["awaiting_user_confirmation"] = False
    elif _contains_any_word(user_text, confirm_yes_hints):
        # Treat explicit YES as confirming the pending order, even if a prior
        # confirmOrderDraftTool didn't set the flag.
        state["pending_confirmation"] = True
        state["awaiting_user_confirmation"] = True

    # Opt-out
    if _contains_any(user_text, optout_phrases):
        state["opted_out"] = True
        state["requires_tts_response"] = False  # do not generate VN for opt-out ack
        state["forced_reply"] = _msg("optout_ack")
        return

    # Injection attempts
    if _contains_any(user_text, INJECTION_HINTS):
        state["forced_reply"] = _msg("injection_block")
        return


def after_agent_guard(callback_context) -> None:
    """
    Post-turn guardrails:
    - forced_reply override (for opt-out, template verbatim, etc.)
    - price claim screening if no pricing evidence
    NOTE: order-id enforcement intentionally NOT implemented per your request.
    """
    state = callback_context.state
    lang_get = _msg

    forced = (state.get("forced_reply") or "").strip()
    if forced:
        state["Engro_response"] = forced
        remember_forced_reply(forced)
        return

    text = (state.get("Engro_response") or "").strip()
    if not text:
        return

    # Block technical/system leakage
    if OUTPUT_PRIVACY_RE.search(text):
        state["Engro_response"] = lang_get("privacy_block")
        remember_forced_reply(state["Engro_response"])
        return

    # Block malformed tool calls (default_api / print style) and attempt recovery
    if DEFAULT_API_RE.search(text) or PRINT_CALL_RE.search(text):
        recovered = _recover_default_api_template_call(text)
        if recovered:
            state["Engro_response"] = recovered
        else:
            state["Engro_response"] = lang_get("default_api_block")
        remember_forced_reply(state["Engro_response"])
        return

    # Block universal blacklisted phrases (keeps immersion)
    if _contains_any(text, BLACKLISTED_OUTPUT_PHRASES):
        state["Engro_response"] = lang_get("blacklist_block")
        remember_forced_reply(state["Engro_response"])
        return

    # Prevent raw tool JSON from leaking to user
    if _looks_like_tool_json(text):
        state["Engro_response"] = lang_get("raw_json_block")
        remember_forced_reply(state["Engro_response"])
        return

    # Block price claims if no evidence captured
    if PRICE_RE.search(text) and not state.get("pricing_evidence"):
        state["Engro_response"] = lang_get("price_guard_block")
        remember_forced_reply(state["Engro_response"])
        return


# =============================================================================
# Tool-level guardrails (call from wrappers)
# =============================================================================

def before_tool_guard(tool_name: str, args: Dict[str, Any], callback_context) -> Optional[Dict[str, Any]]:
    """
    Return a dict to SKIP tool execution (hard block) and return that dict as the tool result.
    """
    state = callback_context.state
    confirm_yes_hints = _phrases("confirm_yes_hints") or CONFIRM_YES_HINTS
    confirm_no_hints = _phrases("confirm_no_hints") or CONFIRM_NO_HINTS

    user_text = _read_latest_user_text(callback_context, state, prefer_state=True)
    if user_text:
        state["last_user_text"] = user_text

    if "awaiting_user_confirmation" not in state:
        state["awaiting_user_confirmation"] = False
    if "pending_confirmation" not in state:
        state["pending_confirmation"] = False

    if _has_any_phrase(user_text, confirm_no_hints):
        state["pending_confirmation"] = False
        state["awaiting_user_confirmation"] = False
    elif bool(state.get("awaiting_user_confirmation")) and _is_plain_order_confirmation(
        user_text,
        confirm_yes_hints=confirm_yes_hints,
        confirm_no_hints=confirm_no_hints,
    ):
        state["pending_confirmation"] = True
    else:
        state["pending_confirmation"] = False

    # Per-turn tool telemetry (best-effort).
    try:
        state["_turn_tool_calls"] = int(state.get("_turn_tool_calls") or 0) + 1
        seq = state.get("_turn_tool_sequence")
        if not isinstance(seq, list):
            seq = []
        if len(seq) < 64:
            seq.append(tool_name)
        state["_turn_tool_sequence"] = seq
    except Exception:
        pass

    # After a successful placeOrderTool call in this turn, block all additional tool work.
    if state.get("_order_placed_this_turn") and tool_name != "placeOrderTool":
        return {
            "success": False,
            "data": None,
            "error": {
                "code": "ORDER_ALREADY_PLACED",
                "message": "Order is already placed in this turn; no further tool calls are allowed.",
                "retryable": False,
            },
            "source": {"tool": tool_name, "timestamp_ms": _now_ms()},
        }

    # Hard checkout lock: when user explicitly confirms a shown draft, only placeOrderTool is allowed.
    if (
        state.get("awaiting_user_confirmation")
        and state.get("pending_confirmation")
        and tool_name != "placeOrderTool"
    ):
        return {
            "success": False,
            "data": None,
            "error": {
                "code": "ORDER_CONFIRMATION_LOCKED",
                "message": (
                    "User explicitly confirmed checkout. "
                    "Call placeOrderTool now; do not call search/cart/recommendation tools first."
                ),
                "retryable": False,
            },
            "source": {"tool": tool_name, "timestamp_ms": _now_ms()},
        }

    if tool_name == CART_TOOL_NAME:
        payload = args.get("payload") if isinstance(args, dict) else None
        if isinstance(payload, dict):
            state["_last_cart_payload"] = payload
            try:
                if _cart_ops_mutate(payload):
                    state["_turn_cart_mutation_count"] = int(state.get("_turn_cart_mutation_count") or 0) + 1
            except Exception:
                pass
    if tool_name == SALES_INTEL_TOOL_NAME:
        state["_last_sales_intel_args"] = dict(args) if isinstance(args, dict) else {}
    if tool_name == PROMO_CART_TOOL_NAME:
        state["_last_promo_args"] = dict(args) if isinstance(args, dict) else {}

    # If opted out, block non-template actions (you can tune allowlist)
    if state.get("opted_out") and tool_name not in TEMPLATE_TOOL_NAMES:
        return {
            "success": False,
            "data": None,
            "error": {"code": "OPTOUT", "message": "User opted out", "retryable": False},
            "source": {"tool": tool_name, "timestamp_ms": _now_ms()},
        }

    # placeOrderTool is allowed when selected.
    if tool_name == "placeOrderTool":
        return None

    # Basic arg checks for common tools
    if tool_name in ("agentflo_cart_tool", "sales_intelligence_engine", "add_promo_items_to_cart"):
        user_id = _normalize_user_id_token(args.get("user_id"))
        if tool_name == "agentflo_cart_tool":
            payload = args.get("payload")
            if isinstance(payload, dict):
                user_id = _normalize_user_id_token(payload.get("user_id")) or user_id
        require_user = tool_name in USER_ID_REQUIRED_TOOL_NAMES
        if require_user and user_id is None:
            user_id = _recover_user_id_in_args(tool_name, args, callback_context)
        if require_user and user_id is None:
            return {
                "success": False,
                "data": None,
                "error": {"code": "MISSING_USER", "message": "Missing user_id", "retryable": False},
                "source": {"tool": tool_name, "timestamp_ms": _now_ms()},
            }

    if tool_name in USER_ID_REQUIRED_TOOL_NAMES and tool_name not in ("agentflo_cart_tool", "sales_intelligence_engine", "add_promo_items_to_cart"):
        user_id = _normalize_user_id_token(args.get("user_id"))
        if user_id is None:
            user_id = _recover_user_id_in_args(tool_name, args, callback_context)
        if user_id is None:
            return {
                "success": False,
                "data": None,
                "error": {"code": "MISSING_USER", "message": "Missing user_id", "retryable": False},
                "source": {"tool": tool_name, "timestamp_ms": _now_ms()},
            }

    if tool_name == SALES_INTEL_TOOL_NAME:
        store_code = args.get("store_code")
        if not isinstance(store_code, str) or not store_code.strip():
            return {
                "success": False,
                "data": None,
                "error": {
                    "code": "MISSING_STORE",
                    "message": "Missing store_code for sales_intelligence_engine",
                    "retryable": False,
                },
                "source": {"tool": tool_name, "timestamp_ms": _now_ms()},
            }

        raw_items = args.get("items")
        if raw_items is not None and not _sales_intel_items_valid(raw_items):
            return {
                "success": False,
                "data": None,
                "error": {
                    "code": "BAD_ITEMS_SELECTOR",
                    "message": (
                        "items must include at least one valid selector. "
                        "Use sku/brand/category values (e.g., {'sku':'SKU00812'}, {'sku':'RIO'}, {'category':'NUTS'})."
                    ),
                    "retryable": False,
                },
                "source": {"tool": tool_name, "timestamp_ms": _now_ms()},
            }

    # Quantity sanity if provided
    qty = args.get("qty") or args.get("quantity")
    if qty is not None:
        try:
            if int(qty) < 0:
                return {
                    "success": False,
                    "data": None,
                    "error": {"code": "BAD_QTY", "message": "Quantity must be >= 0", "retryable": False},
                    "source": {"tool": tool_name, "timestamp_ms": _now_ms()},
                }
        except Exception:
            return {
                "success": False,
                "data": None,
                "error": {"code": "BAD_QTY", "message": "Quantity must be a number", "retryable": False},
                "source": {"tool": tool_name, "timestamp_ms": _now_ms()},
            }

    return None


def after_tool_guard(tool_name: str, raw_result: Any, callback_context) -> Dict[str, Any]:
    """
    - normalize tool outputs into envelope
    - capture evidence into state (pricing evidence)
    - template verbatim: store output into state['forced_reply']
    - after successful placeOrderTool: clear pending_confirmation gate (does NOT check order_id)
    """
    state = callback_context.state
    normalized = normalize_tool_result(tool_name, raw_result)

    # Semantic search: keep payload for downstream reasoning, but do not force an
    # immediate user reply here. Forced replies at this point can prematurely end
    # multi-step tool chains in the same turn.
    if tool_name == "semantic_product_search" and normalized.get("success"):
        state["pending_confirmation"] = False
        state["awaiting_user_confirmation"] = False
        state["_semantic_search_payload"] = normalized.get("data")

    # Sales intelligence: return the basket text verbatim to the user
    if tool_name == "sales_intelligence_engine" and normalized.get("success"):
        data = normalized.get("data")
        if isinstance(data, str) and data.strip():
            state["forced_reply"] = data.strip()
            remember_forced_reply(state["forced_reply"])

    # Capture pricing evidence
    if tool_name == CART_TOOL_NAME and normalized.get("success"):
        state["pricing_evidence"] = normalized.get("source")
    if tool_name == "sales_intelligence_engine" and normalized.get("success"):
        state["pricing_evidence"] = normalized.get("source")

    # Confirmation gates
    if tool_name == "confirmOrderDraftTool" and normalized.get("success"):
        state["pending_confirmation"] = False
        state["awaiting_user_confirmation"] = True

    # Clear confirmation + any forced replies after successful place order
    if tool_name == "placeOrderTool" and normalized.get("success"):
        state["pending_confirmation"] = False
        state["awaiting_user_confirmation"] = False
        state["_order_placed_this_turn"] = True
        try:
            state.pop("forced_reply", None)
        except Exception:
            pass
        remember_forced_reply(None)

    # Template verbatim enforcement
    if tool_name in TEMPLATE_TOOL_NAMES and normalized.get("success"):
        d = normalized.get("data")
        if isinstance(d, str) and d.strip():
            state["forced_reply"] = d.strip()
            remember_forced_reply(state["forced_reply"])
        elif isinstance(d, dict):
            # common patterns: {"result": "..."} or {"text":"..."}
            if isinstance(d.get("result"), str) and d["result"].strip():
                state["forced_reply"] = d["result"].strip()
                remember_forced_reply(state["forced_reply"])
            elif isinstance(d.get("text"), str) and d["text"].strip():
                state["forced_reply"] = d["text"].strip()
                remember_forced_reply(state["forced_reply"])

    # Cart/draft workflow: force order_draft_template after cart updates
    if tool_name == CART_TOOL_NAME and normalized.get("success"):
        if state.get("_cart_auto_fetch"):
            return normalized
        payload = state.get("_last_cart_payload") or {}
        if isinstance(payload, dict) and not _cart_ops_mutate(payload):
            # Do not force order-draft rendering for read-only cart lookups.
            return normalized
        user_id = payload.get("user_id") if isinstance(payload, dict) else None
        if not user_id:
            user_id = state.get("user_id")
        store_id = payload.get("store_id") if isinstance(payload, dict) else None

        cart = _safe_get_cart(user_id, store_id, state=state)

        # If we still don't have a cart, try to extract it from raw tool output
        if not cart:
            cart = _extract_cart(raw_result)

        # If cart remains empty, last resort: try a direct GET_CART with the best hints
        if not cart:
            cart = _safe_get_cart(user_id, store_id, state=state)

        _apply_order_draft_template(
            state,
            cart,
            ok=raw_result.get("ok") if isinstance(raw_result, dict) else None,
            errors=raw_result.get("errors") if isinstance(raw_result, dict) else None,
            warnings=raw_result.get("warnings") if isinstance(raw_result, dict) else None,
        )
        remember_forced_reply(state.get("forced_reply"))

    if tool_name == PROMO_CART_TOOL_NAME and normalized.get("success"):
        args = state.get("_last_promo_args") or {}
        user_id = args.get("user_id") if isinstance(args, dict) else None
        cart = _safe_get_cart(user_id, None, state=state)
        if not cart:
            cart = _extract_cart(raw_result)
        _apply_order_draft_template(state, cart)

    if tool_name == SALES_INTEL_TOOL_NAME and normalized.get("success"):
        args = state.get("_last_sales_intel_args") or {}
        objective = (args.get("objective") or "").strip().upper()
        if objective in CART_UPDATING_OBJECTIVES:
            user_id = args.get("user_id") if isinstance(args, dict) else None
            store_id = args.get("store_code") if isinstance(args, dict) else None
            cart = _safe_get_cart(user_id, store_id, state=state)
            if not cart:
                cart = _extract_cart(raw_result)
            _apply_order_draft_template(state, cart)

    return normalized


# =============================================================================
# Tool wrappers (guard + retry)
# =============================================================================

def _bind_args(func, args: Tuple[Any, ...], kwargs: Dict[str, Any]) -> Dict[str, Any]:
    try:
        sig = inspect.signature(func)
        bound = sig.bind_partial(*args, **kwargs)
        return dict(bound.arguments)
    except Exception:
        return dict(kwargs)


def _is_guarded(obj: Any) -> bool:
    if id(obj) in _GUARDED_TOOL_IDS:
        return True
    return bool(getattr(obj, "__adk_guarded__", False))


def _mark_guarded(obj: Any) -> None:
    _GUARDED_TOOL_IDS.add(id(obj))
    try:
        setattr(obj, "__adk_guarded__", True)
    except Exception:
        pass


def _should_retry(tool_name: str, normalized: Dict[str, Any], attempt: int, max_retries: int) -> bool:
    if attempt >= max_retries:
        return False
    if tool_name in NON_RETRYABLE_TOOL_NAMES:
        return False
    if normalized.get("success"):
        return False
    err = normalized.get("error") or {}
    if isinstance(err, dict):
        return bool(err.get("retryable"))
    return False


def _wrap_callable(func, *, tool_name: str, return_raw: bool, max_retries: int):
    if _is_guarded(func):
        return func

    @wraps(func)
    def _wrapper(*args: Any, **kwargs: Any):
        callback_context = kwargs.pop("callback_context", None) or get_callback_context()
        if callback_context is None:
            class _DummyCtx:
                def __init__(self, state):
                    self.state = state
            callback_context = _DummyCtx({})

        state = getattr(callback_context, "state", {}) or {}

        # Ensure auth/session-critical tools always carry user_id.
        context_user_id = _resolve_user_id_from_context(state, callback_context)
        args, kwargs = _inject_user_id_into_invocation(
            func,
            tool_name=tool_name,
            args=args,
            kwargs=kwargs,
            context_user_id=context_user_id,
        )

        # Auto-inject last user text into greeting_template if the signature accepts it.
        if tool_name == "greeting_template":
            try:
                sig = inspect.signature(func)
                params = sig.parameters
                has_var_kwargs = any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
                )
                accepts_user_msg = "user_message" in params or any(
                    p.kind == inspect.Parameter.VAR_KEYWORD for p in params.values()
                )
                accepts_customer_name = "customer_name" in params or has_var_kwargs
            except Exception:
                accepts_user_msg = False
                accepts_customer_name = False

            if accepts_user_msg and (kwargs.get("user_message") is None):
                user_msg = None
                if isinstance(state, dict):
                    user_msg = (
                        state.get("last_user_text")
                        or state.get("user_message")
                        or state.get("message_text")
                        or state.get("text")
                    )
                if user_msg:
                    kwargs["user_message"] = user_msg

            if accepts_customer_name and (kwargs.get("customer_name") is None):
                customer_name = None
                if isinstance(state, dict):
                    customer_name = (
                        state.get("customer_name")
                        or state.get("contact_name")
                        or state.get("retailer_name")
                        or state.get("owner_name")
                    )
                if isinstance(customer_name, str) and customer_name.strip():
                    kwargs["customer_name"] = customer_name.strip()

        args_dict = _bind_args(func, args, kwargs)

        blocked = before_tool_guard(tool_name, args_dict, callback_context)
        if blocked is not None:
            return blocked

        attempt = 0
        last_raw = None
        last_norm = None

        while True:
            raw = func(*args, **kwargs)
            last_raw = raw
            last_norm = after_tool_guard(tool_name, raw, callback_context)

            if not _should_retry(tool_name, last_norm, attempt, max_retries):
                break
            attempt += 1

        return last_raw if return_raw else (last_norm or last_raw)

    _mark_guarded(_wrapper)
    return _wrapper


def wrap_tool(
    tool: Any,
    *,
    tool_name: Optional[str] = None,
    return_raw: bool = True,
    max_retries: int = DEFAULT_TOOL_RETRIES,
) -> Any:
    """
    Wrap a tool (callable or FunctionTool-like) with guard + retry behavior.
    Returns the wrapped tool or the original tool if it cannot be wrapped.
    """
    if _is_guarded(tool):
        return tool

    name = (
        tool_name
        or getattr(tool, "name", None)
        or getattr(tool, "__name__", None)
        or getattr(getattr(tool, "func", None), "__name__", None)
        or "tool"
    )

    # FunctionTool-like objects often store the callable under these attrs.
    for attr in ("func", "_func", "callable", "_callable", "function"):
        target = getattr(tool, attr, None)
        if callable(target):
            wrapped = _wrap_callable(target, tool_name=name, return_raw=return_raw, max_retries=max_retries)
            try:
                setattr(tool, attr, wrapped)
                _mark_guarded(tool)
                return tool
            except Exception:
                pass

    if inspect.isfunction(tool) or inspect.isbuiltin(tool):
        wrapped = _wrap_callable(tool, tool_name=name, return_raw=return_raw, max_retries=max_retries)
        _mark_guarded(wrapped)
        return wrapped

    return tool
