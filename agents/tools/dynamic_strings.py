import importlib
import os
from functools import lru_cache
from typing import Any, Dict
from dotenv import load_dotenv
load_dotenv()


def _normalize_lang(raw: str) -> str:
    s = (raw or "").strip().lower()
    if not s:
        return "UR"

    # -------------------------------------------------------
    # 1. Specific Region/Variant Checks (Check these first!)
    # -------------------------------------------------------
    # Mandarin Malaysia -> matches CN_MY_strings.py
    if s in {"cn_my", "zh_my", "zh-my", "malaysian chinese", "chinese malaysia"}:
        return "CN_MY"
    if "chinese" in s and "my" in s:
        return "CN_MY"

    # Bahasa Melayu (Malaysia) -> matches BM_MY_strings.py
    if s in {"bm", "bm_my", "ms", "ms_my", "malay", "bahasa", "bahasa melayu", "malaysian"}:
        return "BM_MY"

    # -------------------------------------------------------
    # 2. General Language Checks
    # -------------------------------------------------------
    # Standard Chinese / Mandarin -> matches CN_strings.py
    if s in {"cn", "zh", "chinese", "mandarin", "putonghua", "zh-cn"}:
        return "CN"

    # English -> matches EN_strings.py
    if s in {"en", "eng", "english"}:
        return "EN"

    # Arabic -> matches AR_strings.py
    if s in {"ar", "arabic", "عربي", "العربية"}:
        return "AR"

    # Urdu / Roman Urdu -> matches UR_strings.py
    if "urdu" in s or s in {"ur", "roman urdu", "roman-urdu", "pk"}:
        return "UR"

    # Fallback to default
    return "UR"


def get_lang_code() -> str:
    # You can change precedence if you prefer VN_LANGUAGE etc.
    raw = (
        os.getenv("PROMPT_LANGUAGE")
    )
    return _normalize_lang(raw)


@lru_cache(maxsize=1)
def _strings() -> Dict[str, str]:
    lang = get_lang_code()
    # This expects files like agents/tools/packs/CN_MY_strings.py to exist
    module_name = f"agents.tools.packs.{lang}_strings"
    try:
        mod = importlib.import_module(module_name)
        d = getattr(mod, "STRINGS", {})
        return d if isinstance(d, dict) else {}
    except Exception:
        # Safe fallback to UR
        try:
            mod = importlib.import_module("agents.tools.packs.UR_strings")
            d = getattr(mod, "STRINGS", {})
            return d if isinstance(d, dict) else {}
        except Exception:
            return {}


def t(key: str, **kwargs: Any) -> str:
    """
    Translate a key into the current language. If missing, returns the key.
    Supports format placeholders: t("mode_line", mode="X") etc.
    """
    template = _strings().get(key) or key
    try:
        return template.format(**kwargs)
    except Exception:
        return template