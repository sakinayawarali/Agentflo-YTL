import json
import os
import sys
from typing import Any, Dict, Optional, Tuple

# Updated with CN, CN_MY, and BM packs
LANGUAGE_PACKS: Dict[str, Dict[str, str]] = {
    "EN": {"folder": "English", "config": "EN_agent_config.json", "persona": "EN_agent_persona.txt"},
    "AR": {"folder": "Arabic", "config": "AR_agent_config.json", "persona": "AR_agent_persona.txt"},
    "UR": {"folder": "Urdu", "config": "UR_agent_config.json", "persona": "UR_agent_persona.txt"},
    "CN": {"folder": "Mandarin China", "config": "CN_agent_config.json", "persona": "CN_agent_persona.txt"},
    "CN_MY": {"folder": "Mandarin Malaysia", "config": "CN_MY_agent_config.json", "persona": "CN_MY_agent_persona.txt"},
    "BM": {"folder": "Malay", "config": "BM_MY_agent_config.json", "persona": "BM_MY_agent_persona.txt"},
}

SALES_MODE_BLOCKS: Dict[str, str] = {
    "OUTBOUND": "outbound.txt",
    "INBOUND": "inbound.txt",
}

TARGET_SEGMENT_BLOCKS: Dict[str, str] = {
    "ACQUISITION_NEW": "acquisition_new.txt",
    "GROWTH_EXISTING": "growth_existing.txt",
    "RETENTION_AT_RISK": "retention_at_risk.txt",
    "RECOVERY_CHURNED": "recovery_churned.txt",
}

OBJECTIVE_BLOCKS: Dict[str, str] = {
    "DRIVE_REORDERS": "drive_reorders.txt",
    "PROMOTE_NEW_ARRIVALS": "promote_new_arrivals.txt",
    "UPSELL_CROSS_SELL": "upsell_cross_sell.txt",
    "WINBACK_DORMANT": "winback_dormant.txt",
}


def _normalize_language_code(language_code: Optional[str]) -> str:
    raw = (language_code or os.getenv("PROMPT_LANGUAGE", "UR") or "UR").strip()
    s = raw.upper()

    # -------------------------------------------------------
    # 1. Exact Matches (Short Codes)
    # -------------------------------------------------------
    if s in LANGUAGE_PACKS:
        return s
    
    # -------------------------------------------------------
    # 2. Specific Region/Variant Checks (Longer matches first)
    # -------------------------------------------------------
    # Mandarin Malaysia
    if "CN_MY" in s or "MALAYSIAN CHINESE" in s or ("CHINESE" in s and "MALAYSIA" in s):
        return "CN_MY"
    
    # -------------------------------------------------------
    # 3. General Language Checks
    # -------------------------------------------------------
    # English
    if "EN" in s or "ENGLISH" in s:
        return "EN"
    
    # Arabic
    if "AR" in s or "ARABIC" in s:
        return "AR"
    
    # Urdu (including Roman Urdu)
    if "UR" in s or "URDU" in s or "ROMAN" in s:
        return "UR"

    # Chinese (Standard/Mainland)
    if "CN" in s or "ZH" in s or "CHINESE" in s or "MANDARIN" in s:
        return "CN"

    # Bahasa Melayu
    if "BM" in s or "MALAY" in s or "BAHASA" in s:
        return "BM"

    # Default fallback
    return "UR"


def _resolve_language_paths(base_dir: str, language_code: Optional[str]) -> Tuple[str, str, str, str]:
    normalized_code = _normalize_language_code(language_code)
    language_pack = LANGUAGE_PACKS.get(normalized_code)

    if not language_pack:
        supported = ", ".join(sorted(LANGUAGE_PACKS.keys()))
        raise ValueError(f"Unsupported language code '{normalized_code}'. Supported codes: {supported}")

    lang_dir = os.path.join(base_dir, "languages", language_pack["folder"])

    config_path = os.path.join(lang_dir, language_pack["config"])
    persona_path = os.path.join(lang_dir, language_pack["persona"])
    template_path = os.path.join(base_dir, "prompt_template.txt")

    for path, label in (
        (config_path, "config"),
        (persona_path, "persona"),
        (template_path, "template"),
    ):
        if not os.path.exists(path):
            raise FileNotFoundError(f"{label.title()} file not found for language '{normalized_code}' at {path}")

    return normalized_code, config_path, persona_path, template_path


def _normalize_sales_mode(sales_mode: Optional[str]) -> str:
    raw = (sales_mode or os.getenv("PROMPT_SALES_MODE", "INBOUND") or "INBOUND").strip().upper()
    if "INBOUND" in raw:
        return "INBOUND"
    if "OUTBOUND" in raw:
        return "OUTBOUND"
    return "INBOUND"


def _normalize_target_segment(segment: Optional[str]) -> str:
    raw = (segment or os.getenv("PROMPT_TARGET_SEGMENT", "RECOVERY_CHURNED") or "RECOVERY_CHURNED").strip().upper()
    s = raw.replace(" ", "_").replace("-", "_")
    if s in TARGET_SEGMENT_BLOCKS:
        return s
    return "RECOVERY_CHURNED"


def _normalize_objective(objective: Optional[str]) -> str:
    raw = (objective or os.getenv("PROMPT_OBJECTIVE", "WINBACK_DORMANT") or "WINBACK_DORMANT").strip().upper()
    s = raw.replace(" ", "_").replace("-", "_")
    if s in OBJECTIVE_BLOCKS:
        return s
    return "WINBACK_DORMANT"


def _read_block(path: str, label: str) -> str:
    if not os.path.exists(path):
        raise FileNotFoundError(f"{label} block file not found at {path}")
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def _resolve_block_path(base_dir: str, block_dir: str, filename: str) -> str:
    return os.path.join(base_dir, "blocks", block_dir, filename)


def _cleanup_compiled_prompts(base_dir: str) -> None:
    keep = {"compiled_prompt_outbound.txt", "compiled_prompt_2.txt"}
    for name in os.listdir(base_dir):
        if not (name.startswith("compiled_prompt") and name.endswith(".txt")):
            continue
        if name in keep:
            continue
        path = os.path.join(base_dir, name)
        if os.path.isfile(path):
            try:
                os.remove(path)
            except OSError:
                pass


def _apply_overrides(config_dict: Dict[str, Any], overrides: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Merge overrides into config_dict. Overrides win.
    Values are coerced to string at substitution time.
    """
    if not overrides or not isinstance(overrides, dict):
        return config_dict
    merged = dict(config_dict)
    for k, v in overrides.items():
        merged[str(k)] = v
    return merged


def get_system_prompt(
    language_code: Optional[str] = None,
    overrides: Optional[Dict[str, Any]] = None,
    sales_mode: Optional[str] = None,
    target_segment: Optional[str] = None,
    objective: Optional[str] = None,
) -> str:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    lang_code, config_path, persona_path, template_path = _resolve_language_paths(base_dir, language_code)

    output_path = os.path.join(base_dir, "compiled_prompt.txt")
    _cleanup_compiled_prompts(base_dir)
    print(f"Compiling new system prompt for language '{lang_code}'...")

    with open(config_path, "r", encoding="utf-8") as f:
        config_dict = json.load(f)

    # ✅ apply runtime overrides (Firestore / tenant mapping)
    config_dict = _apply_overrides(config_dict, overrides)

    with open(persona_path, "r", encoding="utf-8") as f:
        persona_text = f.read()

    with open(template_path, "r", encoding="utf-8") as f:
        template_text = f.read()

    sales_mode_code = _normalize_sales_mode(sales_mode)
    sales_mode_file = SALES_MODE_BLOCKS[sales_mode_code]
    sales_mode_path = _resolve_block_path(base_dir, "sales_modes", sales_mode_file)
    sales_mode_block = _read_block(sales_mode_path, "Sales mode")
    dictionary_path = os.path.join(base_dir, "dictionary.txt")
    dictionary_guide_block = _read_block(dictionary_path, "Dictionary guide")

    if sales_mode_code == "OUTBOUND":
        target_segment_code = _normalize_target_segment(target_segment)
        target_segment_file = TARGET_SEGMENT_BLOCKS[target_segment_code]
        target_segment_path = _resolve_block_path(base_dir, "target_segments", target_segment_file)
        target_segment_block = _read_block(target_segment_path, "Target segment")

        objective_code = _normalize_objective(objective)
        objective_file = OBJECTIVE_BLOCKS[objective_code]
        objective_path = _resolve_block_path(base_dir, "objectives", objective_file)
        objective_block = _read_block(objective_path, "Objective")
    else:
        target_segment_block = ""
        objective_block = ""

    # Fill persona placeholders first
    filled_persona_text = persona_text
    for key, value in config_dict.items():
        filled_persona_text = filled_persona_text.replace(f"{{{{{key}}}}}", str(value))

    # Insert persona content into main template placeholders
    config_dict["CULTURE_PACK_CONTENT"] = filled_persona_text
    config_dict["SALES_MODE_BLOCK"] = sales_mode_block
    config_dict["TARGET_SEGMENT_BLOCK"] = target_segment_block
    config_dict["OBJECTIVE_BLOCK"] = objective_block
    config_dict["DICTIONARY_GUIDE_BLOCK"] = dictionary_guide_block

    final_prompt = template_text
    for key, value in config_dict.items():
        final_prompt = final_prompt.replace(f"{{{{{key}}}}}", str(value))

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(final_prompt)

    print(f"System prompt saved to {output_path}")
    return final_prompt


if __name__ == "__main__":
    language_arg = sys.argv[1] if len(sys.argv) > 1 else None
    print("Running manual prompt compilation...")
    get_system_prompt(language_arg)
