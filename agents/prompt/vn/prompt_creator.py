import json
import os

def get_vn_prompt():
    base_dir = os.path.dirname(os.path.abspath(__file__))

    config_path   = os.path.join(base_dir, "vn_config.json")
    template_path = os.path.join(base_dir, "vn_template.txt")
    output_path   = os.path.join(base_dir, "vn_compiled_prompt.txt")
    culture_dir   = os.path.join(base_dir, "culture_packs")

    # OPTIONAL: if you really want caching, you can keep this, but make it
    # language-aware. Easiest: disable for now to avoid stale prompts.
    # if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
    #     with open(output_path, "r", encoding="utf-8") as f:
    #         return f.read()

    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    with open(template_path, "r", encoding="utf-8") as f:
        template_text = f.read()

    # -----------------------------------------------------
    # 1) Resolve language from ENV first, then fallback to config
    # -----------------------------------------------------
    # VN_LANGUAGE: "en", "ur", "ar", "cn", "cn_my", "bm"
    lang_env = (os.getenv("VN_LANGUAGE") or "").strip().lower()
    
    lang_map = {
        "en": "EN",
        "ur": "UR",
        "ar": "AR",
        "cn": "CN",       # Mandarin (Mainland)
        "cn_my": "CN_MY", # Mandarin (Malaysia)
        "bm": "BM"        # Bahasa Melayu (Malaysia)
    }

    if lang_env in lang_map:
        lang = lang_map[lang_env]
    else:
        # fallback to JSON default
        lang = (cfg.get("VN_LANGUAGE_MODE") or "EN").upper()

    cfg["VN_LANGUAGE_MODE"] = lang  # keep in cfg so template can see the actual language

    # VN_LANGUAGE_PACK: optional variant/region, e.g. "EN_VN_v1"
    pack_env = (os.getenv("VN_LANGUAGE_PACK") or "").strip()
    if pack_env:
        cfg["VN_REGION_CULTURE_PACK"] = pack_env

    # -----------------------------------------------------
    # 2) Choose culture pack file based on resolved language
    # -----------------------------------------------------
    culture_file_map = {
        "EN": "en_vn.txt",
        "UR": "ur_vn.txt",
        "AR": "ar_vn.txt",
        "CN": "cn_vn.txt",      # Simplified Chinese (Mainland)
        "CN_MY": "cn_my_vn.txt",# Simplified Chinese (Malaysia)
        "BM": "bm_my_vn.txt",   # Bahasa Melayu (Malaysia)
    }
    
    culture_file = culture_file_map.get(lang, "en_vn.txt")
    culture_path = os.path.join(culture_dir, culture_file)

    with open(culture_path, "r", encoding="utf-8") as f:
        culture_text = f.read()

    cfg["VN_CULTURE_PACK_CONTENT"] = culture_text

    # -----------------------------------------------------
    # 3) Fill placeholders in the VN template
    # -----------------------------------------------------
    final_prompt = template_text
    for key, value in cfg.items():
        placeholder = f"{{{{{key}}}}}"
        final_prompt = final_prompt.replace(placeholder, str(value))

    # OPTIONAL: if you still want a compiled prompt, you can write it here.
    # Just remember: it will be for whatever VN_LANGUAGE / VN_LANGUAGE_PACK
    # were active at startup. If you want to be safe, you can skip the cache.
    # with open(output_path, "w", encoding="utf-8") as f:
    #     f.write(final_prompt)

    return final_prompt