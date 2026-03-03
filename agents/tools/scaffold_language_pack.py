import json
import os
import shutil
import sys

def scaffold_language_pack():
    # Define paths
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir)) # agents/tools -> agents -> root
    
    config_path = os.path.join(current_dir, "..", "prompt", "prompt_config.json")
    packs_dir = os.path.join(current_dir, "packs")
    
    # Default base pack to copy from
    base_pack_name = "PK_Retail_RomanUrdu_v1"
    base_pack_path = os.path.join(packs_dir, f"{base_pack_name}.py")

    # 1. Load Configuration
    if not os.path.exists(config_path):
        print(f"Error: Config file not found at {config_path}")
        return

    try:
        with open(config_path, "r", encoding="utf-8") as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error loading config: {e}")
        return

    target_pack_name = config.get("REGION_CULTURE_PACK_NAME")
    if not target_pack_name:
        print("Error: REGION_CULTURE_PACK_NAME not found in config.")
        return

    target_pack_path = os.path.join(packs_dir, f"{target_pack_name}.py")

    # 2. Check if target pack already exists
    if os.path.exists(target_pack_path):
        print(f"Language pack '{target_pack_name}' already exists at:")
        print(f"  {target_pack_path}")
        print("No action needed.")
        return

    # 3. Check if base pack exists
    if not os.path.exists(base_pack_path):
        print(f"Error: Base pack '{base_pack_name}' not found at {base_pack_path}.")
        print("Cannot scaffold new pack without a base.")
        return

    # 4. Copy base pack to target pack
    try:
        shutil.copy2(base_pack_path, target_pack_path)
        print(f"Successfully created new language pack: {target_pack_name}")
        print(f"Location: {target_pack_path}")
        print("-" * 40)
        print(f"TODO: Open {target_pack_name}.py and translate the strings to the target language.")
        print("-" * 40)
    except Exception as e:
        print(f"Error creating language pack: {e}")

if __name__ == "__main__":
    scaffold_language_pack()
