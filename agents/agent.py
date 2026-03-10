import os
from dotenv import load_dotenv
from typing import Optional
from agents.prompt.vn.prompt_creator import get_vn_prompt
from google.adk.agents import LlmAgent
from google.adk.agents.callback_context import CallbackContext

# Optional: local file-based knowledge injection (ADK memory)
try:
    from google.adk.memory import FileMemory  # type: ignore[import-untyped]
except Exception:
    FileMemory = None  # type: ignore[assignment]
from google.genai import types
from google.genai import Client
from google.genai.types import HttpOptions
from agents.util import load_instruction_from_file
from agents.tools.concrete_calc_tools import calculate_concrete_volume, calculate_trucks_needed
from agents.tools.concrete_specs_tools import get_concrete_technical_properties
from agents.tools.pricing_tools import estimate_concrete_price, generate_quote
from agents.tools.demo_concrete_tools import (
    recommend_concrete_grade,
    estimate_pump_needed,
    nearest_batching_plant,
    delivery_eta,
    recommend_pump,
)
from agents.runtime_config import load_agent_config
from utils.logging import logger
from agents.guardrails.adk_guardrails import (
    before_agent_guard,
    after_agent_guard,
    set_callback_context,
    clear_callback_context,
    wrap_tool,
)

load_dotenv()

_GENAI_API_KEY = (
    os.getenv("GEMINI_API_KEY")
    or os.getenv("GOOGLE_API_KEY")
    or os.getenv("GENAI_API_KEY")
)
_http_opts = HttpOptions(
    baseUrl="https://generativelanguage.googleapis.com",
    apiVersion="v1",
)
genai_client = Client(api_key=_GENAI_API_KEY, http_options=_http_opts, vertexai=False) if _GENAI_API_KEY else Client(http_options=_http_opts, vertexai=False)

TENANT_ID = os.getenv("TENANT_ID", "").strip().lower()

# For the YTL Cement demo we do not expose any legacy EBM tenant mapping.
TENANT_TO_BUSINESS: dict[str, str] = {}

def _get_dict(d, k):
    v = d.get(k)
    return v if isinstance(v, dict) else {}

def _get_list(d, k):
    v = d.get(k)
    return v if isinstance(v, list) else []

def _pick_str(*vals, default=""):
    for v in vals:
        if isinstance(v, str) and v.strip():
            return v.strip()
    return default

def _resolve_lang_code(cfg: dict) -> str:
    # Prefer env override if you explicitly set PROMPT_LANGUAGE
    env_lang = os.getenv("PROMPT_LANGUAGE")
    if isinstance(env_lang, str) and env_lang.strip():
        return env_lang.strip().upper()

    conv = _get_dict(cfg, "conversation")
    out = _pick_str(conv.get("outputLanguage"), default="English").lower()
    lp = _pick_str(conv.get("languagePack")).upper()

    # languagePack can be used too (expand later)
    if out in ("ur", "urdu", "roman urdu") or lp.startswith("UR_"):
        return "UR"
    if out in ("ar", "arabic") or lp.startswith("AR_"):
        return "AR"
    return "EN"

def _resolve_pack_name(cfg: dict, lang_code: str) -> str:
    if lang_code == "UR":
        return "PK_Retail_RomanUrdu_v1"
    if lang_code == "AR":
        return "SA_Retail_Arabic_v1"
    if lang_code == "EN":
        return "EN_Retail_Standard_v1"
    return "EN_Retail_Standard_v1"

def _primary_language_label(lang_code: str) -> str:
    return {"UR": "Roman Urdu", "AR": "Arabic", "EN": "English"}.get(lang_code, "English")

# Load portal agent config from Firestore (saved by orchestrator)
try:
    AGENT_CFG = load_agent_config()
    logger.info("Loaded AGENT_CFG from Firestore", tenant_id=TENANT_ID, config_id=os.getenv("CONFIG_ID"))
except Exception as e:
    # Don't crash the service; fallback to current behavior
    AGENT_CFG = {}
    logger.error("Failed to load AGENT_CFG from Firestore; using defaults", error=str(e))

# YTL Cement: English-only agent (text + VN). Always use English.
PROMPT_LANGUAGE = "EN"
# Ensure downstream template + string dispatchers also see English.
os.environ["PROMPT_LANGUAGE"] = "EN"

business_context = _get_dict(AGENT_CFG, "businessContext")
business_name = _pick_str(
    os.getenv("BUSINESS_NAME"),
    business_context.get("businessName"),
    business_context.get("business_name"),
    AGENT_CFG.get("businessName"),
    AGENT_CFG.get("business_name"),
    TENANT_TO_BUSINESS.get(TENANT_ID),
    default="Your Business",
)
if business_name and business_name != os.getenv("BUSINESS_NAME"):
    os.environ["BUSINESS_NAME"] = business_name

channels = _get_list(AGENT_CFG, "channels")
primary_channel = channels[0] if channels else "WhatsApp"

pack_name = _resolve_pack_name(AGENT_CFG, PROMPT_LANGUAGE)

# Make templates dispatcher (templates.py) pick correct language pack too
os.environ["REGION_CULTURE_PACK_NAME"] = pack_name

overrides = {
    "AGENT_NAME": "Ayesha",  # ← Force this to always be Ayesha
    "BUSINESS_NAME": business_name,
    "PRIMARY_CHANNEL": primary_channel,
    "PRIMARY_LANGUAGE": _primary_language_label(PROMPT_LANGUAGE),
    "REGION_CULTURE_PACK_NAME": pack_name,
}

SYSTEM_INSTRUCTION = (
    "You are Ayesha, a YTL Cement Malaysia representative for ready-mix concrete.\n"
    "You ONLY handle YTL Cement Malaysia concrete enquiries (no biscuits/retail).\n\n"
    "Important definitions:\n"
    "- A bare number like \"20\" can mean either a grade (G20) or a volume (20 m³). Use the most recent question to interpret it.\n"
    "- If you just asked \"How many m³?\" then a bare number is volume in m³. Do NOT ask to clarify.\n\n"
    "Core tasks:\n"
    "- recommend the correct concrete grade\n"
    "- calculate volume (m³), trucks required (8 m³ capacity), and price estimates\n"
    "- generate a structured quote and help schedule delivery\n"
    "- answer concrete technical/delivery questions using the provided knowledge base\n\n"
    "Conversation memory and active order:\n"
    "- Maintain an ACTIVE ORDER snapshot for the user (grade, volume, delivery date/time window, pump requirement, site location) across the whole conversation.\n"
    "- When the user comes back to delivery/quotation after other questions, reuse the ACTIVE ORDER details instead of starting from scratch. Only ask for missing fields.\n"
    "- Also remember other stable facts they share (e.g., project type, green-building goals, budget constraints) and reuse them when relevant.\n\n"
    "Rules:\n"
    "- When user asks for available grades: list G15, G20, G25, G30, G35, G40, G45.\n"
    "- Do NOT invent or list product/mix names unless explicitly present in the knowledge files.\n"
    "- For technical specs questions (slump, aggregate size, setting time, max delivery time), use get_concrete_technical_properties.\n"
    "- When user mentions sustainability/green building/certifications, proactively recommend YTL ECO range (ECOConcrete for ready-mix; ECOCem for cement) using the approved phrasing in upselling_rules.md.\n"
    "- Do not guess policies or specs not present in the knowledge files. If not specified, say so and offer to confirm.\n"
    "- Keep answers concise: usually 2–4 short sentences, or a short bullet list.\n"
    "- Always be clear and professional for contractors and engineers.\n"
    "- For delivery feasibility, request a WhatsApp location pin and use nearest-plant-only + delivery radius logic.\n"
)

# Optional: add YTL sales behavior prompt from knowledge/
try:
    _sales_prompt_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "knowledge", "sales_agent_prompt.txt")
    )
    if os.path.exists(_sales_prompt_path):
        with open(_sales_prompt_path, "r", encoding="utf-8") as f:
            _sales_prompt_text = f.read().strip()
        if _sales_prompt_text:
            SYSTEM_INSTRUCTION = SYSTEM_INSTRUCTION + "\n\n" + _sales_prompt_text
except Exception as e:
    logger.error("Failed to load sales_agent_prompt.txt", error=str(e))

# Optional: add deterministic upselling rules from knowledge/
try:
    _upselling_rules_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "knowledge", "upselling_rules.md")
    )
    if os.path.exists(_upselling_rules_path):
        with open(_upselling_rules_path, "r", encoding="utf-8") as f:
            _upselling_rules_text = f.read().strip()
        if _upselling_rules_text:
            SYSTEM_INSTRUCTION = SYSTEM_INSTRUCTION + "\n\n" + _upselling_rules_text
except Exception as e:
    logger.error("Failed to load upselling_rules.md", error=str(e))

# Optional: add FAQ policy/checklist from knowledge/
try:
    _faq_policy_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "knowledge", "faq_answering_policy.md")
    )
    if os.path.exists(_faq_policy_path):
        with open(_faq_policy_path, "r", encoding="utf-8") as f:
            _faq_policy_text = f.read().strip()
        if _faq_policy_text:
            SYSTEM_INSTRUCTION = SYSTEM_INSTRUCTION + "\n\n" + _faq_policy_text
except Exception as e:
    logger.error("Failed to load faq_answering_policy.md", error=str(e))

# These settings are used by direct agent calls (/tasks/tts-send) or when
# USE_AGENT_TTS_CALLBACKS=true. The webhook path has its own VN logic.
VOICE_NOTE_MODE = os.getenv("VOICE_NOTE_MODE", "summary").lower()
ELEVEN_VOICE_ID = os.getenv("ELEVEN_VOICE_ID")
ELEVEN_MODEL_ID = os.getenv("ELEVEN_MODEL_ID", "eleven_multilingual_v2")

# Flag to enable TTS callbacks on the agent path (webhook has its own async VN)
USE_AGENT_TTS_CALLBACKS = os.getenv("USE_AGENT_TTS_CALLBACKS", "false").lower() == "true"

# ---------------------------
# Agent configuration
# ---------------------------
def _guard_tool(tool, name: Optional[str] = None):
    return wrap_tool(tool, tool_name=name, return_raw=True)

ytl_cement_sales_agent = LlmAgent(
    name="YTLCementSalesAgent",
    model="gemini-2.5-flash",
    instruction=SYSTEM_INSTRUCTION + "\n \n The user_id is: {user_id}",
    # instruction=SYSTEM_INSTRUCTION + "\n \n The user_id is: 923312167555",
    # instruction=SYSTEM_INSTRUCTION + "\n \n The user_id is: 923168242299",
    
    output_key="YTL_response",
    tools=[
        # Concrete tools (deterministic)
        _guard_tool(calculate_concrete_volume),
        _guard_tool(calculate_trucks_needed),
        _guard_tool(get_concrete_technical_properties),
        _guard_tool(estimate_concrete_price),
        _guard_tool(generate_quote),
        _guard_tool(recommend_concrete_grade),
        _guard_tool(estimate_pump_needed),
        _guard_tool(recommend_pump),
        _guard_tool(nearest_batching_plant),
        _guard_tool(delivery_eta),
    ],
)

# Attach file-based knowledge memory (if available)
if FileMemory is not None and os.getenv("USE_FILE_MEMORY", "true").lower() in ("1", "true", "yes", "y"):
    _knowledge_dir = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "knowledge")
    )
    _knowledge_paths = [
        os.path.join(_knowledge_dir, "concrete_products.md"),
        os.path.join(_knowledge_dir, "concrete_pricing.md"),
        os.path.join(_knowledge_dir, "delivery_operations.md"),
        os.path.join(_knowledge_dir, "sustainability_products.md"),
        os.path.join(_knowledge_dir, "construction_advice.md"),
        os.path.join(_knowledge_dir, "operations_demo.json"),
        os.path.join(_knowledge_dir, "concrete_tools.md"),
        os.path.join(_knowledge_dir, "upselling_rules.md"),
        os.path.join(_knowledge_dir, "grade_strength_price_table.md"),
        os.path.join(_knowledge_dir, "customer_faq_intents.md"),
        os.path.join(_knowledge_dir, "faq_answering_policy.md"),
    ]
    try:
        ytl_cement_sales_agent.memory = FileMemory(paths=_knowledge_paths)
    except Exception as e:
        logger.error("Failed to attach FileMemory knowledge", error=str(e), knowledge_paths=_knowledge_paths)

root_agent = ytl_cement_sales_agent
