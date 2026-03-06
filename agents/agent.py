import os
from dotenv import load_dotenv
from typing import Optional
from agents.prompt.vn.prompt_creator import get_vn_prompt
from google.adk.agents import LlmAgent
from google.adk.agents.callback_context import CallbackContext
from google.genai import types
from google.genai import Client
from google.genai.types import HttpOptions
from agents.util import load_instruction_from_file
from agents.tools.templates import (
    greeting_template,
    order_draft_template,
    vn_order_draft_template,  
)
from agents.tools.order_draft_tools import (
    place_order_and_clear_draft,
    get_last_orders,
    _format_draft_for_reply,
    sendProductCatalogueTool,
    placeOrderTool,
    getLastOrdersTool,
    confirmOrderDraftTool,   
)
from agents.tools.promo_cart_tool import add_promo_items_to_cart
from agents.tools.api_tools import (
        search_products_by_sku, 
        search_customer_by_phone,
        semantic_product_search,
)
from agents.tools.knowledge_tool import retrieve_knowledge_base
from agents.tools.cart_tools import agentflo_cart_tool
from agents.runtime_config import load_agent_config
#from agents.tools.test_recommendation import smart_recommendation_template
#from agents.tools.promotion_template import promotions_tool
from agents.tools.sales_intelligence_engine import sales_intelligence_engine, send_order_pdf
# from agents.tools.product_info_csv_tool import product_info_csv_tool
from agents.prompt.prompt_creator import get_system_prompt
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

SYSTEM_INSTRUCTION = get_system_prompt(PROMPT_LANGUAGE, overrides=overrides)

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

engro_assistant_eleven = LlmAgent(
    name="EngroAssistantEleven",
    model="gemini-2.5-flash",
    instruction=SYSTEM_INSTRUCTION + "\n \n The user_id is: {user_id}",
    # instruction=SYSTEM_INSTRUCTION + "\n \n The user_id is: 923312167555",
    # instruction=SYSTEM_INSTRUCTION + "\n \n The user_id is: 923168242299",
    
    output_key="Engro_response",
    tools=[
        #Agent Tool Calls 
        _guard_tool(semantic_product_search),
        _guard_tool(search_products_by_sku),
        _guard_tool(agentflo_cart_tool),
        _guard_tool(placeOrderTool, name="placeOrderTool"),
        _guard_tool(getLastOrdersTool, name="getLastOrdersTool"),
        _guard_tool(retrieve_knowledge_base),
        # CSV catalog search is intentionally decoupled for now.
        # _guard_tool(product_info_csv_tool),
        _guard_tool(sales_intelligence_engine),
        _guard_tool(send_order_pdf),
        _guard_tool(confirmOrderDraftTool, name="confirmOrderDraftTool"),
        _guard_tool(sendProductCatalogueTool, name="send_product_catalogue"),

        # Templates as callable tools
        _guard_tool(greeting_template),  
        _guard_tool(order_draft_template),
        _guard_tool(vn_order_draft_template),

    ],
)

root_agent = engro_assistant_eleven
