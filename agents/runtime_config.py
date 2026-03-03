import os
from typing import Any, Dict
from google.cloud import firestore
from dotenv import load_dotenv
from agents.helpers.firestore_utils import get_agent_id

load_dotenv()

PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT") 
TENANT_ID = os.getenv("TENANT_ID", "")
AGENT_ID = os.getenv("AGENT_ID", "")
CONFIG_ID = os.getenv("CONFIG_ID", "")

_db = firestore.Client(project=PROJECT_ID) if PROJECT_ID else firestore.Client()

def load_agent_config() -> Dict[str, Any]:
    """
    Reads:
      tenants/{TENANT_ID}/agent_id/{AGENT_ID}/agentConfigs/{CONFIG_ID}
    Returns doc["config"] (portal JSON stored by orchestrator).
    """
    agent_id = (AGENT_ID or "").strip() or get_agent_id()
    if not TENANT_ID or not CONFIG_ID or not agent_id:
        raise RuntimeError("Missing TENANT_ID, CONFIG_ID, or AGENT_ID env vars in agent service")

    ref = (
        _db.collection("tenants")
           .document(TENANT_ID)
           .collection("agentConfigs")
           .document(CONFIG_ID)
    )
    snap = ref.get()
    if not snap.exists:
        raise RuntimeError(f"Config not found: tenants/{TENANT_ID}/agentConfigs/{CONFIG_ID}")

    doc = snap.to_dict() or {}
    cfg = doc.get("config") or {}
    return cfg if isinstance(cfg, dict) else {}
