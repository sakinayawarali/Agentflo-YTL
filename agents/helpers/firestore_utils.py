import os
from typing import Optional

from google.cloud import firestore


def get_tenant_id(default: str = "ebm") -> str:
    """
    Return the tenant id to use for Firestore paths.
    Falls back to `default` when TENANT_ID is unset/empty.
    """
    tenant = (os.getenv("TENANT_ID") or "").strip()
    return tenant or default


def get_agent_id() -> str:
    """
    Return the agent_id to use for Firestore paths.
    Defaults to "ytl-demo" when AGENT_ID is unset (YTL demo, no EBM config required).
    """
    agent_id = (os.getenv("AGENT_ID") or "").strip()
    if not agent_id:
        return "ytl-demo"
    return agent_id


def user_root(
    db: firestore.Client,
    user_id: str,
    *,
    tenant_id: Optional[str] = None,
    agent_id: Optional[str] = None,
) -> firestore.DocumentReference:
    """
    Consistent base path for all per-user documents:
    tenants/{tenant_id}/agent_id/{agent_id}/users/{user_id}
    """
    tid = (tenant_id or get_tenant_id()).strip()
    aid = (agent_id or get_agent_id()).strip()
    return (
        db.collection("tenants")
        .document(tid)
        .collection("agent_id")
        .document(aid)
        .collection("users")
        .document(str(user_id))
    )
