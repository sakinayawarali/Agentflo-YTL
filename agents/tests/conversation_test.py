# agents/tests/conversation_test.py

import sys
from pathlib import Path

# --- ensure project root on sys.path ---
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import os
import time
from types import SimpleNamespace

import pytest

from google.api_core.exceptions import AlreadyExists
from agents.helpers.adk_helper import ADKHelper


# ---------- Fake Firestore (in-memory) ----------

class FakeDoc:
    def __init__(self, store, collection_name: str, doc_id: str):
        """
        store: dict[str, dict[str, dict]]
          e.g. store["tenants"]["ebm"] = {...}
        """
        self._store = store
        self._collection = collection_name
        self._doc_id = doc_id

    def _bucket(self):
        return self._store.setdefault(self._collection, {})

    def get(self):
        bucket = self._bucket()
        data = bucket.get(self._doc_id)
        # Match Firestore-ish API: object with .exists and .to_dict()
        return SimpleNamespace(
            exists=data is not None,
            to_dict=lambda: data,
        )

    def set(self, data: dict, merge: bool = False):
        bucket = self._bucket()
        if merge and self._doc_id in bucket and isinstance(bucket[self._doc_id], dict):
            bucket[self._doc_id].update(dict(data))
        else:
            bucket[self._doc_id] = dict(data)

    def update(self, data: dict):
        bucket = self._bucket()
        existing = bucket.setdefault(self._doc_id, {})
        existing.update(dict(data))

    def delete(self):
        bucket = self._bucket()
        bucket.pop(self._doc_id, None)

    def create(self, data: dict):
        bucket = self._bucket()
        if self._doc_id in bucket:
            raise AlreadyExists("doc already exists")
        bucket[self._doc_id] = dict(data)


class FakeCollection:
    def __init__(self, store, name: str):
        self._store = store
        self._name = name

    def document(self, doc_id: str):
        return FakeDoc(self._store, self._name, doc_id)


class FakeFirestore:
    def __init__(self):
        # shape: {collection_name: {doc_id: {field: value}}}
        self._data = {}

    def collection(self, name: str):
        return FakeCollection(self._data, name)


# ---------- Fake SessionStore with conversation helpers ----------

class FakeSessionStore:
    """
    In-memory replacement for SessionStore for testing.

    Implements:
      - auth helpers
      - inactivity / end flags
      - text counters
      - summary helpers
      - catalogue helpers
      - conversation helpers:
          * get_or_start_conversation
          * touch_conversation
          * end_conversation
          * append_session_to_conversation
          * get_active_conversation (test-only)
    """

    def __init__(self):
        self._meta = {}          # per-user session meta
        self._conversations = {} # per-user conversation state
        self._conv_counter = 0

    # ----- time helpers -----
    def _now(self) -> float:
        return float(getattr(self, "_fake_now", time.time()))

    # ----- session meta helpers -----
    def touch(self, user_id: str, *, inbound_key=None, source="user"):
        meta = self._meta.setdefault(user_id, {})
        now = self._now()
        meta["last_inbound_key"] = inbound_key
        if source == "user":
            meta["last_user_at"] = now
        else:
            meta["last_agent_at"] = now

    def should_end_now(self, user_id: str, *, inactivity_sec: int):
        """
        Simplified: only looks at last_user_at and never auto-ends
        within tests unless explicitly manipulated.
        """
        meta = self._meta.get(user_id, {})
        last_user_at = meta.get("last_user_at")
        if not last_user_at:
            return False, None

        if inactivity_sec and (self._now() - last_user_at) >= inactivity_sec:
            return True, "inactivity"
        return False, None

    def end_now(self, user_id: str, *, reason: str = "manual"):
        meta = self._meta.setdefault(user_id, {})
        meta["ended_at"] = self._now()
        meta["last_end_reason"] = reason
        meta["pending_end_at"] = None
        meta["pending_reason"] = None

    def cancel_pending_end(self, user_id: str):
        meta = self._meta.setdefault(user_id, {})
        meta["pending_end_at"] = None
        meta["pending_reason"] = None

    # auth helpers
    def get_auth_status(self, user_id: str) -> bool:
        return bool(self._meta.get(user_id, {}).get("is_authenticated"))

    def set_auth_status(self, user_id: str, status: bool) -> None:
        meta = self._meta.setdefault(user_id, {})
        meta["is_authenticated"] = bool(status)

    # turn + text count
    def inc_turn(self, user_id: str) -> int:
        meta = self._meta.setdefault(user_id, {})
        meta["turn_count"] = int(meta.get("turn_count", 0)) + 1
        return meta["turn_count"]

    def reset_turns(self, user_id: str) -> None:
        meta = self._meta.setdefault(user_id, {})
        meta["turn_count"] = 0

    def inc_text_count(self, user_id: str, delta: int = 1) -> int:
        meta = self._meta.setdefault(user_id, {})
        meta["text_msg_count"] = int(meta.get("text_msg_count", 0)) + int(delta)
        return meta["text_msg_count"]

    def reset_text_count(self, user_id: str) -> None:
        meta = self._meta.setdefault(user_id, {})
        meta["text_msg_count"] = 0

    # summary helpers
    def get_summary(self, user_id: str) -> str:
        return self._meta.get(user_id, {}).get("running_summary", "") or ""

    def set_summary(self, user_id: str, summary: str) -> None:
        meta = self._meta.setdefault(user_id, {})
        meta["running_summary"] = (summary or "")[:4000]

    # catalogue helpers
    def get_last_catalog_session_id(self, user_id: str):
        return self._meta.get(user_id, {}).get("last_catalog_session_id")

    def get_last_catalog_sent_at(self, user_id: str) -> float:
        return float(self._meta.get(user_id, {}).get("last_catalog_sent_at") or 0.0)

    def mark_catalog_sent(self, user_id: str, session_id: str | None):
        meta = self._meta.setdefault(user_id, {})
        now = self._now()
        meta["last_catalog_sent_at"] = now
        if session_id:
            meta["last_catalog_session_id"] = session_id

    # ---------- conversation helpers ----------

    def get_or_start_conversation(self, user_id: str, *, conversation_inactivity_sec: int):
        """
        If there's an active conversation and USER inactivity < threshold, reuse it.
        Otherwise, start a new conversation.
        Returns: (conversation_id, is_new)
        """
        now = self._now()
        conv = self._conversations.get(user_id)

        if conv:
            # If ended explicitly → always new
            if conv.get("ended_at") is not None:
                conv = None
            else:
                last_user = conv.get("active_last_user_at")
                if last_user is not None:
                    if (now - last_user) <= conversation_inactivity_sec:
                        return conv["conversation_id"], False
                # If we reach here, inactivity exceeded
                conv = None

        if conv is None:
            self._conv_counter += 1
            conv_id = f"conv-{self._conv_counter}"
            conv = {
                "conversation_id": conv_id,
                "started_at": now,
                "ended_at": None,
                "last_end_reason": None,
                "active_last_user_at": None,
                "active_last_agent_at": None,
                "sessions": [],
            }
            self._conversations[user_id] = conv
            return conv_id, True

        # Fallback (shouldn't really get here)
        return conv["conversation_id"], False

    def touch_conversation(self, user_id: str, *, source: str = "user"):
        conv = self._conversations.get(user_id)
        if not conv:
            return
        now = self._now()
        if source == "user":
            conv["active_last_user_at"] = now
        else:
            conv["active_last_agent_at"] = now

    def end_conversation(self, user_id: str, reason: str = "manual"):
        conv = self._conversations.get(user_id)
        if not conv:
            return
        conv["ended_at"] = self._now()
        conv["last_end_reason"] = reason

    def append_session_to_conversation(self, user_id: str, session_id: str):
        conv = self._conversations.get(user_id)
        if not conv:
            return
        if session_id not in conv["sessions"]:
            conv["sessions"].append(session_id)

    # test-only convenience
    def get_active_conversation(self, user_id: str):
        return self._conversations.get(user_id)


# ---------- Fake session service ----------

class FakeSession:
    def __init__(self, session_id: str, state=None):
        self.id = session_id
        self.state = state or {}


class FakeSessionService:
    def __init__(self):
        self._counter = 0
        self._sessions = {}

    async def get_session(self, app_name: str, user_id: str, session_id: str | None):
        if not session_id:
            # fallback: create a default
            self._counter += 1
            sid = f"s-{self._counter}"
            sess = FakeSession(sid, state={"user_id": user_id})
            self._sessions[sid] = sess
            return sess
        if session_id in self._sessions:
            return self._sessions[session_id]
        sess = FakeSession(session_id, state={"user_id": user_id})
        self._sessions[session_id] = sess
        return sess

    async def create_session(self, app_name: str, user_id: str, state=None):
        self._counter += 1
        sid = f"s-{self._counter}"
        sess = FakeSession(sid, state=state or {"user_id": user_id})
        self._sessions[sid] = sess
        return sess


# ---------- Helper to build a fully stubbed ADKHelper ----------

def make_stubbed_helper():
    helper = ADKHelper()

    # Use fake Firestore instead of SimpleNamespace
    helper.db = FakeFirestore()

    # Replace session_helper with our fake one
    helper.session_helper = FakeSessionStore()

    # Replace session_service with fake one
    helper.session_service = FakeSessionService()

    # Auth mapping shortcuts
    helper.get_external_user_id = lambda wa_user_id: f"ext-{wa_user_id}"
    helper._bootstrap_user_from_api = lambda wa_user_id: f"ext-{wa_user_id}"

    # Session id persistence: just use the in-memory cache
    helper.get_current_session_id = lambda user_id: helper._session_cache.get(user_id)
    helper.update_session_id = lambda user_id, sid: helper._session_cache.__setitem__(user_id, sid) or True
    helper.create_user_document = lambda user_id, external_user_id=None: None

    # Don’t actually send messages to WhatsApp
    helper._send_text_once = lambda to, body, reply_to_message_id=None: True

    # Don’t send catalog in tests (no external side effects)
    async def _no_catalog(user_id: str, session_id: str | None):
        return
    helper._send_catalog_if_new_session = _no_catalog

    # Don’t call Runner / Vertex — just echo back deterministic response
    async def fake_call_agent_text_only(query: str, user_id: str, session_id: str) -> str:
        return f"agent_reply_to: {query}"

    helper._call_agent_text_only = fake_call_agent_text_only

    # Avoid real TTS calls by making VN path a no-op that just returns
    async def fake_gen_vn_else_text(to_number, text, inbound_key=None, reply_to_message_id=None):
        return

    helper._gen_vn_else_text = fake_gen_vn_else_text
    helper._spawn_vn_or_text = lambda *a, **k: None

    return helper


# ---------- Tests ----------

@pytest.mark.asyncio
async def test_conversation_reused_within_inactivity(monkeypatch):
    # Make inactivity large so second message reuses conversation
    os.environ["CONVERSATION_INACTIVITY_SEC"] = str(24 * 60 * 60)

    helper = make_stubbed_helper()
    user_id = "923001234567"

    # First inbound
    resp1 = await helper.handle_message_async(
        "Hello",
        user_id,
        is_voice_input=False,
        inbound_key="wamid-1",
        reply_to_message_id=None,
    )
    # Our fake agent always wraps query with agent_reply_to:
    assert "agent_reply_to:" in resp1

    conv1 = helper.session_helper.get_active_conversation(user_id)
    assert conv1 is not None
    conv_id_1 = conv1["conversation_id"]
    sessions_after_first = list(conv1["sessions"])
    assert len(sessions_after_first) == 1

    # Second inbound shortly after → same conversation id
    resp2 = await helper.handle_message_async(
        "How are you?",
        user_id,
        is_voice_input=False,
        inbound_key="wamid-2",
        reply_to_message_id=None,
    )
    assert "agent_reply_to:" in resp2

    conv2 = helper.session_helper.get_active_conversation(user_id)
    assert conv2 is not None
    conv_id_2 = conv2["conversation_id"]

    # conversation id should remain the same
    assert conv_id_2 == conv_id_1

    # but we may have more sessions recorded if a rotation happened in between
    assert len(conv2["sessions"]) >= 1


@pytest.mark.asyncio
async def test_conversation_ends_on_goodbye_and_new_conversation_starts():
    os.environ["CONVERSATION_INACTIVITY_SEC"] = str(24 * 60 * 60)

    helper = make_stubbed_helper()
    user_id = "923001234568"

    # First inbound
    _ = await helper.handle_message_async(
        "Hi",
        user_id,
        is_voice_input=False,
        inbound_key="wamid-a1",
        reply_to_message_id=None,
    )
    conv1 = helper.session_helper.get_active_conversation(user_id)
    assert conv1 is not None
    conv_id_1 = conv1["conversation_id"]

    # Goodbye message
    _ = await helper.handle_message_async(
        "thanks, khuda hafiz",    # matches GOODBYE_RE
        user_id,
        is_voice_input=False,
        inbound_key="wamid-a2",
        reply_to_message_id=None,
    )

    # After goodbye, conversation should be marked ended
    conv_after_goodbye = helper.session_helper.get_active_conversation(user_id)
    assert conv_after_goodbye is not None
    assert conv_after_goodbye["ended_at"] is not None
    assert conv_after_goodbye["last_end_reason"] == "goodbye"

    # Next inbound should start a NEW conversation id
    _ = await helper.handle_message_async(
        "I'm back",
        user_id,
        is_voice_input=False,
        inbound_key="wamid-a3",
        reply_to_message_id=None,
    )

    conv2 = helper.session_helper.get_active_conversation(user_id)
    assert conv2 is not None
    conv_id_2 = conv2["conversation_id"]

    assert conv_id_2 != conv_id_1  # new conversation
    assert conv2["ended_at"] is None  # active again


@pytest.mark.asyncio
async def test_conversation_rotates_on_inactivity_threshold():
    # Very small inactivity for test (e.g., 1 second)
    os.environ["CONVERSATION_INACTIVITY_SEC"] = "1"

    helper = make_stubbed_helper()
    user_id = "923001234569"

    store: FakeSessionStore = helper.session_helper  # type: ignore

    # Control fake time
    base = time.time()
    store._fake_now = base

    # First inbound
    _ = await helper.handle_message_async(
        "Start",
        user_id,
        is_voice_input=False,
        inbound_key="wamid-b1",
        reply_to_message_id=None,
    )
    conv1 = store.get_active_conversation(user_id)
    assert conv1 is not None
    conv_id_1 = conv1["conversation_id"]
    assert conv1["active_last_user_at"] == base

    # Advance fake time beyond threshold
    store._fake_now = base + 5  # > 1 second

    # Next inbound should create new conversation because of inactivity
    _ = await helper.handle_message_async(
        "New message after long gap",
        user_id,
        is_voice_input=False,
        inbound_key="wamid-b2",
        reply_to_message_id=None,
    )

    conv2 = store.get_active_conversation(user_id)
    assert conv2 is not None
    conv_id_2 = conv2["conversation_id"]

    assert conv_id_2 != conv_id_1
    assert conv2["ended_at"] is None
    assert conv2["active_last_user_at"] == store._fake_now
