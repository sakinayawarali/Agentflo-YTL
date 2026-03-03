from __future__ import annotations
import os
import time
from typing import Optional, Tuple, Literal

from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists
from datetime import datetime, timezone
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:
    ZoneInfo = None

from agents.helpers.firestore_utils import get_tenant_id, user_root

EventSource = Literal["user", "agent"]


class SessionStore:
    def __init__(self) -> None:
        self.db = firestore.Client()
        self.tenant_id = get_tenant_id()
        self.collection = "sessions_meta"
        self.conv_collection = "conversations"
        # Optional toggle to ignore inactivity endings (useful in dev)
        self.inactivity_enabled = os.getenv("SESSION_INACTIVITY_ENABLED", "true").lower() == "true"
        # Lock TTLs (mirrors env vars used in ADKHelper)
        try:
            self.message_lock_ttl_sec = int(os.getenv("MESSAGE_LOCK_TTL_SECONDS", "45"))
        except Exception:
            self.message_lock_ttl_sec = 45
        try:
            self.message_lock_wait_sec = float(os.getenv("MESSAGE_LOCK_WAIT_SECONDS", "6"))
        except Exception:
            self.message_lock_wait_sec = 6.0
        try:
            self.message_lock_poll_sec = float(os.getenv("MESSAGE_LOCK_POLL_SECONDS", "0.25"))
        except Exception:
            self.message_lock_poll_sec = 0.25

    # ------------ internal meta refs ------------

    def _user_root(self, user_id: str):
        return user_root(self.db, user_id, tenant_id=self.tenant_id)

    def _ref(self, user_id: str):
        return self._user_root(user_id).collection(self.collection).document("meta")

    def _safe_get(self, user_id: str) -> dict:
        try:
            snap = self._ref(user_id).get()
            return snap.to_dict() or {}
        except Exception:
            return {}

    def _merge(self, user_id: str, data: dict) -> None:
        try:
            self._ref(user_id).set(data, merge=True)
        except Exception:
            # best-effort; swallow errors (doesn't block request path)
            pass

    # ------------ internal conversation refs ------------

    def _conv_ref(self, user_id: str):
        return self._user_root(user_id).collection(self.conv_collection).document("meta")

    def _safe_get_conv(self, user_id: str) -> dict:
        try:
            snap = self._conv_ref(user_id).get()
            return snap.to_dict() or {}
        except Exception:
            return {}

    def _conv_merge(self, user_id: str, data: dict) -> None:
        try:
            self._conv_ref(user_id).set(data, merge=True)
        except Exception:
            # best-effort
            pass

    # ------------ time helpers ------------

    def _now(self) -> float:
        return float(time.time())

    def _fmt_pk_local(self, ts: float) -> dict:
        """
        Returns {"local": "YYYY-MM-DD HH:MM:SS PKT", "date": "YYYY-MM-DD"} for Asia/Karachi.
        Falls back to UTC strings if zoneinfo isn't available.
        """
        try:
            ts = float(ts)
            PK_TZ = ZoneInfo("Asia/Karachi") if ZoneInfo else None
            if PK_TZ:
                dt = datetime.fromtimestamp(ts, PK_TZ)
                return {"local": dt.strftime("%Y-%m-%d %H:%M:%S %Z"), "date": dt.strftime("%Y-%m-%d")}
            dt = datetime.fromtimestamp(ts, timezone.utc)
            return {"local": dt.strftime("%Y-%m-%d %H:%M:%S UTC"), "date": dt.strftime("%Y-%m-%d")}
        except Exception:
            return {}

    # ------------ conversation helpers (tenants/{tenant}/agent_id/{agent_id}/users/{user_id}/conversations/meta) ------------

    def get_active_conversation_id(self, user_id: str) -> Optional[str]:
        """
        Return the active conversation_id for this user, or None if none exists.
        """
        doc = self._safe_get_conv(user_id)
        cid = doc.get("active_conversation_id")
        return str(cid) if cid else None

    def _start_new_conversation(self, user_id: str, *, now: Optional[float] = None) -> str:
        """
        Internal helper to create a new active conversation for this user.
        """
        if now is None:
            now = self._now()

        conv_id = f"{user_id}-{int(now)}"

        payload = {
            "active_conversation_id": conv_id,
            "active_started_at": now,
            "active_last_user_at": None,
            "active_last_agent_at": None,
            "active_ended_at": None,
            "active_end_reason": None,
            "active_session_ids": [],
        }
        loc = self._fmt_pk_local(now)
        if loc:
            payload["active_started_at_local"] = loc["local"]
            payload["active_started_at_date"] = loc["date"]

        self._conv_merge(user_id, payload)
        return conv_id

    def get_or_start_conversation(
        self,
        user_id: str,
        *,
        conversation_inactivity_sec: int,
    ) -> tuple[str, bool]:
        """
        Ensure there is an active conversation for this user.

        Returns:
            (conversation_id, is_new)
        where is_new=True if a NEW conversation was created as part of this call.
        """
        now = self._now()
        doc = self._safe_get_conv(user_id)
        cid = doc.get("active_conversation_id")
        ended_at = doc.get("active_ended_at")

        # No conversation yet -> create one
        if not cid:
            conv_id = self._start_new_conversation(user_id, now=now)
            return conv_id, True

        # Has explicit end? -> archive & start new
        conv_already_ended = isinstance(ended_at, (int, float)) and float(ended_at) > 0

        # Conversation-level inactivity check (based on last user activity in that conversation)
        inactive = False
        if conversation_inactivity_sec and conversation_inactivity_sec > 0:
            last_user_at = doc.get("active_last_user_at")
            if isinstance(last_user_at, (int, float)):
                silence = now - float(last_user_at)
                if silence >= float(conversation_inactivity_sec):
                    inactive = True

        if conv_already_ended or inactive:
            # Archive active -> last_*
            archive_reason = doc.get("active_end_reason") or ("inactivity" if inactive else "ended")
            archive_payload = {
                "last_conversation_id": cid,
                "last_started_at": doc.get("active_started_at"),
                "last_ended_at": ended_at or now,
                "last_end_reason": archive_reason,
            }
            loc = self._fmt_pk_local(ended_at or now)
            if loc:
                archive_payload["last_ended_at_local"] = loc["local"]
                archive_payload["last_ended_at_date"] = loc["date"]

            self._conv_merge(user_id, archive_payload)
            conv_id = self._start_new_conversation(user_id, now=now)
            return conv_id, True

        # Reuse existing active conversation
        return str(cid), False

    def end_conversation(self, user_id: str, *, reason: str = "manual") -> None:
        """
        Mark the active conversation for this user as ended with a reason.
        """
        now = self._now()
        doc = self._safe_get_conv(user_id)
        cid = doc.get("active_conversation_id")
        if not cid:
            return

        payload = {
            "active_ended_at": now,
            "active_end_reason": reason,
        }
        loc = self._fmt_pk_local(now)
        if loc:
            payload["active_ended_at_local"] = loc["local"]
            payload["active_ended_at_date"] = loc["date"]

        archive = {
            "last_conversation_id": cid,
            "last_started_at": doc.get("active_started_at"),
            "last_ended_at": now,
            "last_end_reason": reason,
        }
        if loc:
            archive["last_ended_at_local"] = loc["local"]
            archive["last_ended_at_date"] = loc["date"]

        payload.update(archive)
        self._conv_merge(user_id, payload)

    def touch_conversation(self, user_id: str, *, source: EventSource = "user") -> None:
        """
        Record activity INSIDE the active conversation:
        - source="user"  -> active_last_user_at
        - source="agent" -> active_last_agent_at
        """
        now = self._now()
        doc = self._safe_get_conv(user_id)
        if not doc.get("active_conversation_id"):
            return

        payload: dict = {}
        if source == "user":
            payload["active_last_user_at"] = now
            loc = self._fmt_pk_local(now)
            if loc:
                payload["active_last_user_at_local"] = loc["local"]
                payload["active_last_user_at_date"] = loc["date"]
        else:
            payload["active_last_agent_at"] = now
            loc = self._fmt_pk_local(now)
            if loc:
                payload["active_last_agent_at_local"] = loc["local"]
                payload["active_last_agent_at_date"] = loc["date"]

        self._conv_merge(user_id, payload)

    def append_session_to_conversation(self, user_id: str, session_id: Optional[str]) -> None:
        """
        Record that a given Vertex session_id participated in the active conversation.
        """
        if not session_id:
            return
        doc = self._safe_get_conv(user_id)
        if not doc.get("active_conversation_id"):
            return

        sessions = doc.get("active_session_ids") or []
        if session_id in sessions:
            return
        sessions.append(session_id)
        self._conv_merge(user_id, {"active_session_ids": sessions})

    # Optional debug helper
    def get_active_conversation(self, user_id: str) -> dict:
        return self._safe_get_conv(user_id)
    
    # ------------ public API ------------
    def touch(self, user_id: str, *, inbound_key: Optional[str] = None, source: EventSource = "user") -> None:
        """
        Record activity for this user. Typically call on:
        - actual inbound message (source="user")
        - after sending assistant messages (source="agent")
        """
        now = self._now()
        payload = {"last_inbound_key": inbound_key}
        if source == "user":
            payload["last_user_at"] = now
            loc = self._fmt_pk_local(now)
            if loc:
                payload["last_user_at_local"] = loc["local"]
                payload["last_user_at_date"] = loc["date"]
        else:
            payload["last_agent_at"] = now
            loc = self._fmt_pk_local(now)
            if loc:
                payload["last_agent_at_local"] = loc["local"]
                payload["last_agent_at_date"] = loc["date"]
        # touching implies conversation is alive; do not auto-clear pending end here
        # (cancellation is explicit via cancel_pending_end())
        self._merge(user_id, payload)

    def request_end(self, user_id: str, *, delay_sec: int, reason: str = "scheduled", combine: str = "overwrite") -> None:
        """
        Ask to end the session after delay_sec (e.g., 900 for 15 min) unless canceled.
        `combine` controls how a new timer interacts with an existing one:
        - "overwrite": just set to now + delay_sec
        - "min":       tighten deadline to the earlier of (existing, new)
        - "max":       extend deadline to the later of (existing, new)
        """
        now = self._now()
        new_ts = now + max(0, int(delay_sec))
        if combine not in ("overwrite", "min", "max"):
            combine = "overwrite"

        doc = self._safe_get(user_id)
        cur = doc.get("pending_end_at")

        if isinstance(cur, (int, float)) and cur > 0:
            if combine == "min":
                new_ts = min(float(cur), new_ts)
            elif combine == "max":
                new_ts = max(float(cur), new_ts)

        payload = {
            "pending_end_at": new_ts,
            "pending_reason": reason,
        }
        loc = self._fmt_pk_local(new_ts)
        if loc:
            payload["pending_end_at_local"] = loc["local"]
            payload["pending_end_at_date"] = loc["date"]
        self._merge(user_id, payload)


    def cancel_pending_end(self, user_id: str) -> None:
        """
        If the user returns before a pending end fires, clear it.
        Typically run on any real inbound.
        """
        self._merge(user_id, {
            "pending_end_at": None,
            "pending_reason": None,
        })

    def should_end_now(
    self,
    user_id: str,
    *,
    inactivity_sec: int
) -> Tuple[bool, Optional[str]]:
        """
        Lazy enforcement probe. Call this at the start of a request:
        - If a pending end time has passed, return (True, "pending_end").
        - Else if USER inactivity threshold exceeded (and enabled), return (True, "inactivity").
        - Otherwise, return (False, None).
        
        IMPORTANT: For inactivity check, we ONLY look at last_user_at (user silence),
        NOT last_agent_at. Agent messages shouldn't reset the inactivity timer.
        """
        doc = self._safe_get(user_id)
        now = self._now()

        # 1) Pending end has matured? (e.g., 15 min after invoice)
        pending_at = doc.get("pending_end_at")
        if isinstance(pending_at, (int, float)) and pending_at > 0 and now >= float(pending_at):
            return True, (doc.get("pending_reason") or "pending_end")

        # 2) User inactivity check (7 hours of USER silence)
        if self.inactivity_enabled and inactivity_sec and inactivity_sec > 0:
            last_user_at = doc.get("last_user_at")
            
            # Only check user activity, not agent activity
            if isinstance(last_user_at, (int, float)):
                silence_duration = now - float(last_user_at)
                if silence_duration >= float(inactivity_sec):
                    return True, "inactivity"

        return False, None

    def end_now(self, user_id: str, *, reason: str = "manual") -> None:
        """
        Mark session ended immediately. Does not rotate Vertex session itself —
        the caller (adk_helper) handles rotation. We record audit fields so
        future probes start from a clean slate.
        """
        now = self._now()
        payload = {
            "ended_at": now,
            "last_end_reason": reason,
            # clear pending end so we don't double-trigger
            "pending_end_at": None,
            "pending_reason": None,
        }
        loc = self._fmt_pk_local(now)
        if loc:
            payload["ended_at_local"] = loc["local"]
            payload["ended_at_date"] = loc["date"]
        self._merge(user_id, payload)

    def end_session(self, user_id: str, *, reason: str) -> None:
        """
        Unified session termination: marks the session ended AND closes the active
        conversation in one call.

        Use this for all three end triggers:
          - "goodbye"          (user said bye)
          - "invoice_generated" (invoice sent to user)
          - "inactivity"       (12-hour gap / pending-end timer matured)

        The caller (adk_helper) still needs to call save_and_create_new_session()
        to rotate the Vertex session itself.
        """
        self.end_now(user_id, reason=reason)
        self.end_conversation(user_id, reason=reason)

    def should_start_new_session(
        self,
        user_id: str,
        *,
        inactivity_sec: int,
    ) -> Tuple[bool, Optional[str]]:
        """
        Single probe: should we rotate to a new Vertex session before processing
        this message?

        Returns (True, reason) when:
          - A pending-end timer has matured (e.g. 15 min after invoice was sent), OR
          - The user has been silent for >= inactivity_sec (default env: 12 h).

        Returns (False, None) otherwise.

        Delegates to should_end_now so the inactivity toggle
        (SESSION_INACTIVITY_ENABLED) is still honoured.
        """
        return self.should_end_now(user_id, inactivity_sec=inactivity_sec)

    def on_incoming_message(self, user_id: str, *, inbound_key: Optional[str] = None) -> None:
        """
        Record that a real user message arrived.  Call exactly once per inbound,
        before the agent is invoked.

        - Updates last_user_at (resets the inactivity clock).
        - Cancels any pending-end timer (user is still active).

        Replaces the pattern of calling touch() + cancel_pending_end() separately.
        """
        self.touch(user_id, inbound_key=inbound_key, source="user")
        self.cancel_pending_end(user_id)
    
    # --- session_id persistence ---

    def get_current_session_id(self, user_id: str) -> Optional[str]:
        """Read the stored Vertex session_id for this user from Firestore."""
        try:
            doc = self._user_root(user_id).get()
            if doc.exists:
                return (doc.to_dict() or {}).get("session_id")
            return None
        except Exception:
            return None

    def update_session_id(self, user_id: str, new_session_id: str) -> bool:
        """Persist a new Vertex session_id for this user to Firestore."""
        try:
            self._user_root(user_id).update({"session_id": new_session_id})
            return True
        except Exception:
            return False

    # --- message locking (per-user singleflight) ---

    def release_message_lock(self, user_id: str) -> None:
        """Release the per-user message processing lock."""
        try:
            self._user_root(user_id).collection("message_locks").document("active").delete()
        except Exception:
            pass

    def _acquire_message_lock(self, user_id: str, *, ttl_sec: Optional[int] = None) -> bool:
        ref = self._user_root(user_id).collection("message_locks").document("active")
        now = time.time()
        ttl = ttl_sec or self.message_lock_ttl_sec
        try:
            ref.create({"locked_at": now, "expires": now + ttl})
            return True
        except AlreadyExists:
            try:
                doc = ref.get()
                data = doc.to_dict() or {}
                if data.get("expires", 0) < now:
                    ref.set({"locked_at": now, "expires": now + ttl}, merge=True)
                    return True
                return False
            except Exception:
                return False
        except Exception:
            return False

    def wait_for_message_lock(self, user_id: str) -> bool:
        """
        Spin-wait until the per-user message lock is acquired or the deadline passes.
        Returns True if the lock was acquired, False if timed out.
        """
        deadline = time.time() + max(self.message_lock_wait_sec, 0.0)
        while True:
            if self._acquire_message_lock(user_id):
                return True
            if time.time() >= deadline:
                return False
            time.sleep(max(self.message_lock_poll_sec, 0.05))

    # --- turn budget helpers ---
    def inc_turn(self, user_id: str) -> int:
        doc = self._safe_get(user_id)
        n = int((doc.get("turn_count") or 0)) + 1
        self._merge(user_id, {"turn_count": n})
        return n
     
    def inc_text_count(self, user_id: str, delta: int = 1) -> int:
        doc = self._safe_get(user_id)
        n = int(doc.get("text_msg_count") or 0) + int(delta)
        self._merge(user_id, {"text_msg_count": n})
        return n

    def reset_text_count(self, user_id: str) -> None:
        self._merge(user_id, {"text_msg_count": 0})

    def reset_turns(self, user_id: str) -> None:
        self._merge(user_id, {"turn_count": 0})
    
    # --- running summary helpers ---
    def get_summary(self, user_id: str) -> str:
        return (self._safe_get(user_id) or {}).get("running_summary", "") or ""

    def set_summary(self, user_id: str, summary: str) -> None:
        # hard-cap to keep Firestore small
        self._merge(user_id, {"running_summary": (summary or "")[:4000]})

    # --- AUTHENTICATION HELPERS (NEW) ---
    def get_auth_status(self, user_id: str) -> bool:
        """Returns True if the user is already authenticated in the DB, False otherwise."""
        val = (self._safe_get(user_id) or {}).get("is_authenticated")
        return bool(val)

    def set_auth_status(self, user_id: str, status: bool) -> None:
        """Persist authentication status."""
        self._merge(user_id, {"is_authenticated": status})

    # --- onboarding success tracking ---
    def mark_onboarding_verified(self, user_id: str) -> None:
        """Mark the timestamp when onboarding (invoice verification) was completed."""
        now = self._now()
        payload = {"last_onboarding_verified_at": now}
        loc = self._fmt_pk_local(now)
        if loc:
            payload["last_onboarding_verified_at_local"] = loc["local"]
            payload["last_onboarding_verified_at_date"] = loc["date"]
        self._merge(user_id, payload)

    def get_last_onboarding_verified_at(self, user_id: str) -> float:
        """Return timestamp of last onboarding verification (or 0.0)."""
        try:
            doc = self._safe_get(user_id)
            ts = doc.get("last_onboarding_verified_at")
            return float(ts) if ts else 0.0
        except Exception:
            return 0.0

    def get_onboarding_status(self, user_id: str) -> str:
        """Return current onboarding status string (e.g., awaiting_invoice_1)."""
        return str((self._safe_get(user_id) or {}).get("onboarding_status") or "")

    def set_onboarding_status(self, user_id: str, status: Optional[str], *, reason: Optional[str] = None) -> None:
        """Persist onboarding status + timestamp (best-effort)."""
        now = self._now()
        payload = {
            "onboarding_status": status or None,
            "onboarding_status_at": now,
        }
        if reason:
            payload["onboarding_status_reason"] = reason
        loc = self._fmt_pk_local(now)
        if loc:
            payload["onboarding_status_at_local"] = loc["local"]
            payload["onboarding_status_at_date"] = loc["date"]
        self._merge(user_id, payload)

    def inc_onboarding_attempts(self, user_id: str) -> int:
        """Increment onboarding invoice attempts counter."""
        doc = self._safe_get(user_id)
        n = int(doc.get("onboarding_attempts") or 0) + 1
        self._merge(user_id, {"onboarding_attempts": n})
        return n

    def get_onboarding_invoice(self, user_id: str, *, slot: int = 1) -> dict:
        key = f"onboarding_invoice_{int(slot)}"
        val = (self._safe_get(user_id) or {}).get(key)
        return val if isinstance(val, dict) else {}

    def set_onboarding_invoice(self, user_id: str, data: dict, *, slot: int = 1) -> None:
        key = f"onboarding_invoice_{int(slot)}"
        now = self._now()
        payload = {
            key: data or {},
            f"{key}_at": now,
        }
        loc = self._fmt_pk_local(now)
        if loc:
            payload[f"{key}_at_local"] = loc["local"]
            payload[f"{key}_at_date"] = loc["date"]
        self._merge(user_id, payload)

    def clear_onboarding_invoice(self, user_id: str, *, slot: int = 1) -> None:
        key = f"onboarding_invoice_{int(slot)}"
        self._merge(user_id, {key: None})

    # --- catalogue send tracking ---
    def get_last_catalog_session_id(self, user_id: str) -> Optional[str]:
        """Return the session id for which catalog was last attempted."""
        doc = self._safe_get(user_id)
        val = doc.get("last_catalog_session_id")
        return str(val) if val else None

    def get_last_catalog_sent_at(self, user_id: str) -> float:
        """
        Return the timestamp when the catalog was last sent/attempted.
        Returns 0.0 if unknown.
        """
        try:
            doc = self._safe_get(user_id)
            ts = doc.get("last_catalog_sent_at")
            return float(ts) if ts else 0.0
        except Exception:
            return 0.0

    def mark_catalog_sent(self, user_id: str, session_id: Optional[str]) -> None:
        """Persist that catalog was sent/attempted for this session (session_id optional)."""
        now = self._now()
        payload = {
            "last_catalog_sent_at": now,
        }
        if session_id:
            payload["last_catalog_session_id"] = session_id
        loc = self._fmt_pk_local(now)
        if loc:
            payload["last_catalog_sent_at_local"] = loc["local"]
            payload["last_catalog_sent_at_date"] = loc["date"]
        self._merge(user_id, payload)

    # --- invoice-verification catalog guard ---
    def get_last_catalog_after_verification(self, user_id: str) -> float:
        """
        Return the timestamp when catalog was last sent immediately after invoice verification.
        """
        try:
            doc = self._safe_get(user_id)
            ts = doc.get("last_catalog_after_verification_at")
            return float(ts) if ts else 0.0
        except Exception:
            return 0.0

    def mark_catalog_after_verification(self, user_id: str, session_id: Optional[str]) -> None:
        """
        Persist a marker when catalog is sent right after successful invoice verification.
        Helps suppress auto-send on the very next message in the same conversation window.
        """
        now = self._now()
        payload = {
            "last_catalog_after_verification_at": now,
        }
        if session_id:
            payload["last_catalog_after_verification_session_id"] = session_id
        loc = self._fmt_pk_local(now)
        if loc:
            payload["last_catalog_after_verification_local"] = loc["local"]
            payload["last_catalog_after_verification_date"] = loc["date"]
        self._merge(user_id, payload)

    # --- catalog autosend suppression flag (consumed once) ---
    def set_catalog_autosend_suppressed(self, user_id: str, suppressed: bool = True) -> None:
        """Set or clear the single-use suppression flag for catalog autosend."""
        try:
            self._merge(user_id, {"catalog_autosend_suppressed": bool(suppressed)})
        except Exception:
            pass

    def is_catalog_autosend_suppressed(self, user_id: str) -> bool:
        """Peek suppression flag without clearing."""
        try:
            doc = self._safe_get(user_id)
            return bool(doc.get("catalog_autosend_suppressed"))
        except Exception:
            return False

    def consume_catalog_autosend_suppressed(self, user_id: str) -> bool:
        """
        Atomically read & clear the suppression flag.
        Returns True if suppression was active.
        """
        try:
            doc = self._safe_get(user_id)
            val = bool(doc.get("catalog_autosend_suppressed"))
            if val:
                self._merge(user_id, {"catalog_autosend_suppressed": False})
            return val
        except Exception:
            return False