import os
import ast
os.environ.setdefault("MEM0_DIR", "/tmp/mem0")
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
try:
    from google import genai
    from google.genai import types

    GenerativeModel = genai.GenerativeModel
    Content = types.Content
    Part = types.Part
except Exception:
    GenerativeModel = None
    Content = None
    Part = None
import requests
import json
import hashlib
import tempfile
import subprocess
import shutil
import threading
import asyncio
import re
import time
import datetime
import base64
import uuid
import boto3
from typing import Optional, Dict, Tuple, Any, List
from agents.tools.order_draft_tools import send_product_catalogue
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from dotenv import load_dotenv
from google.adk.runners import Runner  # type: ignore[import-untyped]
from google.adk.sessions import VertexAiSessionService  # type: ignore[import-untyped]
from google.cloud import firestore
from google.api_core.exceptions import AlreadyExists
from agents.audio.processing import VoiceNoteProcessor
from agents.agent import root_agent
from agents.helpers.session_helper import SessionStore
from agents.helpers.firestore_utils import get_tenant_id, get_agent_id, user_root
from utils.logging import logger
from agents.tools.api_tools import search_customer_by_phone, search_products_by_sku, unwrap_tool_response
from agents.guardrails import adk_guardrails
from agents.tools.templates import (
    order_draft_template,
    vn_order_draft_template,
    MULTI_MESSAGE_DELIMITER,
    extract_first_name,
)
from agents.audio import VoiceNoteTranscriber, TTSGenerator
from agents.audio.utils import (
    trim_trailing_silence,
    sniff_audio_mime,
    is_audio_too_small,
    clean_store_name_for_vn,
)
from agents.audio.greeting_cache import GreetingVNCache
load_dotenv()

TRIVIAL_GREETS = {
    "hi",
    "hello",
    "hey",
    "hi there",
    "hello there",
    "good morning",
    "good afternoon",
    "good evening",
}

_BARE_NUMBER_RE = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*$")

# --- Product discovery (YTL) ---
PROJECT_USECASE_STATUS = "awaiting_project_usecase"

GOODBYE_RE = re.compile(
    r"""
    ^\s*(
        ok\s*bye|
        bye\s*bye|
        bye|
        goodbye|
        good\s*bye|
        see\s*you|
        take\s*care
    )\s*[.!]*\s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

CUSTOMER_SAFE_FALLBACK_TEXT = (
    "I didn't quite catch that. Could you please repeat or rephrase your message in English?"
)
INTERNAL_ERROR_REPLY_MARKERS = (
    "request invalid lag rahi hai",
    "system error",
    "connection error",
    "no_feasible_recommendation",
    "requested_skus",
    "traceback",
    "stack trace",
    "api_jwt_token",
    "tenant_id",
)

TWILIO_TYPING_URL = "https://messaging.twilio.com/v2/Indicators/Typing.json"
PENDING_RECOMMENDATIONS_FIELD = "pending_recommendations"
PENDING_RECOMMENDATIONS_CONTEXT_MAX_ITEMS = 20
PENDING_ORDER_CONFIRMATION_FIELD = "pending_order_confirmation"
try:
    PENDING_RECOMMENDATIONS_TTL_SEC = max(
        60,
        int(os.getenv("PENDING_RECOMMENDATIONS_TTL_SEC", str(4 * 60 * 60))),
    )
except Exception:
    PENDING_RECOMMENDATIONS_TTL_SEC = 4 * 60 * 60
try:
    PENDING_ORDER_CONFIRMATION_TTL_SEC = max(
        5 * 60,
        int(os.getenv("PENDING_ORDER_CONFIRMATION_TTL_SEC", str(2 * 60 * 60))),
    )
except Exception:
    PENDING_ORDER_CONFIRMATION_TTL_SEC = 2 * 60 * 60

import asyncio
from google.api_core.exceptions import GoogleAPICallError

async def _vertex_op_with_retry(coro_factory, *, retries=2, base_delay=1.5, label="vertex_op"):
    last_exc = None
    for attempt in range(retries + 1):
        try:
            return await coro_factory()
        except Exception as exc:
            msg = str(exc).lower()
            is_transient = (
                "timeout" in msg or "deadline" in msg
                or "unavailable" in msg or "503" in msg
            )
            if not is_transient or attempt == retries:
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(f"{label}.retry", attempt=attempt + 1, delay=delay, error=str(exc))
            await asyncio.sleep(delay)
    raise last_exc

class ADKHelper:
    def __init__(self):
        # surface shared handles for mixins
        self.GenerativeModel = GenerativeModel
        # Default VN language; English for YTL Cement.
        self.vn_language = (os.getenv("VN_LANGUAGE") or "en").strip().lower()
        try:
            from google import genai
            from google.genai import types

            api_key = (
                os.getenv("GEMINI_API_KEY")
                or os.getenv("GOOGLE_API_KEY")
                or os.getenv("GENAI_API_KEY")
            )
            if api_key:
                http_opts_cls = getattr(types, "HttpOptions", None)
                http_opts = http_opts_cls(
                    baseUrl="https://generativelanguage.googleapis.com",
                    apiVersion="v1",
                ) if http_opts_cls else None
                self.client = genai.Client(api_key=api_key, http_options=http_opts, vertexai=False)
            else:
                self.client = None
                logger.error("Failed to initialize genai.Client: no API key present")
        except (ImportError, Exception):
            self.client = None
            logger.error("Failed to initialize genai.Client")

        self.model = os.getenv("VN_LLM_MODEL", "gemini-2.5-flash")

        # Transport toggle: meta (default) or twilio
        self.transport = (os.getenv("WHATSAPP_TRANSPORT", "meta") or "meta").strip().lower()
        self.is_twilio = self.transport == "twilio"

        # WhatsApp Cloud API
        self.whatsapp_token = os.getenv("WHATSAPP_ACCESS_TOKEN")
        self.phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
        self.WHATSAPP_API_VER = os.getenv("WHATSAPP_API_VER", "v23.0")
        self.WHATSAPP_BASE = f"https://graph.facebook.com/{self.WHATSAPP_API_VER}"
        self.whatsapp_api_url = f"{self.WHATSAPP_BASE}/{self.phone_number_id}/messages"
        self.whatsapp_media_url = f"{self.WHATSAPP_BASE}/{self.phone_number_id}/media"
        try:
            self.whatsapp_text_limit = int(os.getenv("WHATSAPP_TEXT_LIMIT", "4096"))
        except ValueError:
            self.whatsapp_text_limit = 4096

        # Twilio (used when transport == twilio)
        self.twilio_account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        self.twilio_auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        self.twilio_from_number = os.getenv("TWILIO_WHATSAPP_FROM") or os.getenv("TWILIO_FROM")
        self.twilio_status_callback = os.getenv("TWILIO_STATUS_CALLBACK_URL")
        self.twilio_media_bucket = os.getenv("TWILIO_MEDIA_BUCKET") or os.getenv("S3_BUCKET_NAME")
        try:
            self.twilio_media_expiry = int(os.getenv("TWILIO_MEDIA_EXPIRY_SEC", "3600"))
        except ValueError:
            self.twilio_media_expiry = 3600
        try:
            self.twilio_text_limit = int(os.getenv("TWILIO_TEXT_LIMIT", "1600"))
        except ValueError:
            self.twilio_text_limit = 1600

        # Vertex / Reasoning Engine
        REASONING_ENGINE_APP_NAME = "projects/agentflo/locations/us-central1/reasoningEngines/400917123859218432"
        self.LOCATION = os.environ.get("GOOGLE_CLOUD_LOCATION", "us-central1")
        self.PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT", "agentflo")
        self.APP_NAME = REASONING_ENGINE_APP_NAME

        # Clients
        self.db = firestore.Client()
        self.tenant_id = get_tenant_id()
        if (self.tenant_id or "").lower() == "ytl":
            self.vn_language = "en"
        self.session_service = VertexAiSessionService(project=self.PROJECT_ID, location=self.LOCATION)
                
        # HTTP sessions
        self.http_session = requests.Session()
        self.http_session.headers.update({
            "Authorization": f"Bearer {self.whatsapp_token}",
            "Content-Type": "application/json"
        })
        try:
            from requests.adapters import HTTPAdapter
            from urllib3.util.retry import Retry
            adapter = HTTPAdapter(pool_connections=50, pool_maxsize=50, max_retries=Retry(total=1, backoff_factor=0.2))
            self.http_session.mount("https://", adapter)
            self.http_session.mount("http://", adapter)
        except Exception:
            pass

        # ElevenLabs
        self.eleven_api_key = os.getenv("ELEVENLABS_API_KEY", "")
        self.eleven_voice_id = os.getenv("ELEVENLABS_VOICE_ID", os.getenv("ELEVENLABS_VOICE_ID", ""))
        self.eleven_model_id = os.getenv("ELEVEN_MODEL_ID", os.getenv("ELEVENLABS_MODEL_ID", "eleven_turbo_v2_5"))
        self.eleven_timeout = int(os.getenv("ELEVEN_TTS_TIMEOUT", "45"))
        self.tts_max_chars = int(os.getenv("TTS_MAX_CHARS", "3000"))
        self.enable_legacy_tts = str(os.getenv("ELEVEN_USE_LEGACY_FALLBACK", "false")).lower() == "true"

        # Speed knobs / budgets
        self.tts_short_threshold = int(os.getenv("TTS_SHORT_THRESHOLD", "240"))
        self.stream_watchdog_sec = float(os.getenv("TTS_STREAM_WATCHDOG_SEC", "8.0"))
        self.parallel_deadline_sec = float(os.getenv("VN_PARALLEL_DEADLINE_SEC", "10"))
        # default remains 30s unless env overrides
        self.tts_global_deadline_sec = float(os.getenv("TTS_GLOBAL_DEADLINE_SEC", "30"))

        self.eleven_session = requests.Session()
        # default Accept header for eleven_session; individual calls will pass their own headers too
        self.eleven_session.headers.update({"xi-api-key": self.eleven_api_key, "Accept": "audio/ogg; codecs=opus"})
        try:
            from requests.adapters import HTTPAdapter
            adapter2 = HTTPAdapter(pool_connections=50, pool_maxsize=50)
            self.eleven_session.mount("https://", adapter2)
            self.eleven_session.mount("http://", adapter2)
        except Exception:
            pass

        # Behavior (updated defaults)
        self.interjection_text = os.getenv(
            "VOICE_INTERJECTION_TEXT",
            "Hold on, let me explain in a voice note"
        )
        self.interjection_enabled = str("false")
        self.voice_policy = os.getenv("VOICE_INPUT_TEXT_POLICY", "vn_or_text")

        # Text dedupe
        self.text_dedupe_sec = int(os.getenv("TEXT_DEDUPE_SECONDS", "30"))
        self.text_dedupe_enabled = str(os.getenv("TEXT_DEDUPE_ENABLED", "true")).lower() == "true"

        # Inbound + outbox idempotency
        self.inbound_dedupe_enabled = str(os.getenv("INBOUND_DEDUPE_ENABLED", "true")).lower() == "true"
        self.inbound_ttl_sec = int(os.getenv("INBOUND_TTL_SECONDS", "900"))
        self.voice_lock_ttl_sec = int(os.getenv("VOICE_LOCK_TTL_SECONDS", "40"))
        self.VN_TRIM_MODE = os.getenv("VN_TRIM_MODE", "safe").lower()
        self.message_lock_ttl_sec = int(os.getenv("MESSAGE_LOCK_TTL_SECONDS", "45"))
        self.message_lock_wait_sec = float(os.getenv("MESSAGE_LOCK_WAIT_SECONDS", "6"))
        self.message_lock_poll_sec = float(os.getenv("MESSAGE_LOCK_POLL_SECONDS", "0.25"))
        self.debug_agent_logs = str(os.getenv("DEBUG_AGENT_LOGS", "false")).lower() == "true"

        # Fallback behavior toggles
        self.audio_fallback_to_mp3 = str(os.getenv("VOICE_FALLBACK_TO_AUDIO", "true")).lower() == "true"

        # VN Cache
        self.greeting_vn_cache = GreetingVNCache(
            db=self.db,
            tenant_id=self.tenant_id,
            tts_generator=TTSGenerator(),
            vn_processor=VoiceNoteProcessor(
                language=self.vn_language or "en",
                genai_client=self.client,
                model=self.model,
            ),
            send_audio_func=self._upload_and_send_audio,
            get_metadata_func=self._get_stored_customer_metadata,
            agent_id=get_agent_id(),
        )

        # Cache
        self._session_cache: Dict[str, str] = {}

        # NEW: Session lifecycle helper
        self.session_helper = SessionStore()

        # Latest per-user Gemini usage snapshot for external billing logs
        self._last_gemini_usage: Dict[str, dict] = {}
        # Latest per-user ElevenLabs TTS usage snapshot for external billing logs
        self._last_eleven_tts_usage: Dict[str, dict] = {}

    # ---------- Helpers ----------
    def _sha(self, s: str) -> str:
        return hashlib.sha1((s or "").encode("utf-8")).hexdigest()

    async def _prepare_vn_text(self, text: str, lang_code: Optional[str] = None) -> str:
        """
        Prepare text for voice note generation.
        Delegates to VoiceNoteProcessor.
        """
        if not text:
            return ""
        
        try:
            vn_processor = self.greeting_vn_cache.vn_processor
            return await vn_processor.process(text, lang_code=lang_code)
        except Exception as e:
            logger.error("_prepare_vn_text.error", error=str(e))
            return text

    async def _tts_get_audio_bytes(self, text: str, lang_code: Optional[str] = None) -> Tuple[Optional[bytes], dict, Optional[bytes]]:
        """
        Generate TTS audio bytes.
        Delegates to TTSGenerator.
        
        Returns:
            (preferred_bytes, meta, mp3_bytes)
        """
        if not text:
            return None, {}, None
        
        try:
            tts_generator = self.greeting_vn_cache.tts_generator
            return await tts_generator.generate_audio(text, language_code=lang_code)
        except Exception as e:
            logger.error("_tts_get_audio_bytes.error", error=str(e))
            return None, {}, None

    def _user_root(self, user_id: str):
        return user_root(self.db, user_id, tenant_id=self.tenant_id)

    def _inbound_mark_if_new(self, user_id: str, inbound_key: str) -> bool:
        doc_id = self._sha(inbound_key)
        ref = self._user_root(user_id).collection("inbound_keys").document(doc_id)
        try:
            ref.create({"created": time.time(), "ttl": time.time() + self.inbound_ttl_sec})
            return True
        except AlreadyExists:
            return False
        except Exception as e:
            logger.warning(f"inbound.mark error (processing anyway): {e}")
            return True

    def _outbox_try_start(self, user_id: str, inbound_key: Optional[str]) -> bool:
        if not inbound_key:
            return True
        ref = self._user_root(user_id).collection("outbox").document(self._sha(inbound_key))
        try:
            ref.create({"state": "started", "ts": time.time()})
            return True
        except AlreadyExists:
            return False
        except Exception as e:
            logger.warning(f"outbox.start error (allow once): {e}")
            return True

    def _outbox_mark_sent(self, user_id: str, inbound_key: Optional[str]):
        if not inbound_key:
            return
        ref = self._user_root(user_id).collection("outbox").document(self._sha(inbound_key))
        try:
            ref.set({"state": "sent", "ts": time.time()}, merge=True)
        except Exception as e:
            logger.warning(f"outbox.mark_sent error: {e}")

    def _release_voice_lock(self, user_id: str):
        try:
            self._user_root(user_id).collection("voice_locks").document("lock").delete()
        except Exception:
            pass

    def _acquire_voice_lock(self, user_id: str) -> bool:
        ref = self._user_root(user_id).collection("voice_locks").document("lock")
        now = time.time()
        try:
            ref.create({"locked_at": now, "expires": now + self.voice_lock_ttl_sec})
            return True
        except AlreadyExists:
            try:
                doc = ref.get()
                data = doc.to_dict() or {}
                if data.get("expires", 0) < now:
                    ref.set({"locked_at": now, "expires": now + self.voice_lock_ttl_sec}, merge=True)
                    return True
                return False
            except Exception:
                return False
        except Exception:
            return False

    # ---------- Session ----------
    def _get_cached_session_id(self, user_id: str) -> Optional[str]:
        sid = self._session_cache.get(user_id)
        if sid:
            return sid
        sid = self.session_helper.get_current_session_id(user_id)
        if sid:
            self._session_cache[user_id] = sid
        return sid

    def _update_cached_session_id(self, user_id: str, session_id: str):
        self._session_cache[user_id] = session_id
        self.session_helper.update_session_id(user_id, session_id)

    async def setup_session_and_runner(self, user_id: str, session_id: Optional[str]):
        session = None
        try:
            if session_id:
                session = await _vertex_op_with_retry(
                    lambda: self.session_service.get_session(
                        app_name=self.APP_NAME, user_id=user_id, session_id=session_id
                    ),
                    label="session.get",
                )
        except Exception as e:
            logger.warning(
                "session.get_failed.creating_new",
                user_id=user_id,
                session_id=session_id,
                error=str(e),
            )

        if session is None:
            # Stale/expired/timed-out — mint a fresh one
            session = await _vertex_op_with_retry(
                lambda: self.session_service.create_session(
                    app_name=self.APP_NAME,
                    user_id=user_id,
                    state={"user_id": user_id, "wa_user_id": user_id},
                ),
                label="session.create_fallback",
            )
            self._update_cached_session_id(user_id, session.id)

        runner = Runner(
            agent=root_agent,
            app_name=self.APP_NAME,
            session_service=self.session_service,
        )
        return session, runner

    # ---------- Location helpers ----------
    def user_exists(self, user_id: str) -> bool:
        try:
            doc_ref = self._user_root(user_id)
            return doc_ref.get().exists
        except Exception as e:
            logger.error(f"user_exists error: {e}")
            return False

    def has_already_requested_location(self, user_id: str) -> bool:
        try:
            doc_ref = self._user_root(user_id)
            snap = doc_ref.get()
            if not snap.exists:
                return False
            data = snap.to_dict() or {}
            return bool(data.get("location_request_sent"))
        except Exception:
            return False

    def mark_location_request_sent(self, user_id: str) -> None:
        try:
            self._user_root(user_id).set(
                {"location_request_sent": True, "location_request_ts": time.time()},
                merge=True
            )
        except Exception as e:
            logger.warning(f"mark_location_request_sent failed: {e}")

    # ---------- Main API ----------
    def _run_async(self, coro):
        """
        Run async coroutines safely from sync contexts.
        Avoid asyncio.run() when an event loop is already running.
        """
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(coro)

        result: dict = {"value": None, "error": None}

        def _runner():
            try:
                result["value"] = asyncio.run(coro)
            except Exception as e:
                result["error"] = e

        t = threading.Thread(target=_runner, daemon=True)
        t.start()
        t.join()
        if result["error"]:
            raise result["error"]
        return result["value"]

    def handle_message(
        self, 
        message: str, 
        user_id: str, 
        *, 
        is_voice_input: bool = False, 
        inbound_key: Optional[str] = None, 
        reply_to_message_id: Optional[str] = None,
        disable_catalog: bool = False,
        vn_lang_code: Optional[str] = None,
    ) -> str:
        return self._run_async(
            self.handle_message_async(
                message,
                user_id,
                is_voice_input=is_voice_input,
                inbound_key=inbound_key,
                reply_to_message_id=reply_to_message_id,
                disable_catalog=disable_catalog,
                vn_lang_code=vn_lang_code,
            )
        )

    async def _summarize_turn(self, running_summary: str, user_text: str, assistant_text: str) -> str:
        """
        Update a concise, durable session summary: decisions, commitments, preferences,
        order state. Keep ~250–400 words. No chit-chat.
        """
        try:
            if GenerativeModel is None:
                return running_summary or ""
            model = GenerativeModel(os.getenv("SUMMARIZER_MODEL", "gemini-2.5-flash"))
            sys = ("You maintain a concise session summary for a WhatsApp sales assistant. "
                "Record only durable facts, decisions, commitments, and active order status. "
                "Keep it brief (<= 400 words).")
            prompt = (
                f"[PRIOR]\n{running_summary or ''}\n[/PRIOR]\n"
                f"[TURN]\n[U]{(user_text or '').strip()}[/U]\n[A]{(assistant_text or '').strip()}[/A]\n[/TURN]\n"
                "[TASK]Return ONLY the updated summary.[/TASK]"
            )
            resp = await asyncio.to_thread(model.generate_content, [sys, prompt])
            out = (getattr(resp, "text", "") or "").strip()
            return out or (running_summary or "")
        except Exception:
            return running_summary or ""

    # --- AUTHENTICATION + SESSION LOGIC (JSON Parsing) ---
    def _bootstrap_user_from_api(self, wa_user_id: str) -> Optional[str]:
        """
        Ensure this WhatsApp user is mapped to a backend user_id.

        Returns:
          canonical backend user_id on success, or None on failure.
        """
        # 1) If we already have a mapping, just return it
        existing = self.get_external_user_id(wa_user_id)
        if existing:
            return existing

        logger.info("bootstrap_user_from_api.start", wa_user_id=wa_user_id)

        api_result = search_customer_by_phone(wa_user_id)
        ok, data, err = unwrap_tool_response(api_result, system_name="search_customer_by_phone")
        if not ok:
            logger.warning("bootstrap_user_from_api.error", wa_user_id=wa_user_id, error=err)
            return None

        if not data or not isinstance(data, dict):
            logger.warning(
                "bootstrap_user_from_api.invalid_payload",
                wa_user_id=wa_user_id,
                payload_type=type(data).__name__,
            )
            return None

        if data.get("success") is not True:
            logger.warning("bootstrap_user_from_api.success_false", wa_user_id=wa_user_id, data=data)
            return None

        # Adjust this to your real schema
        payload = (
            data.get("customer")
            or data.get("data")
            or data
        )
        canonical_id = (
            payload.get("user_id")
            or payload.get("id")
            or payload.get("customer_id")
        )

        if not canonical_id:
            logger.warning("bootstrap_user_from_api.no_canonical_id", wa_user_id=wa_user_id, data=payload)
            return None

        # Persist mapping
        self.create_user_document(wa_user_id, external_user_id=canonical_id)
        self.set_external_user_id(wa_user_id, canonical_id)
        logger.info("bootstrap_user_from_api.success", wa_user_id=wa_user_id, canonical_id=canonical_id)
        return canonical_id

    def _fetch_customer_metadata(self, wa_user_id: str) -> Dict[str, Optional[str]]:
        """
        Best-effort wrapper around search_customer_by_phone to extract store/customer details.
        Returns a dict with None defaults so callers can safely merge into session state.
        """
        meta: Dict[str, Optional[str]] = {
            "store_code": None,
            "storecode": None,  # alias for downstream tools
            "store_company_code": None,
            "customer_name": None,
            "store_name": None,
            "store_name_en": None,
            "channel_name": None,
        }

        try:
            api_result = search_customer_by_phone(wa_user_id)
            ok, payload, err = unwrap_tool_response(api_result, system_name="search_customer_by_phone")
            if not ok or not payload:
                logger.warning(
                    "customer.metadata.lookup_failed",
                    wa_user_id=wa_user_id,
                    reason="empty_or_error",
                    error=err,
                )
                return meta

            if not isinstance(payload, dict):
                logger.warning(
                    "customer.metadata.invalid_payload",
                    wa_user_id=wa_user_id,
                    payload_type=type(payload).__name__,
                )
                return meta

            data = payload.get("data", {}) if isinstance(payload, dict) else {}
            additional = data.get("additional_info") or {}

            store_code = (
                additional.get("storecode")
                or data.get("store_key")
            )
            if not store_code:
                for entry in data.get("external_ids") or []:
                    if isinstance(entry, dict):
                        ref = entry.get("ref")
                        if ref:
                            store_code = ref
                            break

            meta["store_code"] = store_code or None
            meta["storecode"] = store_code or None
            meta["store_company_code"] = (additional.get("storecompanycode") or None)
            meta["customer_name"] = (data.get("contact_name") or data.get("customer_name") or None)
            meta["store_name"] = (data.get("display_name") or data.get("store_name") or None)
            meta["store_name_en"] = meta["store_name"]

            channel = data.get("channel") or {}
            if isinstance(channel, dict):
                meta["channel_name"] = channel.get("name") or None

        except Exception as e:
            logger.warning("customer.metadata.lookup_exception", wa_user_id=wa_user_id, error=str(e))

        # Normalize to strings where present
        return {k: (str(v) if v is not None else None) for k, v in meta.items()}

    def _get_user_doc(self, user_id: str) -> dict:
        try:
            snap = self._user_root(user_id).get()
            return snap.to_dict() or {}
        except Exception:
            return {}

    def _get_pending_recommendations(self, wa_user_id: str) -> Optional[Dict[str, Any]]:
        data = self._get_user_doc(wa_user_id)
        pending = data.get(PENDING_RECOMMENDATIONS_FIELD)
        if not isinstance(pending, dict):
            return None

        created_epoch = pending.get("created_epoch")
        if isinstance(created_epoch, (int, float)):
            if (time.time() - float(created_epoch)) > PENDING_RECOMMENDATIONS_TTL_SEC:
                try:
                    self._user_root(wa_user_id).set(
                        {PENDING_RECOMMENDATIONS_FIELD: firestore.DELETE_FIELD},
                        merge=True,
                    )
                except Exception:
                    pass
                return None

        raw_items = pending.get("items")
        if not isinstance(raw_items, list):
            return None

        items: List[Dict[str, Any]] = []
        seen: set[str] = set()
        for itm in raw_items:
            if not isinstance(itm, dict):
                continue
            sku = str(itm.get("sku_code") or itm.get("sku") or "").strip()
            if not sku:
                continue
            sku_key = sku.lower()
            if sku_key in seen:
                continue
            try:
                qty = int(itm.get("qty") or 0)
            except Exception:
                qty = 0
            if qty <= 0:
                continue
            name = str(itm.get("name") or "").strip()
            entry: Dict[str, Any] = {"sku_code": sku, "qty": qty}
            if name:
                entry["name"] = name
            items.append(entry)
            seen.add(sku_key)
            if len(items) >= PENDING_RECOMMENDATIONS_CONTEXT_MAX_ITEMS:
                break

        if not items:
            return None

        return {
            "objective": pending.get("objective"),
            "store_id": pending.get("store_id"),
            "created_at": pending.get("created_at"),
            "items": items,
        }

    def _is_order_confirmation_prompt_text(self, text: str) -> bool:
        if not isinstance(text, str):
            return False
        t = " ".join(text.strip().lower().split())
        if not t:
            return False

        # Common pack phrases across supported locales.
        fixed_hints = (
            "should i confirm this for you",
            "is this the final order",
            "ready to place this order",
            "ready to place your order",
            "are you ready to place your order",
            "would you like to place this order",
            "adakah ini pesanan akhir",
            "هل هذا هو الطلب النهائي",
            "这是最终订单吗",
        )
        if any(hint in t for hint in fixed_hints):
            return True
        if "yes / confirm" in t:
            return True
        if "place" in t and "order" in t and ("ready" in t or "confirm" in t or "final" in t):
            return True
        return ("confirm" in t and "order" in t and ("final" in t or "should i" in t))

    def remember_order_confirmation_context(
        self,
        wa_user_id: str,
        messages: List[str],
        *,
        source: str = "unknown",
    ) -> None:
        """
        Persist the latest order-summary + confirmation prompt messages so
        affirmative replies (yes/confirm) can be interpreted correctly.
        """
        clean_messages = [
            str(m).strip()
            for m in (messages or [])
            if isinstance(m, str) and str(m).strip()
        ]
        if not clean_messages:
            return
        if not any(self._is_order_confirmation_prompt_text(msg) for msg in clean_messages):
            return

        now_epoch = time.time()
        payload = {
            PENDING_ORDER_CONFIRMATION_FIELD: {
                "messages": clean_messages[:6],
                "source": source,
                "created_epoch": now_epoch,
                "created_at": datetime.datetime.utcfromtimestamp(now_epoch).isoformat() + "Z",
            }
        }
        try:
            self._user_root(wa_user_id).set(payload, merge=True)
            logger.info(
                "order_confirm.context.saved",
                user_id=wa_user_id,
                source=source,
                message_count=len(clean_messages[:6]),
            )
        except Exception as e:
            logger.warning("order_confirm.context.save_failed", user_id=wa_user_id, error=str(e))

    def clear_pending_order_confirmation(self, wa_user_id: str, *, reason: str = "") -> None:
        try:
            self._user_root(wa_user_id).set(
                {PENDING_ORDER_CONFIRMATION_FIELD: firestore.DELETE_FIELD},
                merge=True,
            )
            logger.info(
                "order_confirm.context.cleared",
                user_id=wa_user_id,
                reason=reason or "manual",
            )
        except Exception as e:
            logger.warning("order_confirm.context.clear_failed", user_id=wa_user_id, error=str(e))

    def _get_pending_order_confirmation(self, wa_user_id: str) -> Optional[Dict[str, Any]]:
        data = self._get_user_doc(wa_user_id)
        pending = data.get(PENDING_ORDER_CONFIRMATION_FIELD)
        if not isinstance(pending, dict):
            return None

        created_epoch = pending.get("created_epoch")
        if isinstance(created_epoch, (int, float)):
            if (time.time() - float(created_epoch)) > PENDING_ORDER_CONFIRMATION_TTL_SEC:
                self.clear_pending_order_confirmation(wa_user_id, reason="ttl_expired")
                return None

        raw_messages = pending.get("messages")
        if not isinstance(raw_messages, list):
            return None
        messages = [
            str(m).strip()
            for m in raw_messages
            if isinstance(m, str) and str(m).strip()
        ][:6]
        if not messages:
            return None
        if not any(self._is_order_confirmation_prompt_text(msg) for msg in messages):
            return None

        return {
            "messages": messages,
            "source": pending.get("source"),
            "created_at": pending.get("created_at"),
        }

    def _build_pending_order_confirmation_context(self, wa_user_id: str) -> str:
        pending = self._get_pending_order_confirmation(wa_user_id)
        if not pending:
            return ""

        lines: List[str] = [
            "A pending order confirmation prompt was already sent to the user.",
            "Treat simple affirmations (yes/confirm/ok/final) as confirmation for this exact pending draft.",
            "If user says no/edit/change, do not place order and continue cart-edit flow.",
        ]
        source = pending.get("source")
        created_at = pending.get("created_at")
        if source:
            lines.append(f"source: {source}")
        if created_at:
            lines.append(f"created_at: {created_at}")

        lines.append("recent_assistant_messages:")
        for idx, msg in enumerate(pending.get("messages") or [], 1):
            compact = " ".join(str(msg).split())
            lines.append(f"{idx}. {compact}")

        lines.append(
            "This pending order confirmation context takes precedence over pending recommendation confirmations."
        )
        return "\n".join(lines)

    def _build_pending_recommendations_context(self, wa_user_id: str) -> str:
        pending = self._get_pending_recommendations(wa_user_id)
        if not pending:
            return ""

        lines: List[str] = [
            "There is a pending recommendation list from a previous turn.",
            "Use it only if the user confirms the previously suggested recommendations (e.g., yes/add these/add all).",
        ]
        objective = pending.get("objective")
        store_id = pending.get("store_id")
        created_at = pending.get("created_at")
        if objective:
            lines.append(f"objective: {objective}")
        if store_id:
            lines.append(f"store_id: {store_id}")
        if created_at:
            lines.append(f"created_at: {created_at}")

        lines.append("items:")
        for idx, itm in enumerate(pending.get("items") or [], 1):
            sku = itm.get("sku_code")
            qty = itm.get("qty")
            name = itm.get("name")
            if name:
                lines.append(f"{idx}. sku={sku}; qty={qty}; name={name}")
            else:
                lines.append(f"{idx}. sku={sku}; qty={qty}")

        lines.append(
            "If the current user message is an affirmative confirmation without a different item, add this full list in one agentflo_cart_tool call."
        )
        return "\n".join(lines)

    def _persist_customer_metadata(self, wa_user_id: str, meta: Dict[str, Optional[str]]) -> None:
        if not meta:
            return
        try:
            clean = {k: v for k, v in meta.items() if v}
            if not clean:
                return
            self._user_root(wa_user_id).set(clean, merge=True)
        except Exception as e:
            logger.warning("customer.metadata.persist_failed", wa_user_id=wa_user_id, error=str(e))

    def _get_stored_customer_metadata(self, wa_user_id: str) -> Dict[str, Optional[str]]:
        data = self._get_user_doc(wa_user_id)
        keys = [
            "store_code",
            "storecode",
            "store_company_code",
            "customer_name",
            "store_name",
            "store_name_en",
            "channel_name",
        ]
        return {k: (data.get(k) or None) for k in keys}

    def _ensure_customer_metadata(self, wa_user_id: str) -> Dict[str, Optional[str]]:
        stored = self._get_stored_customer_metadata(wa_user_id)
        if any(stored.values()):
            return stored
        fetched = self._fetch_customer_metadata(wa_user_id)
        if any(fetched.values()):
            self._persist_customer_metadata(wa_user_id, fetched)
            return fetched
        return stored

    def _build_customer_context_for_agent(self, wa_user_id: str) -> str:
        meta = self._get_stored_customer_metadata(wa_user_id)
        if not meta or not any(meta.values()):
            return ""
        parts = []
        code = meta.get("store_code") or meta.get("storecode")
        if code:
            parts.append(f"store_code: {code}")
        if meta.get("store_company_code"):
            parts.append(f"store_company_code: {meta['store_company_code']}")
        if meta.get("store_name"):
            parts.append(f"store_name: {meta['store_name']}")
        if meta.get("customer_name"):
            parts.append(f"customer_name: {meta['customer_name']}")
        if meta.get("channel_name"):
            parts.append(f"channel: {meta['channel_name']}")
        return " | ".join(parts)

    # ---------- Name change flow ----------
    def get_name_change_state(self, user_id: str) -> bool:
        """Returns True if we're waiting for the user to supply a new name."""
        try:
            doc = self._user_root(user_id).get()
            if not doc.exists:
                return False
            return bool((doc.to_dict() or {}).get("name_change_pending"))
        except Exception as e:
            logger.warning("name_change.get_state_failed", user_id=user_id, error=str(e))
            return False

    def set_name_change_state(
        self,
        user_id: str,
        pending: bool,
        *,
        require_explicit_name: bool = False,
    ) -> None:
        """Set/clear the name-capture flow flag and optional explicit-name requirement."""
        try:
            payload = {"name_change_pending": bool(pending)}
            if pending:
                payload["name_change_require_explicit"] = bool(require_explicit_name)
            else:
                payload["name_change_require_explicit"] = False
            self._user_root(user_id).set(payload, merge=True)
        except Exception as e:
            logger.warning("name_change.set_state_failed", user_id=user_id, error=str(e))

    def get_name_change_require_explicit(self, user_id: str) -> bool:
        """Returns True when current name-capture flow requires explicit name text."""
        try:
            doc = self._user_root(user_id).get()
            if not doc.exists:
                return False
            return bool((doc.to_dict() or {}).get("name_change_require_explicit"))
        except Exception as e:
            logger.warning("name_change.get_require_explicit_failed", user_id=user_id, error=str(e))
            return False

    def extract_name_from_text(self, text: str) -> Optional[str]:
        """
        Use Gemini to extract a person's name from free-form text.
        Falls back to a simple capitalised-word heuristic.

        Returns the extracted name string, or None if nothing plausible found.
        """
        if not text or not text.strip():
            return None

        # ------------------------------------------------------------------
        # 1) LLM extraction (best effort)
        # ------------------------------------------------------------------
        if self.client:
            try:
                from google.genai import types as _gtypes

                prompt = (
                    "Extract only the person's name from the text below. "
                    "Reply with just the name and nothing else — no punctuation, no explanation. "
                    "If no name is present, reply exactly: NONE\n\n"
                    f"Text: {text.strip()}"
                )
                response = self.client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                )
                raw = (getattr(response, "text", "") or "").strip()
                if raw and raw.upper() != "NONE" and len(raw) <= 60:
                    # Basic sanity: shouldn't contain digits or common junk phrases
                    if not any(ch.isdigit() for ch in raw) and "\n" not in raw:
                        logger.info(
                            "name_change.extracted_via_llm",
                            raw_text=text[:80],
                            extracted=raw,
                        )
                        return raw
            except Exception as e:
                logger.warning("name_change.llm_extract_failed", error=str(e))

        # ------------------------------------------------------------------
        # 2) Heuristic fallback: first capitalised word(s) not in a stop-list
        # ------------------------------------------------------------------
        _STOP = {
            "ok", "okay", "yes", "no", "sure", "right",
            "change", "update", "keep",
            "bro",
            "my", "name", "is", "its", "it's", "i", "am", "this", "that",
        }
        words = text.strip().split()
        candidates = [
            w.strip(".,!?;:'\"")
            for w in words
            if w and w[0].isupper() and w.lower().strip(".,!?;:'\"") not in _STOP
        ]
        if candidates:
            # take up to 2 consecutive capitalised words (handles "Muhammad Ali" etc.)
            name = " ".join(candidates[:2])
            if name:
                logger.info(
                    "name_change.extracted_via_heuristic",
                    raw_text=text[:80],
                    extracted=name,
                )
                return name

        # 3) Lowercase / non-capitalized fallback:
        # accept up to two alphabetic tokens not in stop-list.
        plain_tokens = []
        for raw_word in words:
            tok = raw_word.strip(".,!?;:'\"")
            if not tok:
                continue
            low = tok.lower()
            if low in _STOP:
                continue
            if any(ch.isdigit() for ch in tok):
                continue
            if not all(ch.isalpha() or ch in {"-", "'"} for ch in tok):
                continue
            plain_tokens.append(tok)
            if len(plain_tokens) >= 2:
                break

        if plain_tokens:
            name = " ".join(plain_tokens).strip()
            if name:
                logger.info(
                    "name_change.extracted_via_plain_fallback",
                    raw_text=text[:80],
                    extracted=name,
                )
                return name

        return None

    def _get_onboarding_invoice_prompt(self, *, step: int = 1) -> str:
        return (
            "You are not yet verified in our system.\n"
            "Please send a clear photo of your invoice.\n"
            "Just one invoice photo is needed — once verified, you can place orders."
        )

    def _was_order_placed(self, agent_text: str, session_state: dict) -> bool:
        """
        Detects if an order was placed in this turn.
        1. Checks if specific tools were used (if available in state).
        2. Checks for specific text keywords in the agent's response.
        """
        # 1. Check Tool Usage (High Confidence)
        # Note: This relies on the runner populating state (which your code does via adk_guardrails)
        tool_sequence = session_state.get("_turn_tool_sequence", []) if session_state else []
        order_tools = {"confirmOrderDraftTool", "place_order", "generate_invoice"}
        
        for tool_call in tool_sequence:
            # tool_call might be a dict or string depending on implementation
            name = tool_call.get("name") if isinstance(tool_call, dict) else str(tool_call)
            if name in order_tools:
                return True

        # 2. Check Text Heuristics (Fallback)
        # Detect phrases indicating a generated invoice or successful order
        text = (agent_text or "").lower()
        success_phrases = [
            "order id", 
            "order has been placed", 
            "invoice generated", 
            "invoice has been generated",
            "thank you, order confirm",
            "order confirmed"
        ]
        
        return any(phrase in text for phrase in success_phrases)
    
    async def handle_message_async(
        self,
        message: str,
        user_id: str,
        *,
        is_voice_input: bool = False,
        inbound_key: Optional[str] = None,
        reply_to_message_id: Optional[str] = None,
        disable_catalog: bool = False,
        vn_lang_code: Optional[str] = None,
    ) -> str:
        # Defaults
        conversation_id: Optional[str] = None
        conv_is_new: bool = False
        wa_user_id = user_id
        lock_acquired = False
        
        try:
            # --- 0. LOCKING ---
            lock_acquired = await asyncio.to_thread(self.session_helper.wait_for_message_lock, wa_user_id)
            if not lock_acquired:
                logger.info("message.lock.busy", user_id=wa_user_id)
                busy_msg = "jee thora sa time lag raha hai, main dobara check karti hoon."
                self._send_text_once(wa_user_id, busy_msg, reply_to_message_id=reply_to_message_id)
                return busy_msg

            # --- 1. NO AUTHENTICATION (YTL demo) ---
            # All users can chat. We only do a lightweight profile bootstrap:
            # - If we have customer metadata in Firestore, use it.
            # - Else try local customers.json via search_customer_by_phone (through _ensure_customer_metadata).
            # - If still missing name, ask once and store it.
            onboarding_status = self.session_helper.get_onboarding_status(wa_user_id)

            # If we're waiting for the user's name, treat this message as the name and store it.
            if onboarding_status == "awaiting_name":
                proposed = " ".join((message or "").strip().split())
                # Keep it simple: accept any non-empty text up to 60 chars as name.
                if proposed:
                    from typing import Dict, Optional as _Opt
                    meta: Dict[str, _Opt[str]] = {"customer_name": proposed}
                    self._persist_customer_metadata(wa_user_id, meta)
                    self.session_helper.set_onboarding_status(wa_user_id, None)
                else:
                    self._send_text_once(
                        wa_user_id,
                        "Please share your name (e.g., \"Sakina\").",
                        reply_to_message_id=reply_to_message_id,
                    )
                    return "Please share your name (e.g., \"Sakina\")."

            # Ensure we have best-effort metadata (Firestore first, then local dummy data)
            try:
                await asyncio.to_thread(self._ensure_customer_metadata, wa_user_id)
            except Exception:
                pass

            meta_now = self._get_stored_customer_metadata(wa_user_id)
            if not (meta_now.get("customer_name") or "").strip():
                self.session_helper.set_onboarding_status(wa_user_id, "awaiting_name", reason="missing_name")
                msg = "Hi! What’s your name?"
                self._send_text_once(wa_user_id, msg, reply_to_message_id=reply_to_message_id)
                return msg

            # --- 2. SESSION ROTATION CHECK (single, unified) ---
            # Rotate when: pending-end timer matured OR user silent >= SESSION_INACTIVITY_SEC (default 12 h)
            try:
                _inactivity_sec = int(os.getenv("SESSION_INACTIVITY_SEC", str(12 * 60 * 60)))
            except Exception:
                _inactivity_sec = 12 * 60 * 60

            session_id = self._get_cached_session_id(wa_user_id)
            should_rotate, rotate_reason = self.session_helper.should_start_new_session(
                wa_user_id, inactivity_sec=_inactivity_sec
            )

            if should_rotate or not session_id:
                logger.info("session.rotate", user_id=wa_user_id, reason=rotate_reason or "fresh_start", had_session=bool(session_id))
                self.session_helper.end_session(wa_user_id, reason=rotate_reason or "inactivity")
                session_id = await self.save_and_create_new_session(wa_user_id, session_id)

            # --- 3. RECORD INCOMING MESSAGE (touch + cancel pending end, exactly once) ---
            self.session_helper.on_incoming_message(wa_user_id, inbound_key=inbound_key)

            # Best-effort metadata fetch & persist
            try:
                await asyncio.to_thread(self._ensure_customer_metadata, wa_user_id)
            except Exception:
                pass

            # --- 4. INBOUND DEDUPE ---
            if inbound_key and self.inbound_dedupe_enabled:
                first_time = self._inbound_mark_if_new(wa_user_id, inbound_key)
                if not first_time:
                    logger.info("inbound.duplicate.skip", inbound=inbound_key, user_id=wa_user_id)
                    return ""

            # --- 5. CONVERSATION TRACKING ---
            try:
                conversation_id, conv_is_new = self.session_helper.get_or_start_conversation(
                    wa_user_id, conversation_inactivity_sec=_inactivity_sec
                )
                self.session_helper.touch_conversation(wa_user_id, source="user")
            except Exception as e:
                logger.warning("conversation.ensure_failed", user_id=wa_user_id, error=str(e))
                conversation_id, conv_is_new = None, False

            # --- 5.5 PRODUCT DISCOVERY (guided by project/use-case) ---
            # This is intentionally handled BEFORE catalog autosend and BEFORE calling the agent
            # to avoid dumping long SKU lists and to reduce hallucinated pricing.
            try:
                flow_status = self.session_helper.get_onboarding_status(wa_user_id)
            except Exception:
                flow_status = ""

            if flow_status == PROJECT_USECASE_STATUS:
                resp = await self._handle_project_usecase_reply(
                    wa_user_id,
                    message,
                    reply_to_message_id=reply_to_message_id,
                )
                if resp:
                    return resp

            # --- 5.6 CONTEXT GLUE: interpret bare number replies deterministically ---
            # If we previously asked for volume (m³), treat "20" as 20 m³ (not grade G20).
            try:
                expected = self.session_helper.consume_expected_reply(wa_user_id)
            except Exception:
                expected = None
            if expected == "volume_m3":
                m = _BARE_NUMBER_RE.match(message or "")
                if m:
                    message = f"I need {m.group(1)} m³ of ready-mix concrete."
            elif expected == "area":
                m = _BARE_NUMBER_RE.match(message or "")
                if m:
                    message = f"The total built-up area is {m.group(1)} square feet."

            if self._is_product_overview_intent(message):
                # Ask the project question first (best UX).
                try:
                    self.session_helper.set_onboarding_status(
                        wa_user_id,
                        PROJECT_USECASE_STATUS,
                        reason="product_overview",
                    )
                except Exception:
                    pass
                menu = self._project_usecase_menu_text()
                self._send_text_once(wa_user_id, menu, reply_to_message_id=reply_to_message_id)
                return menu

            # --- 6. SESSION FETCH/CREATE ---
            session_id = self._get_cached_session_id(wa_user_id)
            session_created_this_turn = False
            if not session_id:
                session_created_this_turn = True
                external_user_id = self.get_external_user_id(wa_user_id)
                self.create_user_document(wa_user_id)
                customer_meta = await asyncio.to_thread(self._ensure_customer_metadata, wa_user_id)
                state = {"user_id": wa_user_id, "wa_user_id": wa_user_id}
                if external_user_id:
                    state["external_user_id"] = external_user_id
                state.update({k: v for k, v in customer_meta.items() if v})
                new_session = await _vertex_op_with_retry(
                    lambda: self.session_service.create_session(
                        app_name=self.APP_NAME, user_id=wa_user_id, state=state
                    ),
                    label="session.create",
                )
                session_id = new_session.id
                self._update_cached_session_id(wa_user_id, session_id)

            try:
                if conversation_id:
                    self.session_helper.append_session_to_conversation(wa_user_id, session_id)
            except Exception:
                pass

            # --- 7. GOODBYE / CATALOG ---
            is_goodbye = self.is_goodbye_message(message)

            if is_goodbye or disable_catalog:
                logger.info(
                    "catalog.not_sent",
                    user_id=wa_user_id,
                    reason="goodbye_or_disabled",
                    detail="Catalog skipped: user said goodbye or catalog was disabled for this request.",
                    is_goodbye=is_goodbye,
                    disable_catalog=disable_catalog,
                )
            else:
                try:
                    await self._maybe_send_catalog(
                        wa_user_id,
                        session_id,
                        message_text=message,
                        conversation_id=conversation_id or "",
                        conv_is_new=conv_is_new,
                        session_created_this_turn=session_created_this_turn,
                    )
                except Exception as e:
                    logger.warning(
                        "catalog.not_sent",
                        user_id=wa_user_id,
                        reason="maybe_send_exception",
                        detail=f"_maybe_send_catalog raised: {e!s}",
                        error=str(e),
                    )

            if is_goodbye:
                response = "Goodbye! If you need anything else, just message me."
                self._send_text_once(wa_user_id, response, reply_to_message_id=reply_to_message_id)
                self.session_helper.end_session(wa_user_id, reason="goodbye")
                await self.save_and_create_new_session(wa_user_id, session_id)
                return response

            # --- 8. CALL AGENT (with 1 retry on empty) ---
            agent_response = await self._call_agent_text_only(message, wa_user_id, session_id)
            if not agent_response or agent_response == CUSTOMER_SAFE_FALLBACK_TEXT:
                logger.warning("agent.empty_response.retrying", user_id=wa_user_id, session_id=session_id)
                await asyncio.sleep(1)
                agent_response = await self._call_agent_text_only(message, wa_user_id, session_id)
            if not agent_response:
                raise ValueError("Agent response is empty after retry.")

            agent_response = await self._send_text_then_optional_vn_then_finalize(
                wa_user_id,
                agent_response,
                is_voice_input=is_voice_input,
                reply_to_message_id=reply_to_message_id,
                inbound_key=inbound_key,
                session_id_at_start=session_id,
                threshold_hit=False,
                user_utterance_for_summary=message,
                vn_lang_code=vn_lang_code,
            )

            # --- 9. INVOICE / ORDER PLACED ---
            if self._was_order_placed(agent_response, {}):
                logger.info("session.order_placed_rotation", user_id=wa_user_id)
                try:
                    self.session_helper.set_summary(wa_user_id, "Order placed successfully. Last Invoice detected.")
                except Exception:
                    pass
                self.session_helper.end_session(wa_user_id, reason="invoice_generated")
                await self.save_and_create_new_session(wa_user_id, session_id)

            return agent_response

        except Exception as e:
            # NOTE: exc_info=True so we get a full stack trace in Cloud Logging
            msg = str(e)

            # If the process is already shutting down its async executor, avoid
            # noisy fallback messages and just exit quietly for this turn.
            if isinstance(e, RuntimeError) and "cannot schedule new futures after shutdown" in msg:
                logger.warning(
                    "handle_message_async.shutdown_in_progress",
                    error=msg,
                    user_id=user_id,
                    exc_info=True,
                )
                return ""

            logger.error(
                "handle_message_async.error",
                error=msg,
                user_id=user_id,
                exc_info=True,
            )
            err = CUSTOMER_SAFE_FALLBACK_TEXT
            self._send_text_once(user_id, err, reply_to_message_id=reply_to_message_id)
            return err
        finally:
            if lock_acquired:
                try:
                    self.session_helper.release_message_lock(wa_user_id)
                except Exception:
                    pass

    async def _send_greeting_gif(self, user_id: str) -> bool:
        """
        Upload and send the local greeting GIF/MP4 (set via GREETING_GIF_PATH env var)
        as a WhatsApp video message right after the greeting VN.
        """
        
        gif_path = os.getenv("GREETING_GIF_PATH", "media/WhatsApp Video 2026-02-18 at 12.19.30 PM.mp4").strip()
        if not gif_path or not os.path.isfile(gif_path):
            logger.warning("greeting_gif.skip", reason="GREETING_GIF_PATH not set or file missing", path=gif_path)
            return False

        try:
            import aiohttp

            with open(gif_path, "rb") as f:
                gif_bytes = f.read()

            filename = os.path.basename(gif_path)
            content_type = "video/mp4"

            headers = {"Authorization": f"Bearer {self.whatsapp_token}"}

            async with aiohttp.ClientSession() as session:
                data = aiohttp.FormData()
                data.add_field("file", gif_bytes, filename=filename, content_type=content_type)
                data.add_field("messaging_product", "whatsapp")

                async with session.post(
                    self.whatsapp_media_url, headers=headers, data=data, timeout=60
                ) as resp:
                    if resp.status not in (200, 201):
                        txt = await resp.text()
                        logger.error("greeting_gif.upload_failed", status=resp.status, body=txt[:300])
                        return False
                    js = await resp.json()
                    media_id = js.get("id")

            if not media_id:
                logger.error("greeting_gif.no_media_id")
                return False

            payload = {
                "messaging_product": "whatsapp",
                "to": user_id,
                "type": "video",
                "video": {"id": media_id},
            }

            r = self.http_session.post(self.whatsapp_api_url, json=payload, timeout=20)
            if r.status_code in (200, 201):
                logger.info("greeting_gif.sent", user_id=user_id, filename=filename)
                return True

            logger.error("greeting_gif.send_failed", status=r.status_code, body=r.text[:300])
            return False

        except Exception as e:
            logger.error("greeting_gif.exception", user_id=user_id, error=str(e), exc_info=True)
            return False

    async def _send_sample_invoice_image(self, user_id: str, reply_to_message_id: Optional[str] = None) -> bool:
        """
        Upload and send a sample invoice image to guide the user during onboarding.
        """
        img_path = os.getenv("SAMPLE_INVOICE_IMAGE_PATH", "media/sample_invoice.jpg").strip()
        if not img_path or not os.path.isfile(img_path):
            logger.warning("sample_invoice_img.skip", reason="SAMPLE_INVOICE_IMAGE_PATH not set or missing", path=img_path)
            return False

        try:
            import aiohttp

            with open(img_path, "rb") as f:
                img_bytes = f.read()

            filename = os.path.basename(img_path)
            content_type = "image/png" if filename.lower().endswith(".png") else "image/jpeg"
            headers = {"Authorization": f"Bearer {self.whatsapp_token}"}

            async with aiohttp.ClientSession() as session:
                data = aiohttp.FormData()
                data.add_field("file", img_bytes, filename=filename, content_type=content_type)
                data.add_field("messaging_product", "whatsapp")

                async with session.post(
                    self.whatsapp_media_url, headers=headers, data=data, timeout=60
                ) as resp:
                    if resp.status not in (200, 201):
                        txt = await resp.text()
                        logger.error("sample_invoice_img.upload_failed", status=resp.status, body=txt[:300])
                        return False
                    js = await resp.json()
                    media_id = js.get("id")

            if not media_id:
                logger.error("sample_invoice_img.no_media_id")
                return False

            payload = {
                "messaging_product": "whatsapp",
                "to": user_id,
                "type": "image",
                "image": {"id": media_id},
            }
            if reply_to_message_id:
                payload["context"] = {"message_id": reply_to_message_id}

            r = self.http_session.post(self.whatsapp_api_url, json=payload, timeout=20)
            if r.status_code in (200, 201):
                logger.info("sample_invoice_img.sent", user_id=user_id, filename=filename)
                return True

            logger.error("sample_invoice_img.send_failed", status=r.status_code, body=r.text[:300])
            return False

        except Exception as e:
            logger.error("sample_invoice_img.exception", user_id=user_id, error=str(e), exc_info=True)
            return False

    async def _gen_vn_else_text(self, to_number: str, text: str, *, vn_text: Optional[str], inbound_key: Optional[str], reply_to_message_id: Optional[str], vn_lang_code: Optional[str] = None):
        try:
            if not self._outbox_try_start(to_number, inbound_key):
                logger.info("outbox.already_in_progress", user_id=to_number)
                return

            # Resolve effective language for this VN
            regional_enabled = os.getenv("VN_REGIONAL_LANG_ENABLED", "false").lower() == "true"
            effective_lang = (vn_lang_code or "en") if regional_enabled else "en"
            logger.info(
                "vn.lang.resolved",
                user_id=to_number,
                detected=vn_lang_code,
                effective=effective_lang,
                regional_enabled=regional_enabled,
            )
            tts_text = await self._prepare_vn_text(vn_text or text, lang_code=effective_lang)

            t0 = time.perf_counter()
            preferred_bytes, meta, mp3_bytes = await self._tts_get_audio_bytes(tts_text, lang_code=effective_lang)

            elapsed = time.perf_counter() - t0
            logger.info(f"TTS took {elapsed:.2f}s", user_id=to_number, meta=meta)

            # Optional: trim trailing silence to avoid long empty tails
            # try:
            #     if preferred_bytes:
            #         preferred_bytes = trim_trailing_silence(
            #             preferred_bytes,
            #             mime=(meta or {}).get("mime"),
            #             max_silence_sec=float(os.getenv("VN_TRIM_TAIL_SEC", "1.5")),
            #             threshold_db=float(os.getenv("VN_TRIM_THRESHOLD_DB", "-40")),
            #         )
            #     if mp3_bytes:
            #         mp3_bytes = trim_trailing_silence(
            #             mp3_bytes,
            #             mime="audio/mpeg",
            #             max_silence_sec=float(os.getenv("VN_TRIM_TAIL_SEC", "1.5")),
            #             threshold_db=float(os.getenv("VN_TRIM_THRESHOLD_DB", "-40")),
            #         )
            # except Exception as e:
            #     logger.warning(f"vn.trim_silence.error: {e}")

            # prefer the returned preferred_bytes (could be OGG or MP3 depending on meta)
            if preferred_bytes and not is_audio_too_small(len(tts_text), preferred_bytes, kbps=8):
                sent = await self._upload_and_send_audio(
                    to_number,
                    preferred_bytes,
                    voice=True,   # VN if Opus
                    mp3=False,
                    meta=meta,
                    reply_to_message_id=reply_to_message_id,
                    inbound_key=inbound_key,
                )

                if sent:
                    logger.info("vn.sent.ok (preferred)", user_id=to_number, latency_sec=elapsed)
                    try:
                        chars = len(tts_text)
                        eleven_tts_usage = {
                            "enabled": True,
                            "model": self.eleven_model_id,
                            "voice_id": self.eleven_voice_id,
                            "request_id": "",
                            "latency_ms": int(elapsed * 1000),
                            "input_characters": chars,
                            "pricing": {
                                "unit": "characters",
                                "price_per_characters_usd": 0.0001,
                            },
                        }
                        self._last_eleven_tts_usage[to_number] = eleven_tts_usage
                    except Exception:
                        pass
                    self._outbox_mark_sent(to_number, inbound_key)
                    # schedule pending end after successful VN
                    try:
                        self.session_helper.request_end(
                            to_number,
                            delay_sec=int(os.getenv("SESSION_PENDING_AFTER_AGENT_SEC", str(15*60))),
                            reason="agent_last_message",
                            combine="max",
                        )
                    except Exception:
                        pass
                    return

            # try mp3 fallback if present
            if mp3_bytes and not is_audio_too_small(len(tts_text), mp3_bytes, kbps=8):
                sent = await self._upload_and_send_audio(
                    to_number,
                    mp3_bytes,
                    voice=False,
                    mp3=True,
                    meta={"mime": "audio/mpeg", "path": "http_or_stream_mp3"},
                    reply_to_message_id=reply_to_message_id,
                    inbound_key=inbound_key,
                )
                if sent:
                    logger.info("audio.sent.ok (mp3)", user_id=to_number, latency_sec=elapsed)
                    try:
                        chars = len(tts_text)
                        eleven_tts_usage = {
                            "enabled": True,
                            "model": self.eleven_model_id,
                            "voice_id": self.eleven_voice_id,
                            "request_id": "",
                            "latency_ms": int(elapsed * 1000),
                            "input_characters": chars,
                            "pricing": {
                                "unit": "characters",
                                "price_per_characters_usd": 0.0001,
                            },
                        }
                        self._last_eleven_tts_usage[to_number] = eleven_tts_usage
                    except Exception:
                        pass
                    self._outbox_mark_sent(to_number, inbound_key)
                    # schedule pending end after successful audio
                    try:
                        self.session_helper.request_end(
                            to_number,
                            delay_sec=int(os.getenv("SESSION_PENDING_AFTER_AGENT_SEC", str(15*60))),
                            reason="agent_last_message",
                            combine="max",
                        )
                    except Exception:
                        pass
                    return

            # Last-resort: direct MP3 call before giving up to text
            logger.warning("TTS primary+fallback returned nothing, trying direct MP3", user_id=to_number)
            try:
                last_resort_mp3 = await self._direct_mp3_lastresort(tts_text)
                if last_resort_mp3:
                    sent = await self._upload_and_send_audio(
                        to_number,
                        last_resort_mp3,
                        voice=False,
                        mp3=True,
                        meta={"mime": "audio/mpeg", "path": "last_resort_mp3"},
                        reply_to_message_id=reply_to_message_id,
                        inbound_key=inbound_key,
                    )
                    if sent:
                        logger.info("audio.sent.ok (last_resort_mp3)", user_id=to_number)
                        self._outbox_mark_sent(to_number, inbound_key)
                        return
            except Exception as lr_err:
                logger.warning("last_resort_mp3.failed", user_id=to_number, error=str(lr_err))

            logger.warning("TTS failed completely, falling back to text", user_id=to_number)
            self._send_text_once(to_number, text, reply_to_message_id=reply_to_message_id)
            self._outbox_mark_sent(to_number, inbound_key)
        except Exception as e:
            logger.error("vn.error -> text fallback", error=str(e), user_id=to_number)
            self._send_text_once(to_number, text, reply_to_message_id=reply_to_message_id)
            self._outbox_mark_sent(to_number, inbound_key)
        finally:
            self._release_voice_lock(to_number)

    async def _direct_mp3_lastresort(self, text: str) -> Optional[bytes]:
        """
        Standalone last-resort MP3 generation — bypasses TTSGenerator entirely.
        Uses a fresh HTTP call to ElevenLabs with minimal config.
        """
        api_key = os.getenv("ELEVENLABS_API_KEY", "")
        voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")
        if not api_key or not voice_id:
            logger.warning("last_resort_mp3.missing_config", has_key=bool(api_key), has_voice=bool(voice_id))
            return None

        def _call():
            url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
            payload = {
                "text": text[:5000],
                "model_id": os.getenv("ELEVENLABS_MODEL_ID", "eleven_turbo_v2_5"),
                "voice_settings": {
                    "stability": 0.5,
                    "similarity_boost": 0.75,
                    "style": 0.0,
                    "use_speaker_boost": True,
                    "speed": 1.12,
                },
                "output_format": "mp3_22050_32",
            }
            headers = {
                "xi-api-key": api_key,
                "Accept": "audio/mpeg",
                "Content-Type": "application/json",
            }
            import json as _json
            r = requests.post(url, headers=headers, data=_json.dumps(payload), timeout=15)
            r.raise_for_status()
            if r.content and len(r.content) > 500:
                return r.content
            return None

        return await asyncio.to_thread(_call)

    # ---------- VN (or fallback audio/text) ----------
    async def _send_text_then_optional_vn_then_finalize(
        self,
        user_id: str,
        agent_text: Any,
        *,
        is_voice_input: bool,
        reply_to_message_id: Optional[str],
        inbound_key: Optional[str],
        session_id_at_start: str,
        threshold_hit: bool,
        user_utterance_for_summary: str,
        vn_lang_code: Optional[str] = None,
    ):
        """
        Order of ops on rotation turn (every 10 *agent* texts):
        1) send agent TEXT
        2) increment agent text counter and recompute threshold (exactly on multiples of 10)
        3) if voice-input flow: send VN AFTER text (sync only if threshold turn)
        4) update summary (only on threshold turns)
        5) rotate Vertex session (only on threshold turns)
        6) reset the 10-text counter (only on threshold turns)
        """
        def _coerce_messages(raw: Any) -> List[str]:
            if isinstance(raw, str):
                if MULTI_MESSAGE_DELIMITER in raw:
                    return [p.strip() for p in raw.split(MULTI_MESSAGE_DELIMITER) if p and p.strip()]
                return [raw.strip()] if raw.strip() else []
            if isinstance(raw, (list, tuple)):
                return [str(p).strip() for p in raw if isinstance(p, str) and str(p).strip()]
            if isinstance(raw, dict):
                candidate = raw.get("messages") or raw.get("parts")
                if isinstance(candidate, (list, tuple)):
                    return [str(p).strip() for p in candidate if isinstance(p, str) and str(p).strip()]
                if isinstance(raw.get("text"), str) and raw.get("text").strip():
                    return [raw.get("text").strip()]
            if raw:
                return [str(raw).strip()]
            return []

        messages = _coerce_messages(agent_text)
        # Detect [SEND_LOCATION_PIN] tag before stripping — triggers location button
        _raw_text = " ".join(messages)
        _needs_location_pin = bool(re.search(r"\[SEND_LOCATION_PIN\]", _raw_text))

        # If agent asks for built-up area, set expectation so a bare number reply is interpreted as area
        if re.search(r"built.?up area|total area|floor area|project area|area .{0,20}(sq|square)", _raw_text, re.IGNORECASE):
            try:
                self.session_helper.set_expected_reply(user_id, "area")
            except Exception:
                pass
        # Strip the internal tag so the user never sees it
        messages = [re.sub(r"\s*\[SEND_LOCATION_PIN\]\s*", "", m).strip() for m in messages]
        messages = [m for m in messages if m]
        combined_text = "\n\n".join(messages).strip() if messages else (agent_text.strip() if isinstance(agent_text, str) else "")

        # 1) send agent TEXT (must be first; support multiple parts)
        sent_count = 0
        for idx, msg in enumerate(messages or ([combined_text] if combined_text else [])):
            if not msg:
                continue
            sent = self._send_text_once(
                user_id,
                msg,
                reply_to_message_id=reply_to_message_id if idx == 0 else None,
                inbound_key=inbound_key,
            )
            if sent:
                sent_count += 1
        if sent_count == 0:
            # do not count failed sends; do not VN/rotate
            logger.warning(
                "agent.text_send.skipped_all",
                user_id=user_id,
                inbound_key=inbound_key,
                part_count=len(messages or ([combined_text] if combined_text else [])),
            )
            return combined_text or agent_text
        # If agent included [SEND_LOCATION_PIN], fire the interactive location button now
        if _needs_location_pin:
            try:
                tenant = (os.getenv("TENANT_ID") or "").strip().lower()
                if tenant == "ytl":
                    _phone_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID") or ""
                    _wa_token = os.getenv("WHATSAPP_ACCESS_TOKEN") or ""
                    _wa_url = f"https://graph.facebook.com/v23.0/{_phone_id}/messages" if _phone_id else ""
                    if _wa_url and _wa_token:
                        _loc_payload = {
                            "messaging_product": "whatsapp",
                            "to": user_id,
                            "type": "interactive",
                            "interactive": {
                                "type": "location_request_message",
                                "body": {"text": "Tap the button to share your site location."},
                                "action": {"name": "send_location"},
                            },
                        }
                        _loc_resp = requests.post(
                            _wa_url,
                            headers={"Authorization": f"Bearer {_wa_token}", "Content-Type": "application/json"},
                            json=_loc_payload,
                            timeout=15,
                        )
                        if 200 <= _loc_resp.status_code < 300:
                            logger.info("wa.location_pin.sent_via_tag", user_id=user_id)
                            try:
                                self.session_helper.mark_location_request_sent(user_id)
                            except Exception:
                                pass
                        else:
                            logger.warning("wa.location_pin.fail", user_id=user_id, status=_loc_resp.status_code, body=_loc_resp.text[:200])
                    else:
                        logger.warning("wa.location_pin.skip_no_creds", user_id=user_id, have_phone_id=bool(_phone_id), have_token=bool(_wa_token))
            except Exception as e:
                logger.warning("wa.location_pin.tag_trigger_error", user_id=user_id, error=str(e))

        # Prepare VN script separately so we never mutate the text reply
        vn_script = combined_text or agent_text

        # Persist explicit order-confirmation prompts sent by the assistant so
        # next-turn "yes/confirm" replies resolve against the pending draft.
        try:
            self.remember_order_confirmation_context(
                user_id,
                messages if messages else ([combined_text] if combined_text else []),
                source="agent_response",
            )
        except Exception as e:
            logger.warning("order_confirm.context.capture_failed", user_id=user_id, error=str(e))

        # 2) Count **agent** text now that it actually sent, then recompute threshold here
        try:
            new_count = self.session_helper.inc_text_count(user_id, sent_count)
        except Exception:
            new_count = None
        # Recompute threshold: rotate exactly on 10, 20, 30, ...
        threshold_hit = (isinstance(new_count, int) and new_count % 10 == 0)

        # 3) if this turn originated from VOICE input, send VN AFTER text
        if is_voice_input:
            if threshold_hit:
                # synchronous VN to guarantee "text -> VN -> finalize"
                try:
                    if self._acquire_voice_lock(user_id):
                        await self._gen_vn_else_text(
                            user_id,
                            vn_script,
                            vn_text=vn_script,
                            inbound_key=inbound_key,
                            reply_to_message_id=reply_to_message_id,
                            vn_lang_code=vn_lang_code,
                        )
                finally:
                    self._release_voice_lock(user_id)
            else:
                # non-threshold: fire-and-forget VN path
                self._spawn_vn_or_text(
                    user_id,
                    vn_script,
                    vn_text=vn_script,
                    inbound_key=inbound_key,
                    reply_to_message_id=reply_to_message_id,
                    vn_lang_code=vn_lang_code,
                )

        # 4–6) finalize lifecycle if we just hit the 10th **agent** text
        if threshold_hit:
            # 4) summary (sync on this last turn)
            try:
                prior = self.session_helper.get_summary(user_id)
            except Exception:
                prior = ""
            try:
                updated = await self._summarize_turn(prior, user_utterance_for_summary, vn_script)
                if updated and updated != prior:
                    self.session_helper.set_summary(user_id, updated[:4000])
            except Exception:
                pass

            # 5) rotate session
            try:
                await self.save_and_create_new_session(user_id, session_id_at_start)
            except Exception:
                pass

            # 6) reset the 10-text window
            try:
                self.session_helper.reset_text_count(user_id)
            except Exception:
                pass

        # agent activity touch (doesn't extend inactivity)
        self.session_helper.touch(user_id, source="agent")

        # NEW: also touch conversation-level activity
        try:
            self.session_helper.touch_conversation(user_id, source="agent")
        except Exception:
            pass

        return vn_script


    def _spawn_vn_or_text(self, user_id: str, text: str, *, vn_text: Optional[str], inbound_key: Optional[str], reply_to_message_id: Optional[str], vn_lang_code: Optional[str] = None):
        def _runner():
            try:
                asyncio.run(
                    self._gen_vn_else_text(
                        user_id,
                        text,
                        vn_text=vn_text,
                        inbound_key=inbound_key,
                        reply_to_message_id=reply_to_message_id,
                        vn_lang_code=vn_lang_code,
                    )
                )
            except Exception as e:
                logger.error("vn.text.runner.crash", error=str(e), user_id=user_id)

        if not self._acquire_voice_lock(user_id):
            logger.info("voice.lock.busy.skip", user_id=user_id)
            return

        # daemon=False + join so Cloud Run doesn't kill the thread on scale-down
        t = threading.Thread(target=_runner, daemon=False)
        t.start()
        t.join(timeout=25)
    
    # ---------- TTS ----------
    def _round_rupee_str(self, raw: str) -> tuple[Optional[int], str]:
        """
        Safely round a numeric string to whole rupees.

        - Strips commas (e.g. "12,345.67")
        - Uses Decimal + ROUND_HALF_UP (standard financial rounding)
        - Returns (rounded_int, canonical_str) or (None, original_raw) on failure
        """
        if not raw:
            return None, raw

        value_str = raw.replace(",", "").strip()
        try:
            d = Decimal(value_str)
            rounded = int(d.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            return rounded, str(rounded)
        except (InvalidOperation, ValueError):
            try:
                rounded = int(round(float(value_str)))
                return rounded, str(rounded)
            except Exception:
                return None, raw

    async def _maybe_send_catalog(
        self,
        user_id: str,
        session_id: str,
        message_text: str,
        conversation_id: str,
        conv_is_new: bool,
        session_created_this_turn: bool = False,
    ) -> None:
        """
        Decide whether to send catalog, purely hardcoded (no LLM / tools):

        1) If user explicitly asks for catalog/list/rates:
        → ALWAYS send (ignores conv_is_new + cooldown).

        2) Otherwise, only auto-send when:
        - (This is a NEW conversation (conv_is_new == True) OR a new session was created this turn), AND
        - Catalog was not sent too recently (cooldown).

        Applies to BOTH text and voice (since voice is already transcribed).
        """
        logger.info(
            "catalog.maybe_send.entry",
            user_id=user_id,
            conv_is_new=conv_is_new,
            session_created_this_turn=session_created_this_turn,
            has_session_id=bool(session_id),
            conversation_id=conversation_id or "",
        )
        if not session_id:
            logger.info(
                "catalog.not_sent",
                user_id=user_id,
                reason="no_session_id",
                detail="Session ID is missing; catalog send skipped.",
            )
            return

        text = (message_text or "").strip()
        
        # Helper: spawn greeting VN in a non-daemon thread so Cloud Run doesn't kill it on scale-down.
        # Returns the thread so caller can join(timeout) before request returns.
        def _spawn_greeting_vn_thread():
            import threading
            def _runner():
                try:
                    asyncio.run(self.greeting_vn_cache.send_greeting_vn(user_id))
                except asyncio.CancelledError as e:
                    logger.warning("greeting_vn_bg_thread.cancelled", user_id=user_id, error=str(e))
                except Exception as e:
                    logger.error("greeting_vn_bg_thread.error", user_id=user_id, error=str(e))
            t = threading.Thread(target=_runner, daemon=False)
            t.start()
            return t

        # ------------------------------------------------------------------
        # 1) Explicit "send me catalog" intent → always send
        # ------------------------------------------------------------------
        if self._explicit_catalog_intent(text):
            logger.info(
                "catalog.explicit_intent",
                user_id=user_id,
                session_id=session_id,
            )
            t = None
            try:
                await asyncio.to_thread(send_product_catalogue, user_id, session_id)
                logger.info(
                    "catalog.sent",
                    user_id=user_id,
                    reason="explicit_intent",
                    session_id=session_id,
                )
                # Just bookkeeping; DOES NOT block future explicit requests
                try:
                    self.session_helper.mark_catalog_sent(user_id, session_id)
                    
                except TypeError:
                    # In case SessionStore has older signature, fail soft
                    logger.warning(
                        "catalog.explicit.mark_failed_signature",
                        user_id=user_id,
                        session_id=session_id,
                    )
                except Exception as e:
                    logger.warning(
                        "catalog.explicit.mark_failed",
                        user_id=user_id,
                        session_id=session_id,
                        error=str(e),
                    )
            except Exception as e:
                logger.warning(
                    "catalog.not_sent",
                    user_id=user_id,
                    reason="explicit_send_failed",
                    detail=f"User asked for catalog but send failed: {e!s}",
                    session_id=session_id,
                    error=str(e),
                )
            finally:
                # Greeting VN fires regardless of whether catalog succeeded or failed.
                try:
                    t = _spawn_greeting_vn_thread()
                    if t:
                        await asyncio.to_thread(t.join, 25)
                except Exception as e:
                    logger.warning("catalog.explicit.vn_failed", user_id=user_id, error=str(e))
            return

        # Single-use suppression flag (set after invoice verification)
        try:
            suppressed = self.session_helper.consume_catalog_autosend_suppressed(user_id)
        except Exception:
            suppressed = False
        if suppressed:
            logger.info(
                "catalog.not_sent",
                user_id=user_id,
                reason="autosend_suppressed",
                detail="Catalog autosend was suppressed (e.g. after invoice verification).",
                conversation_id=conversation_id,
            )
            return

        # ------------------------------------------------------------------
        # 2) AUTO-SEND on first message of a conversation OR when a new session started this turn
        #    (e.g. user returned after order/session end, or first message ever)
        # ------------------------------------------------------------------

        if not conv_is_new and not session_created_this_turn:
            logger.info(
                "catalog.not_sent",
                user_id=user_id,
                reason="not_new_conversation_or_session",
                detail="Neither new conversation nor new session this turn; autosend only on first message or new session.",
                conv_is_new=conv_is_new,
                session_created_this_turn=session_created_this_turn,
                conversation_id=conversation_id,
            )
            return

        # Cooldown (default 24h) so we don't auto-spam across conversations
        try:
            cooldown = int(os.getenv("CATALOG_COOLDOWN_SEC", str(24 * 60 * 60)))
        except Exception:
            cooldown = 24 * 60 * 60

        last_ts = self.session_helper.get_last_catalog_sent_at(user_id)
        now = time.time()

        if last_ts and (now - last_ts) < max(0, cooldown):
            seconds_since = round(now - last_ts, 1)
            logger.info(
                "catalog.not_sent",
                user_id=user_id,
                reason="cooldown",
                detail=f"Catalog was sent {seconds_since}s ago; cooldown is {cooldown}s.",
                conversation_id=conversation_id,
                seconds_since_last=seconds_since,
                cooldown_sec=cooldown,
            )
            # Greeting VN still fires on new conversation even when catalog is on cooldown
            t = _spawn_greeting_vn_thread()
            if t:
                await asyncio.to_thread(t.join, 25)
            return

        # Actually auto-send catalog
        logger.info(
            "catalog.autosend.start",
            user_id=user_id,
            session_id=session_id,
            conversation_id=conversation_id,
        )
        t = None
        try:
            await asyncio.to_thread(send_product_catalogue, user_id, session_id)
            try:
                self.session_helper.mark_catalog_sent(user_id, session_id)
                
            except TypeError:
                logger.warning(
                    "catalog.autosend.mark_failed_signature",
                    user_id=user_id,
                    session_id=session_id,
                )
            except Exception as e:
                logger.warning(
                    "catalog.autosend.mark_failed",
                    user_id=user_id,
                    session_id=session_id,
                    error=str(e),
                )

            logger.info(
                "catalog.sent",
                user_id=user_id,
                reason="autosend",
                session_id=session_id,
                conversation_id=conversation_id,
            )
        except Exception as e:
            logger.warning(
                "catalog.not_sent",
                user_id=user_id,
                reason="send_failed",
                detail=f"Catalog send raised: {e!s}",
                session_id=session_id,
                conversation_id=conversation_id,
                error=str(e),
            )
        finally:
            # Greeting VN fires regardless of whether catalog succeeded or failed.
            try:
                t = _spawn_greeting_vn_thread()
                if t:
                    await asyncio.to_thread(t.join, 25)
            except Exception as e:
                logger.warning("catalog.autosend.vn_failed", user_id=user_id, error=str(e))
       
    async def _upload_and_send_audio(
        self,
        to_number: str,
        audio_bytes: bytes,
        *,
        voice: bool,
        mp3: bool = False,
        meta: Optional[dict] = None,
        reply_to_message_id: Optional[str] = None,
        inbound_key: Optional[str] = None,
    ) -> bool:
        """
        Uploads file to WhatsApp media endpoint and sends message.
        Uses meta['mime'] first (if present) to determine whether to set voice/PTT.
        If meta not provided or ambiguous, falls back to byte-sniffing.

        If reply_to_message_id is provided, the outbound message will appear
        as a quoted reply to that WAMID in the WhatsApp UI.
        """
        # YTL Cement: allow toggling audio via env. By default audio is enabled;
        # set YTL_AUDIO_ENABLED=false to disable uploads without code changes.
        if (self.tenant_id or "").lower() == "ytl":
            audio_enabled = os.getenv("YTL_AUDIO_ENABLED", "true").lower() in ("1", "true", "yes")
            if not audio_enabled:
                logger.info("audio.upload.disabled_ytl", to=to_number)
                return False

        if self.is_twilio:
            return await asyncio.to_thread(
                self._upload_and_send_audio_twilio,
                to_number,
                audio_bytes,
                voice=voice,
                mp3=mp3,
                meta=meta,
                reply_to_message_id=reply_to_message_id,
                inbound_key=inbound_key,
            )

        tmp_suffix = ".mp3" if mp3 else ".ogg"
        # meta may contain 'mime' or 'path'
        mime_from_meta = None
        if meta and isinstance(meta, dict):
            mime_from_meta = meta.get("mime")

        # decide mime to upload
        if mime_from_meta:
            ctype = mime_from_meta
        else:
            # fallback to sniffing bytes
            sniff = sniff_audio_mime(audio_bytes)
            ctype = "audio/mpeg" if "mpeg" in sniff else "audio/ogg; codecs=opus"
            # force mp3 extension if sniff says mpeg
            if "mpeg" in sniff:
                tmp_suffix = ".mp3"
                mp3 = True

        # ensure PTT/voice only if the mime indicates OGG/Opus
        voice_flag = False
        if mime_from_meta:
            if "ogg" in (mime_from_meta or "") or "opus" in (mime_from_meta or ""):
                voice_flag = True
            else:
                voice_flag = False
        else:
            if "ogg" in ctype or "opus" in ctype:
                voice_flag = True
            else:
                voice_flag = False

        # normalized content type for upload (WhatsApp expects common types)
        upload_content_type = "audio/mpeg" if mp3 or "mpeg" in ctype else "audio/ogg"

        try:
            import aiohttp
            headers = {"Authorization": f"Bearer {self.whatsapp_token}"}
            with tempfile.NamedTemporaryFile(delete=False, suffix=tmp_suffix, prefix="audio-") as f:
                f.write(audio_bytes)
                path = f.name

            try:
                async with aiohttp.ClientSession() as session:
                    with open(path, "rb") as fh:
                        data = aiohttp.FormData()
                        data.add_field("file", fh, filename=os.path.basename(path), content_type=upload_content_type)
                        data.add_field("messaging_product", "whatsapp")
                        async with session.post(self.whatsapp_media_url, headers=headers, data=data, timeout=90) as resp:
                            if resp.status not in (200, 201):
                                txt = await resp.text()
                                logger.error(f"Audio upload failed {resp.status}: {txt}")
                                return False
                            js = await resp.json()
                            media_id = js.get("id")

                payload = {
                    "messaging_product": "whatsapp",
                    "to": to_number,
                    "type": "audio",
                    "audio": {"id": media_id},
                }
                if voice_flag:
                    payload["audio"]["voice"] = True

                if reply_to_message_id:
                    payload["context"] = {"message_id": reply_to_message_id}

                r = self.http_session.post(self.whatsapp_api_url, json=payload, timeout=20)
                if r.status_code in (200, 201):
                    logger.info("wa.audio.sent", to=to_number, mode=("voice" if voice_flag else "audio"))
                    return True
                logger.error(f"WA audio send failed {r.status_code}: {r.text}")
                return False
            finally:
                try:
                    os.unlink(path)
                except Exception:
                    pass
        except Exception as e:
            logger.error(f"Upload/send audio exception: {e}")
            return False
    
    # def _upload_and_send_audio_twilio(
    #     self,
    #     to_number: str,
    #     audio_bytes: bytes,
    #     @*,
    #     voice: bool,
    #     mp3: bool = False,
    #     meta: Optional[dict] = None,
    #     reply_to_message_id: Optional[str] = None,
    #     inbound_key: Optional[str] = None,
    # ) -> bool:
    #     """
    #     Twilio path: upload bytes to S3 (presigned URL) and send via Twilio REST API.
    #     """
    #     if not (self.twilio_account_sid and self.twilio_auth_token and self.twilio_from_number):
    #         logger.error("twilio.audio.missing_creds")
    #         return False
    #     if not audio_bytes:
    #         logger.error("twilio.audio.empty_payload")
    #         return False

    #     mime_from_meta = None
    #     if meta and isinstance(meta, dict):
    #         mime_from_meta = meta.get("mime")

    #     sniff = _sniff_audio_mime(audio_bytes)
    #     use_mpeg = bool(
    #         mp3
    #         or ("mpeg" in (mime_from_meta or "").lower())
    #         or ("mpeg" in (sniff or "").lower())
    #     )
    #     # Align with Meta upload: prefer plain audio/ogg for WhatsApp voice notes.
    #     content_type = "audio/mpeg" if use_mpeg else "audio/ogg"

    #     user_phone = self._twilio_format_number(to_number).replace("whatsapp:", "").lstrip("+")
    #     conversation_id = None
    #     try:
    #         conversation_id = self.session_helper.get_active_conversation_id(user_phone)
    #     except Exception:
    #         conversation_id = None
    #     if not conversation_id:
    #         conversation_id = f"{user_phone}-{int(time.time())}"

    #     msg_id = inbound_key or reply_to_message_id or f"msg-{int(time.time())}"
    #     if "::" in msg_id:
    #         msg_id = msg_id.split("::", 1)[0]

    #     media_url = self._twilio_upload_media_bytes(
    #         audio_bytes,
    #         content_type,
    #         user_phone=user_phone,
    #         conversation_id=conversation_id,
    #         msg_id=msg_id,
    #     )
    #     if not media_url:
    #         logger.error("twilio.audio.upload_failed")
    #         return False

    #     # Twilio does not support voice/PTT flag; audio will appear as a file attachment.
    #     sent = self._twilio_send_media(
    #         to_number,
    #         media_url,
    #         body="",
    #         content_type=content_type,
    #     )
    #     return sent

    def _chunk_text(self, text: str, limit: int) -> List[str]:
        """
        Split a text into limit-safe chunks favoring sentence/newline boundaries.
        Keeps SKU lines intact; only falls back to word/limit slicing if a single unit
        is longer than the limit.
        """
        if not text or limit <= 0:
            return []

        if len(text) <= limit:
            return [text]

        def split_units(s: str) -> List[str]:
            """
            Break text on sentence endings or newlines while keeping delimiters attached.
            """
            units: List[str] = []
            start = 0
            for m in re.finditer(r'(?<=[.!?])\s+|\n', s):
                end = m.end()
                units.append(s[start:end])
                start = end
            if start < len(s):
                units.append(s[start:])
            return units

        def split_long_unit(unit: str) -> List[str]:
            """
            Split a single over-limit unit on the nearest space; if none, hard-slice.
            """
            pieces: List[str] = []
            text_left = unit
            while len(text_left) > limit:
                cut = text_left.rfind(" ", 0, limit)
                if cut == -1:
                    cut = limit
                pieces.append(text_left[:cut].rstrip())
                text_left = text_left[cut:].lstrip()
            if text_left:
                pieces.append(text_left)
            return pieces

        chunks: List[str] = []
        current = ""

        for unit in split_units(text):
            candidate = unit if not current else f"{current}{unit}"

            if len(candidate) <= limit:
                current = candidate
                continue

            if current:
                chunks.append(current.rstrip())
                current = ""

            if len(unit) > limit:
                for piece in split_long_unit(unit):
                    if len(piece) <= limit:
                        if current and len(current) + len(piece) + 1 <= limit:
                            current = f"{current} {piece}"
                        elif current:
                            chunks.append(current.rstrip())
                            current = piece
                        else:
                            current = piece
                    else:
                        chunks.append(piece)
                        current = ""
                continue

            current = unit

        if current:
            chunks.append(current.rstrip())

        return chunks

    def _chunk_whatsapp_text(self, text: str) -> List[str]:
        raw_limit = max(1, int(self.whatsapp_text_limit or 4096))
        limit = min(raw_limit, 4000)  # preempt the 4096 cap with a safer ceiling
        return self._chunk_text(text, limit)

    def _chunk_twilio_text(self, text: str) -> List[str]:
        raw_limit = max(1, int(self.twilio_text_limit or 1600))
        limit = min(raw_limit, 1600)
        return self._chunk_text(text, limit)

    # ---------- Twilio helpers ----------
    def _twilio_format_number(self, number: str) -> str:
        """
        Normalize phone number for Twilio WhatsApp (adds whatsapp:+ prefix).
        """
        if not number:
            return ""
        n = str(number)
        n = n.replace("whatsapp:", "").strip()
        if not n.startswith("+"):
            n = f"+{n}"
        return f"whatsapp:{n}"

    def send_typing_indicator(self, message_sid: str) -> bool:
        """
        Trigger Twilio WhatsApp typing indicator for an inbound MessageSid.
        """
        if not message_sid:
            logger.warning("twilio.typing.skip", reason="missing_message_sid")
            return False
        if not (self.twilio_account_sid and self.twilio_auth_token):
            logger.warning(
                "twilio.typing.skip",
                reason="missing_creds",
                have_account_sid=bool(self.twilio_account_sid),
                have_auth_token=bool(self.twilio_auth_token),
            )
            return False

        data = {"messageId": message_sid, "channel": "whatsapp"}
        try:
            last_resp = None
            for attempt in range(2):
                resp = requests.post(
                    TWILIO_TYPING_URL,
                    data=data,
                    auth=(self.twilio_account_sid, self.twilio_auth_token),
                    timeout=5,
                )
                last_resp = resp

                ok_status = 200 <= resp.status_code < 300
                success_flag = ok_status
                if ok_status:
                    try:
                        payload = resp.json()
                        if isinstance(payload, dict) and payload.get("success") is False:
                            success_flag = False
                    except Exception:
                        pass

                if ok_status and success_flag:
                    logger.info("twilio.typing.sent", message_sid=message_sid)
                    return True

                body = resp.text[:300] if getattr(resp, "text", "") else ""
                if attempt == 0 and resp.status_code == 400:
                    mdr_hint = "mdr" in (body or "").lower()
                    if not mdr_hint:
                        try:
                            payload = resp.json()
                            msg = str(payload.get("message") or "")
                            mdr_hint = "mdr" in msg.lower()
                        except Exception:
                            pass
                    if mdr_hint:
                        try:
                            time.sleep(0.6)
                        except Exception:
                            pass
                        continue

                logger.warning(
                    "twilio.typing.failed",
                    status=resp.status_code,
                    success=success_flag,
                    body=body,
                    message_sid=message_sid,
                )
                return False

            if last_resp is not None:
                logger.warning(
                    "twilio.typing.failed",
                    status=last_resp.status_code,
                    success=False,
                    body=last_resp.text[:300] if getattr(last_resp, "text", "") else "",
                    message_sid=message_sid,
                )
            return False
        except Exception as e:
            logger.warning("twilio.typing.error", error=str(e), message_sid=message_sid)
            return False

    def _twilio_upload_media_bytes(
        self,
        payload: bytes,
        content_type: str,
        *,
        suffix: Optional[str] = None,
        user_phone: str = "",
        conversation_id: str = "",
        msg_id: str = "",
    ) -> Optional[str]:
        """
        Upload media bytes to S3 (or configured bucket) and return a presigned URL for Twilio to fetch.
        """
        if not payload:
            logger.error("twilio.upload.skip", reason="missing_payload")
            return None
        if not self.twilio_media_bucket:
            logger.error(
                "twilio.upload.skip",
                reason="missing_bucket",
                hint="Set TWILIO_MEDIA_BUCKET or S3_BUCKET_NAME",
            )
            return None

        if not suffix:
            if "mpeg" in (content_type or "") or "mp3" in (content_type or ""):
                suffix = ".mp3"
            elif "wav" in (content_type or ""):
                suffix = ".wav"
            else:
                suffix = ".ogg"

        try:
            from agents.helpers.vn_s3_upload import upload_bytes_to_s3

            _key, url = upload_bytes_to_s3(
                payload,
                content_type=content_type,
                user_phone=user_phone,
                conversation_id=conversation_id,
                msg_id=msg_id,
                key_prefix="twilio-media",
                expires_sec=int(self.twilio_media_expiry or 3600),
            )
            return url
        except Exception as e:
            logger.error("twilio.upload.error", error=str(e))
            return None

    def _twilio_send_text(self, to_number: str, message_body: str) -> bool:
        """
        Send a WhatsApp text via Twilio.
        """
        if not (self.twilio_account_sid and self.twilio_auth_token and self.twilio_from_number):
            logger.error("twilio.send_text.missing_creds")
            return False

        url = f"https://api.twilio.com/2010-04-01/Accounts/{self.twilio_account_sid}/Messages.json"
        data = {
            "To": self._twilio_format_number(to_number),
            "From": self._twilio_format_number(self.twilio_from_number),
            "Body": message_body,
        }
        if self.twilio_status_callback:
            data["StatusCallback"] = self.twilio_status_callback

        try:
            r = requests.post(
                url,
                data=data,
                auth=(self.twilio_account_sid, self.twilio_auth_token),
                timeout=20,
            )
            if 200 <= r.status_code < 300:
                logger.info("twilio.text.sent", to=to_number)
                return True
            logger.error("twilio.text.failed", status=r.status_code, body=r.text[:300])
            return False
        except Exception as e:
            logger.error("twilio.text.exception", error=str(e))
            return False

    def _twilio_send_media(
        self,
        to_number: str,
        media_url: str,
        *,
        body: str = "",
        content_type: str = "",
        return_sid: bool = False,
    ):
        """
        Send media (audio/image) via Twilio using a pre-hosted URL.
        """
        if not (self.twilio_account_sid and self.twilio_auth_token and self.twilio_from_number and media_url):
            logger.error("twilio.send_media.missing_params")
            return (False, None) if return_sid else False

        url = f"https://api.twilio.com/2010-04-01/Accounts/{self.twilio_account_sid}/Messages.json"
        data = {
            "To": self._twilio_format_number(to_number),
            "From": self._twilio_format_number(self.twilio_from_number),
            "MediaUrl": media_url,
        }
        if body:
            data["Body"] = body
        if self.twilio_status_callback:
            data["StatusCallback"] = self.twilio_status_callback

        try:
            r = requests.post(
                url,
                data=data,
                auth=(self.twilio_account_sid, self.twilio_auth_token),
                timeout=30,
            )
            if 200 <= r.status_code < 300:
                sid = None
                try:
                    sid = (r.json() or {}).get("sid")
                except Exception:
                    sid = None
                logger.info("twilio.media.sent", to=to_number, content_type=content_type, sid=sid)
                return (True, sid) if return_sid else True
            logger.error("twilio.media.failed", status=r.status_code, body=r.text[:300])
            return (False, None) if return_sid else False
        except Exception as e:
            logger.error("twilio.media.exception", error=str(e))
            return (False, None) if return_sid else False

    # ---------- Text send (with dedupe) ----------
    def _send_text_once(
        self,
        to_number: str,
        message_body: str,
        reply_to_message_id: Optional[str] = None,
        inbound_key: Optional[str] = None,
    ) -> bool:
        """
        Sends a text message with dedupe.

        FIXES:
        - Dedupe is now inbound-aware: we only skip duplicates within the same inbound_key window.
        - Returns False if ALL chunks were skipped or failed (previously could return True even if nothing sent).

        If reply_to_message_id is provided, first chunk is sent as a quoted reply.
        Also normalizes forbidden wording like 'not found in system'.
        """
        if not message_body:
            return False

        # Clean up phrasing before sending to the user
        message_body = self._clean_system_phrase(message_body)

        if self.is_twilio:
            chunks = self._chunk_twilio_text(message_body)
        else:
            chunks = self._chunk_whatsapp_text(message_body)
        if not chunks:
            return False

        if len(chunks) > 1:
            logger.info(
                "twilio.text.chunked" if self.is_twilio else "wa.text.chunked",
                to=to_number,
                parts=len(chunks),
                orig_len=len(message_body),
            )

        overall_success = True
        sent_any = False

        for idx, chunk in enumerate(chunks):
            # Dedupe (inbound-aware)
            if self.text_dedupe_enabled and not self._ok_to_send_text(to_number, chunk, inbound_key=inbound_key):
                logger.info(
                    "wa.text.skipped_duplicate",
                    to=to_number,
                    part=idx + 1,
                    inbound_key=inbound_key,
                )
                overall_success = False
                continue

            if self.is_twilio:
                sent = self._twilio_send_text(to_number, chunk)
                if sent:
                    logger.info("twilio.text.sent", to=to_number, part=idx + 1, total=len(chunks))
                    sent_any = True
                    self._record_text_hash(to_number, chunk, inbound_key=inbound_key)
                else:
                    overall_success = False
                continue

            if not self.whatsapp_token or not self.phone_number_id:
                logger.error("Missing WhatsApp credentials.")
                return False

            payload = {
                "messaging_product": "whatsapp",
                "to": to_number,
                "type": "text",
                "text": {"body": chunk},
            }

            # Only attach reply context to the first chunk
            if reply_to_message_id and idx == 0:
                payload["context"] = {"message_id": reply_to_message_id}

            try:
                r = self.http_session.post(self.whatsapp_api_url, json=payload, timeout=20)
                if r.status_code in (200, 201):
                    logger.info("wa.text.sent", to=to_number, part=idx + 1, total=len(chunks))
                    sent_any = True
                    self._record_text_hash(to_number, chunk, inbound_key=inbound_key)
                    continue

                overall_success = False
                logger.error(f"WA text send failed {r.status_code} (part {idx + 1}/{len(chunks)}): {r.text}")

            except Exception as e:
                overall_success = False
                logger.error(f"WA text send exception (part {idx + 1}/{len(chunks)}): {e}")

        # If nothing actually sent, report failure
        return sent_any and overall_success


    def _msg_sha(self, text: str) -> str:
        return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

    def _ok_to_send_text(self, user_id: str, text: str, *, inbound_key: Optional[str] = None) -> bool:
        """
        Dedupe decision.

        New behavior:
        - If inbound_key is present, we only skip "duplicate text" if it was already sent
        for the SAME inbound_key within TEXT_DEDUPE_SECONDS.
        - If inbound_key differs, we allow the send even if text matches (prevents silent drops).
        """
        try:
            doc_ref = self._user_root(user_id)
            doc = doc_ref.get()
            if not doc.exists:
                return True

            data = doc.to_dict() or {}
            last_sha = data.get("last_text_sha")
            last_ts = data.get("last_text_epoch")
            last_inbound = data.get("last_text_inbound_key")
            now = time.time()

            # If we have a new inbound message, do NOT block just because text matches
            if inbound_key and last_inbound and inbound_key != last_inbound:
                return True

            # If inbound_key exists but we didn't store one last time, fail open
            if inbound_key and not last_inbound:
                return True

            if last_sha and last_ts:
                if last_sha == self._msg_sha(text) and (now - float(last_ts) < self.text_dedupe_sec):
                    return False

            return True

        except Exception as e:
            logger.warning(f"Text dedupe check failed (will send): {e}")
            return True


    def _record_text_hash(self, user_id: str, text: str, inbound_key: Optional[str] = None):
        try:
            payload = {
                "last_text_sha": self._msg_sha(text),
                "last_text_epoch": time.time(),
            }
            # new: store inbound_key for inbound-aware dedupe
            if inbound_key:
                payload["last_text_inbound_key"] = inbound_key

            self._user_root(user_id).set(payload, merge=True)
        except Exception as e:
            logger.warning(f"Text dedupe record failed: {e}")

    # ---------- Firestore helpers ----------
    def get_external_user_id(self, wa_user_id: str) -> Optional[str]:
        """
        Returns the canonical backend user_id for this WhatsApp user,
        if already mapped in Firestore.
        """
        try:
            doc = self._user_root(wa_user_id).get()
            if not doc.exists:
                return None
            data = doc.to_dict() or {}
            return data.get("external_user_id")
        except Exception as e:
            logger.error(f"get_external_user_id error: {e}")
            return None

    def set_external_user_id(self, wa_user_id: str, external_user_id: str) -> None:
        """
        Persists the canonical backend user_id for this WA user.
        """
        try:
            self._user_root(wa_user_id).set(
                {"external_user_id": external_user_id},
                merge=True,
            )
            logger.info("external_user_id.saved", wa_user_id=wa_user_id, external_user_id=external_user_id)
        except Exception as e:
            logger.error(f"set_external_user_id error: {e}")

    def _get_canonical_user_id_for_state(self, wa_user_id: str) -> str:
        """
        What we inject into the agent as {user_id}.
        Always keep it as the WhatsApp id; external_user_id stays as metadata only.
        """
        return wa_user_id

    
    def create_user_document(self, user_id: str, external_user_id: Optional[str] = None) -> None:
        try:
            doc_ref = self._user_root(user_id)
            if not doc_ref.get().exists:
                payload = {"session_id": None}
                if external_user_id:
                    payload["external_user_id"] = external_user_id
                doc_ref.set(payload)
                logger.info(f"User document created for {user_id}.")
            elif external_user_id:
                # Ensure we don't lose the mapping if doc already existed
                doc_ref.set({"external_user_id": external_user_id}, merge=True)
        except Exception as e:
            logger.error(f"Create user doc error: {e}")

    def maybe_store_whatsapp_profile_name(self, wa_user_id: str, wa_profile_name: str) -> None:
        """
        Best-effort: if the Firestore user doc does NOT exist yet, create it and
        seed customer_name from the WhatsApp Cloud API contact profile name.

        - Only runs when explicitly called (Cloud API webhook path).
        - Safe to call repeatedly; it will no-op once the doc exists.
        """
        try:
            name = (wa_profile_name or "").strip()
            if not name:
                return

            ref = self._user_root(wa_user_id)
            snap = ref.get()
            if snap.exists:
                # Do not override existing docs; name flows are handled elsewhere.
                return

            payload = {
                "session_id": None,
                "customer_name": name,
            }
            ref.set(payload)
            logger.info("whatsapp_name.stored_new_user", user_id=wa_user_id, customer_name=name)
        except Exception as e:
            logger.warning("whatsapp_name.store_failed", user_id=wa_user_id, error=str(e))

    
    async def save_and_create_new_session(self, user_id: str, current_session_id: str) -> Optional[str]:
        """
        This version removes VertexAiMemoryBankService saving.
        We optionally write a compact mem0 session summary, then rotate the session.
        """
        try:
            # concise mem0 summary (best-effort, non-blocking semantics)
            try:
                summary_text = f"Session {current_session_id} concluded for user {user_id}."
                # self.memory.add_session_summary(user_id, summary_text, session_id=current_session_id)
            except Exception:
                pass

            state = {"user_id": user_id, "wa_user_id": user_id}
            try:
                meta = self._get_stored_customer_metadata(user_id)
                if not any(meta.values()):
                    meta = await asyncio.to_thread(self._fetch_customer_metadata, user_id)
                    self._persist_customer_metadata(user_id, meta)
                state.update({k: v for k, v in (meta or {}).items() if v})
            except Exception:
                pass

            
            new_session = await _vertex_op_with_retry(
                lambda: self.session_service.create_session(
                    app_name=self.APP_NAME, user_id=user_id, state=state  # ← correct
                ),
                label="session.create",
            )
            new_id = new_session.id
            self._update_cached_session_id(user_id, new_id)

            # NEW: record this rotated session in the active conversation
            try:
                self.session_helper.append_session_to_conversation(user_id, new_id)
            except Exception as e:
                logger.warning(
                    "conversation.append_session_failed_on_rotate",
                    user_id=user_id,
                    session_id=new_id,
                    error=str(e),
                )

            return new_id
        except Exception as e:
            logger.error(f"Save+rotate session error: {e}")
            return None


    def save_user_location(self, user_id: str, lat: float, lon: float, name: str = "", address: str = "") -> None:
        try:
            self._user_root(user_id).set(
                {
                    "location": {
                        "latitude": lat,
                        "longitude": lon,
                        "name": name,
                        "address": address,
                    },
                    "location_updated_ts": time.time(),
                },
                merge=True
            )
            logger.info("user.location.saved", user_id=user_id, lat=lat, lon=lon)
        except Exception as e:
            logger.warning(f"user.location.save_failed: {e}")

    def _extract_text_from_part(self, part: Any) -> Optional[str]:
        """
        Extract textual payloads from a genai Part or dict, including tool responses.
        
        CRITICAL FIX: We need to distinguish between:
        1. Agent's natural language response (KEEP)
        2. Tool call JSON sent BY agent to tools (SKIP) 
        3. Tool response JSON sent FROM tools to agent (KEEP - agent needs this!)
        """
        
        # First try: direct text attribute (agent's natural language)
        txt = getattr(part, "text", None)
        if not txt and isinstance(part, dict):
            txt = part.get("text")
        
        if isinstance(txt, str) and txt.strip():
            stripped = txt.strip()
            # If it looks like JSON, try to pull human text out (so we don't drop formatted outputs)
            if stripped.startswith("{") and stripped.endswith("}"):
                try:
                    js = json.loads(stripped)
                    # If this was actually a tool call (params), skip
                    if isinstance(js, dict):
                        name = js.get("name") or js.get("tool") or js.get("function")
                        args = js.get("args") or js.get("arguments")
                        if isinstance(name, str) and isinstance(args, dict):
                            if name == "order_draft_template":
                                try:
                                    return order_draft_template(cart=args.get("cart") or args.get("draft") or args)
                                except Exception as e:
                                    logger.warning("malformed_call.render_failed", target=name, error=str(e))
                            elif name == "vn_order_draft_template":
                                try:
                                    return vn_order_draft_template({"draft": args.get("draft") or args.get("cart") or args})
                                except Exception as e:
                                    logger.warning("malformed_call.render_failed", target=name, error=str(e))
                            # Treat other tool call JSON as non-user text
                            logger.debug("_extract_text_from_part: skipping tool call JSON", tool=name)
                            return None

                        keys_lower = " ".join(js.keys()).lower()
                        is_call = any(k in keys_lower for k in ["query", "limit", "user_id", "operations", "function", "args"]) and not any(
                            k in keys_lower for k in ["success", "message", "error", "data", "items", "cart", "products"]
                        )
                        if is_call:
                            logger.debug("_extract_text_from_part: skipping tool call JSON", sample=stripped[:100])
                            return None
                    if isinstance(js, dict):
                        for key in ("text", "message", "result", "response", "formatted_text"):
                            val = js.get(key)
                            if isinstance(val, str) and val.strip():
                                return val.strip()
                except Exception:
                    pass
            return stripped

        # Second try: function_response (tool outputs FROM tools)
        # These ARE important - they contain the product data the agent needs
        func_resp = getattr(part, "function_response", None)
        if func_resp is None and isinstance(part, dict):
            func_resp = part.get("function_response") or part.get("functionResponse")
        
        if func_resp:
            # Get the response payload
            payload = getattr(func_resp, "response", None)
            if payload is None and isinstance(func_resp, dict):
                payload = func_resp.get("response")
            
            # If it's a string, check if it contains useful data
            if isinstance(payload, str):
                stripped = payload.strip()
                
                # KEEP tool responses that have actual data
                # These have keys like "success", "products", "data", "message"
                has_data_keys = any(key in stripped.lower() for key in [
                    '"success":', '"products":', '"data":', '"items":', 
                    '"results":', '"message":', '"cart":'
                ])
                
                if has_data_keys:
                    # This is a tool response with actual data - KEEP IT
                    # The agent NEEDS this to formulate its response
                    logger.debug("_extract_text_from_part: keeping tool response with data", sample=stripped[:100])
                    # Try to extract text field from JSON if present
                    try:
                        js = json.loads(stripped)
                        if isinstance(js, dict):
                            for key in ("text", "message", "result", "response", "formatted_text"):
                                val = js.get(key)
                                if isinstance(val, str) and val.strip():
                                    return val.strip()
                    except Exception:
                        pass
                    return stripped
                
                # Only skip if it's truly empty/useless
                if not stripped or len(stripped) < 10:
                    return None
                
                return stripped
            
            # If it's a dict, try to extract useful text
            if isinstance(payload, dict):
                # Check for common text fields in tool responses
                for key in ("message", "text", "result", "output", "response", "summary"):
                    val = payload.get(key)
                    if isinstance(val, str) and val.strip():
                        # Skip if this nested value is JSON
                        if val.strip().startswith("{") and '"' in val:
                            continue
                        return val.strip()
                
                # If no text field, but has "success" + "data", keep the whole JSON
                # The agent can parse it
                if payload.get("success") or payload.get("data"):
                    logger.debug("_extract_text_from_part: keeping tool response dict", keys=list(payload.keys())[:5])
                    return json.dumps(payload)
        
        return None


    async def _call_agent_text_only(self, query: str, user_id: str, session_id: str) -> str:
        """
        Invoke the agent with a compact prompt and properly extract responses.
        
        CRITICAL: Must distinguish between:
        - Tool responses (contain data agent needs)
        - Agent's natural language responses (what we return to user)
        """
        if not query or not query.strip():
            raise ValueError("Query cannot be empty")

        # Fetch & inject running summary
        try:
            summary = self.session_helper.get_summary(user_id)
        except Exception:
            summary = ""

        # NEW: Detect if this is a continuation (not a trivial greeting)
        is_continuation = not self._is_trivial_greeting(query) and not self.is_goodbye_message(query)
        
        merged_query = query.strip()

        # If user writes quantity in piece/pack terms, force this turn to treat it as boxes.
        if self._looks_like_piece_pack_order_quantity(query):
            merged_query = (
                "[UNIT_ALIAS_POLICY]\n"
                "User quantity terms piece/pieces/packet/pack/packs/pc/pcs must be treated as boxes.\n"
                "Never place or confirm orders in pieces/packets/packs; use boxes (and cartons only as conversion shorthand).\n"
                "[/UNIT_ALIAS_POLICY]\n"
                f"{merged_query}"
            )
        
        if summary:
            merged_query = f"[SESSION_SUMMARY]\n{summary}\n[/SESSION_SUMMARY]\n{merged_query}"
        
        # NEW: Add continuation hint for buffered messages
        if is_continuation and summary:
            merged_query = f"[CONTEXT: User is continuing previous conversation]\n{merged_query}"

        # Hard runtime execution policy: complete tool chain before replying.
        execution_policy = (
            "[EXECUTION_POLICY]\n"
            "Complete all required tool calls for this request before producing any user-visible response.\n"
            "Do not send per-item progress updates.\n"
            "For multi-item cart actions, resolve all required SKUs and complete all cart mutations in the same turn,\n"
            "then send one consolidated response.\n"
            "[/EXECUTION_POLICY]"
        )
        merged_query = f"{execution_policy}\n{merged_query}"

        # Prepend customer/store context if available
        try:
            customer_ctx = self._build_customer_context_for_agent(user_id)
        except Exception:
            customer_ctx = ""
        if customer_ctx:
            merged_query = f"[CUSTOMER_CONTEXT]\n{customer_ctx}\n[/CUSTOMER_CONTEXT]\n{merged_query}"

        try:
            pending_order_confirm_ctx = self._build_pending_order_confirmation_context(user_id)
        except Exception:
            pending_order_confirm_ctx = ""
        # Do not inject pending order context for trivial greetings (hi/hello); agent should greet normally.
        if pending_order_confirm_ctx and not self._is_trivial_greeting(query):
            merged_query = (
                f"[PENDING_ORDER_CONFIRMATION]\n{pending_order_confirm_ctx}\n[/PENDING_ORDER_CONFIRMATION]\n"
                f"{merged_query}"
            )

        try:
            # If there is a pending order-confirmation prompt, it must take precedence
            # over recommendation-confirmation context for short affirmative replies.
            pending_rec_ctx = "" if pending_order_confirm_ctx else self._build_pending_recommendations_context(user_id)
        except Exception:
            pending_rec_ctx = ""
        if pending_rec_ctx:
            merged_query = (
                f"[PENDING_RECOMMENDATIONS]\n{pending_rec_ctx}\n[/PENDING_RECOMMENDATIONS]\n"
                f"{merged_query}"
            )

        # Build Content object
        if Content is not None and Part is not None:
            content = Content(role="user", parts=[Part(text=merged_query)])
        else:
            # Fallback shim
            class _ShimPart:
                def __init__(self, text: str):
                    self.text = text

            class _ShimContent:
                def __init__(self, role: str, parts):
                    self.role = role
                    self.parts = parts

            content = _ShimContent("user", [_ShimPart(merged_query)])

        # Agent execution
        session, runner = await self.setup_session_and_runner(user_id, session_id)
        if not session:
            raise ValueError("Session not found")

        # Seed guardrail callback context for this turn so tool wrappers can
        # reliably read latest user text and mutable state.
        turn_state: Dict[str, Any] = {}
        try:
            state_obj = getattr(session, "state", None)
            if not isinstance(state_obj, dict):
                state_obj = {}
            # Keep customer metadata fresh in callback state so template tools can
            # always personalize without waiting for session rotation.
            try:
                latest_meta = self._get_stored_customer_metadata(user_id)
                state_obj.update({k: v for k, v in latest_meta.items() if v})
            except Exception:
                pass
            state_obj["last_user_text"] = query
            state_obj["user_message"] = query
            # Per-turn execution telemetry for guardrails/tool wrappers.
            state_obj["_turn_tool_calls"] = 0
            state_obj["_turn_tool_sequence"] = []
            state_obj["_turn_cart_mutation_count"] = 0
            state_obj["_order_placed_this_turn"] = False
            state_obj["_turn_user_query"] = query.strip()
            turn_state = state_obj

            class _SeededGuardrailContext:
                def __init__(self, state: Dict[str, Any], user_content: Any):
                    self.state = state
                    self.user_content = user_content

            adk_guardrails.set_callback_context(
                _SeededGuardrailContext(state_obj, content)
            )
        except Exception as seed_err:
            logger.warning(
                "guardrail.context.seed_failed",
                user_id=user_id,
                error=str(seed_err),
            )

        events = runner.run_async(
            user_id=user_id,
            session_id=session.id,
            new_message=content,
        )

        final_text = ""
        last_seen_text = ""
        last_seen_tool_data = ""
        saw_final_response = False
        event_count = 0
        last_event_type = ""
        last_part_types: List[str] = []
        gemini_usage = None
        agent_start_time = time.perf_counter()

        try:
            async for event in events:
                try:
                    event_count += 1
                    last_event_type = getattr(event, "event_type", "") or ""
                    lower_event_type = str(last_event_type).lower()
                    is_tool_event = "tool" in lower_event_type
                    # Extract text from all parts
                    candidate_parts: List[str] = []
                    content_obj = getattr(event, "content", None)
                    
                    if content_obj and getattr(content_obj, "parts", None):
                        if self.debug_agent_logs:
                            try:
                                last_part_types = [type(p).__name__ for p in content_obj.parts]
                            except Exception:
                                last_part_types = []
                        for p in content_obj.parts:
                            maybe_txt = self._extract_text_from_part(p)  # NOTE: using self. here
                            if maybe_txt:
                                candidate_parts.append(maybe_txt)
                        
                        if candidate_parts:
                            # Join all parts
                            combined = " ".join(candidate_parts).strip()
                            
                            # CRITICAL: Only keep if this looks like AGENT TEXT, not tool JSON
                            # Agent text doesn't start with { or contain "success": true patterns
                            is_agent_response = not (
                                combined.startswith("{") and
                                any(marker in combined.lower() for marker in [
                                    '"success":',
                                    '"products":',
                                    '"data":',
                                    '"error":'
                                ])
                            )
                            
                            if is_agent_response and not is_tool_event:
                                last_seen_text = combined
                            else:
                                if combined.startswith("{") or combined.startswith("[") or is_tool_event:
                                    last_seen_tool_data = combined
                                # This is tool data - agent is still processing
                                if self.debug_agent_logs:
                                    logger.info(
                                        "agent.tool_data.part",
                                        user_id=user_id,
                                        sample=combined[:200],
                                        event_type=last_event_type,
                                    )

                    # Check if this is the final response
                    is_final = False
                    attr = getattr(event, "is_final_response", None)
                    if callable(attr):
                        is_final = attr()
                    elif isinstance(attr, bool):
                        is_final = attr
                    else:
                        is_final = (getattr(event, "event_type", "") == "final_response")

                    if is_final:
                        saw_final_response = True
                        # Prefer last_seen_text if we have agent text
                        if last_seen_text and not last_seen_text.startswith("{"):
                            final_text = last_seen_text
                        
                        # Capture usage metadata
                        try:
                            usage_meta = getattr(event, "usage_metadata", None)
                            if usage_meta is not None:
                                agent_latency_ms = int((time.perf_counter() - agent_start_time) * 1000)

                                prompt_tokens = int(getattr(usage_meta, "prompt_token_count", None) or 0)
                                candidates_tokens = int(getattr(usage_meta, "candidates_token_count", None) or 0)
                                total_tokens = int(getattr(usage_meta, "total_token_count", None) or 0)
                                thoughts_tokens = int(getattr(usage_meta, "thoughts_token_count", None) or 0)

                                input_price_gemini = 0.30 / 1_000_000
                                output_price_gemini = 2.50 / 1_000_000

                                gemini_usage = {
                                    "enabled": True,
                                    "model": "gemini-2.5-flash",
                                    "request_id": getattr(event, "invocation_id", "") or "",
                                    "latency_ms": agent_latency_ms,
                                    "input_tokens": prompt_tokens,
                                    "output_tokens": candidates_tokens,
                                    "total_tokens": total_tokens,
                                    "thoughts_tokens": thoughts_tokens,
                                    "pricing": {
                                        "currency": "USD",
                                        "input_unit": "1M_tokens",
                                        "output_unit": "1M_tokens",
                                        "input_price_per_unit": input_price_gemini,
                                        "output_price_per_unit": output_price_gemini,
                                    },
                                    "cost": {
                                        "input_cost_usd": round(prompt_tokens * input_price_gemini, 6),
                                        "output_cost_usd": round(candidates_tokens * output_price_gemini, 6),
                                    },
                                }
                                gemini_usage["cost"]["total_cost_usd"] = round(
                                    gemini_usage["cost"]["input_cost_usd"]
                                    + gemini_usage["cost"]["output_cost_usd"],
                                    6,
                                )

                                try:
                                    self._last_gemini_usage[user_id] = gemini_usage
                                except Exception:
                                    pass

                                logger.info(
                                    "gemini.usage.captured",
                                    user_id=user_id,
                                    prompt_tokens=prompt_tokens,
                                    candidates_tokens=candidates_tokens,
                                    total_tokens=total_tokens,
                                    thoughts_tokens=thoughts_tokens,
                                    latency_ms=agent_latency_ms,
                                    total_cost_usd=gemini_usage["cost"]["total_cost_usd"],
                                )
                        except Exception as usage_err:
                            logger.warning("gemini.usage_extraction.error", error=str(usage_err))

                        # Clarifying-question exception:
                        # If no tool calls were made this turn and the final text is a
                        # short clarifying question, allow early exit.
                        turn_tool_calls = 0
                        try:
                            if isinstance(turn_state, dict):
                                turn_tool_calls = int(turn_state.get("_turn_tool_calls") or 0)
                        except Exception:
                            turn_tool_calls = 0

                        if (
                            final_text
                            and not final_text.startswith("{")
                            and turn_tool_calls == 0
                            and self._is_clarifying_question_text(final_text)
                        ):
                            if self.debug_agent_logs:
                                logger.info(
                                    "agent.final_response.early_exit_clarifying_question",
                                    user_id=user_id,
                                    session_id=session_id,
                                    event_count=event_count,
                                    preview=final_text[:160],
                                )
                            break

                        # Default path: drain the stream so all tool work finishes
                        # before returning a user-visible reply.
                        continue
                        
                except Exception as per_event:
                    logger.warning("agent.event.process.error", error=str(per_event))
                    continue
                    
        finally:
            aclose = getattr(events, "aclose", None)
            if callable(aclose):
                try:
                    await aclose()
                except Exception:
                    pass
            try:
                adk_guardrails.clear_callback_context()
            except Exception:
                pass

        # Fallback to last seen text if no final
        if not final_text and last_seen_text:
            final_text = last_seen_text
        if not final_text and saw_final_response and self.debug_agent_logs:
            logger.info(
                "agent.final_response.seen_without_text",
                user_id=user_id,
                session_id=session_id,
                event_count=event_count,
                last_event_type=last_event_type,
            )
        if not final_text and last_seen_tool_data:
            recovered = self._recover_malformed_template_call(last_seen_tool_data)
            if recovered != last_seen_tool_data:
                final_text = recovered
            else:
                tool_data_keys = None
                tool_products = None
                tool_name = None
                try:
                    parsed = last_seen_tool_data
                    if isinstance(parsed, str):
                        try:
                            parsed = json.loads(parsed)
                        except Exception:
                            parsed = None
                    if isinstance(parsed, dict):
                        tool_data_keys = list(parsed.keys())[:8]
                        tool_name = parsed.get("tool") or parsed.get("name") or parsed.get("system")
                        data = parsed.get("data") or parsed.get("result")
                        if isinstance(data, dict) and isinstance(data.get("data"), list):
                            tool_products = len(data.get("data"))
                        elif isinstance(data, list):
                            tool_products = len(data)
                    elif isinstance(parsed, list):
                        tool_products = len(parsed)
                except Exception:
                    tool_data_keys = "parse_failed"

                logger.warning(
                    "agent.final_text.empty_with_tool_data",
                    user_id=user_id,
                    session_id=session_id,
                    event_count=event_count,
                    last_event_type=last_event_type,
                    tool_data_preview=last_seen_tool_data[:300],
                    tool_data_keys=tool_data_keys,
                    tool_products=tool_products,
                    tool_name=tool_name,
                )

        # Validate we didn't get raw tool JSON as final response
        if final_text:
            stripped = final_text.strip()
            if stripped.startswith("{") and ('"success"' in stripped or '"data"' in stripped or '"products"' in stripped):
                logger.error(
                    "agent.returned_raw_json",
                    user_id=user_id,
                    sample=stripped[:200]
                )
                recovered = self._recover_malformed_template_call(final_text)
                if recovered != final_text:
                    final_text = recovered
                else:
                    # Force fallback instead of sending JSON
                    final_text = "Mujhe products mil gaye hain. Kya aap quantity batayen?"

        # Fix malformed tool calls
        final_text = self._recover_malformed_template_call(final_text)
        if final_text and "Malformed function call" in final_text:
            logger.warning("agent.malformed_tool_call_fallback", user_id=user_id)
            final_text = "jee thora sa time lag raha hai, main dobara check karti hoon."

        # Guardrail/template overrides: prefer forced replies captured in state or guardrails.
        try:
            forced = adk_guardrails.pop_forced_reply(user_id=user_id)
            if not forced and hasattr(session, "state") and isinstance(getattr(session, "state"), dict):
                forced = (session.state.get("forced_reply") or "").strip()
            if forced:
                final_text = forced
                # Ensure forced replies are one-shot and don't persist into next turns
                try:
                    if hasattr(session, "state") and isinstance(getattr(session, "state"), dict):
                        session.state.pop("forced_reply", None)
                except Exception:
                    pass
                try:
                    adk_guardrails.remember_forced_reply(None, user_id=user_id)
                except Exception:
                    pass
        except Exception:
            pass

        if final_text:
            final_text = self._sanitize_customer_visible_text(final_text, user_id=user_id)

        if not final_text:
            logger.warning(
                "agent.final_text.empty",
                user_id=user_id,
                session_id=session_id,
                event_count=event_count,
                last_event_type=last_event_type,
                last_part_types=last_part_types,
                last_seen_text_preview=last_seen_text[:200],
                last_seen_tool_data_preview=last_seen_tool_data[:200],
            )

        # Update summary in background
        try:
            if final_text:
                prior_summary = summary
                user_text = query
                assistant_text = final_text

                # FIXED: Spawn a real thread instead of an asyncio task so it isn't killed
                def _summary_thread_runner():
                    try:
                        # Run the async summarizer in this new thread's event loop
                        new_summary = asyncio.run(self._summarize_turn(prior_summary, user_text, assistant_text))
                        if new_summary and new_summary != prior_summary:
                            try:
                                self.session_helper.set_summary(user_id, new_summary[:4000])
                            except Exception:
                                pass
                    except Exception as e:
                        logger.error("summary_bg_thread.error", error=str(e))

                import threading
                threading.Thread(target=_summary_thread_runner, daemon=True).start()
        except Exception:
            pass

        return final_text or CUSTOMER_SAFE_FALLBACK_TEXT

    def _looks_like_internal_error_reply(self, text: str) -> bool:
        if not isinstance(text, str):
            return False
        lowered = text.strip().lower()
        if not lowered:
            return False
        if any(marker in lowered for marker in INTERNAL_ERROR_REPLY_MARKERS):
            return True
        if re.search(r"\berror\s*:\s*", lowered):
            return True
        if re.search(r"\bcode\s+[a-z0-9_]+\b", lowered):
            return True
        if re.search(r"\brequested_[a-z0-9_]+\b", lowered):
            return True
        return False

    def _looks_like_piece_pack_order_quantity(self, text: str) -> bool:
        if not isinstance(text, str):
            return False
        t = text.strip().lower()
        if not t:
            return False
        alias = r"(?:piece|pieces|packet|packets|pack|packs|pc|pcs)"
        order_hint = (
            r"\b("
            r"order|orders|add|added|buy|qty|quantity|cart|draft|confirm|place|send"
            r")\b"
        )
        if re.search(rf"^\d+\s*{alias}$", t):
            return True
        if re.search(rf"\b\d+\s*{alias}\b", t) and re.search(order_hint, t):
            return True

        # Informational packaging questions should remain in piece/box terms.
        if re.search(r"\bpieces?\s*(?:in|per)\s*(?:a\s+)?(?:box|boxes|carton|cartons)\b", t):
            return False
        if re.search(r"\b(?:box|boxes|carton|cartons)\s*(?:per)\b.*\bpieces?\b", t):
            return False

        if re.search(rf"\b(how many|qty|quantity)\b.*\b{alias}\b", t) and re.search(order_hint, t):
            return True
        return False

    def _normalize_order_unit_terms(self, text: str) -> str:
        if not text:
            return text

        normalized = text
        normalized = re.sub(
            r"\bcartons?\s*/\s*(?:packs?|packets?|pieces?|pcs?)\b",
            "cartons / boxes",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"\bhow many\s+(?:packs?|packets?|pieces?|pcs?)\b",
            lambda m: "How many boxes" if m.group(0)[:1].isupper() else "how many boxes",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"\b(?P<num>\d+)\s*(?:pieces?|packs?|packets?|pcs?)\b(?P<tail>\s*(?:order|orders|qty|quantity|add|added|cart|draft|confirm|place))",
            lambda m: f"{m.group('num')} boxes{m.group('tail')}",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"\b(?P<prefix>add|added|order|orders|qty|quantity|confirm|place)\s+(?P<num>\d+)\s*(?:pieces?|packs?|packets?|pcs?)\b",
            lambda m: f"{m.group('prefix')} {m.group('num')} boxes",
            normalized,
            flags=re.IGNORECASE,
        )
        normalized = re.sub(
            r"^\s*(?P<num>\d+)\s*(?:pieces?|packs?|packets?|pcs?)\s*$",
            lambda m: f"{m.group('num')} boxes",
            normalized,
            flags=re.IGNORECASE,
        )
        return normalized

    def _get_customer_first_name(self, user_id: str) -> Optional[str]:
        try:
            meta = self._get_stored_customer_metadata(user_id)
        except Exception:
            meta = {}
        if not isinstance(meta, dict):
            return None
        raw_name = (
            meta.get("customer_name")
            or meta.get("contact_name")
            or meta.get("retailer_name")
            or meta.get("owner_name")
        )
        return extract_first_name(raw_name)

    def _personalize_customer_addressing(self, text: str, *, user_id: str) -> str:
        """
        Add light first-name addressing in high-signal confirmations.
        Keep this sparse for natural tone (at most once per message).
        """
        if not text:
            return text
        return text

    def _sanitize_customer_visible_text(self, text: Any, *, user_id: str) -> str:
        if not isinstance(text, str):
            return CUSTOMER_SAFE_FALLBACK_TEXT
        candidate = text.strip()
        if not candidate:
            return CUSTOMER_SAFE_FALLBACK_TEXT
        if self._looks_like_internal_error_reply(candidate):
            logger.warning(
                "agent.customer_reply.redacted_error",
                user_id=user_id,
                preview=candidate[:500],
            )
            return CUSTOMER_SAFE_FALLBACK_TEXT
        # Convert markdown bold (**text**) to WhatsApp bold (*text*)
        candidate = re.sub(r'\*\*(.+?)\*\*', r'*\1*', candidate)
        normalized = self._normalize_order_unit_terms(candidate)
        return self._personalize_customer_addressing(normalized, user_id=user_id)

    def _is_clarifying_question_text(self, text: str) -> bool:
        if not isinstance(text, str):
            return False
        t = text.strip().lower()
        if not t or "?" not in t:
            return False
        if len(t) > 280:
            return False
        hints = (
            "which one",
            "did you mean",
            "could you clarify",
            "please clarify",
            "how many",
            "confirm",
            "choose",
            "select",
            "pick",
            "quantity",
            "qty",
            "which one",
            "how much",
            "how many",
        )
        return any(h in t for h in hints)

    def get_last_gemini_usage(self, user_id: str) -> Optional[dict]:
        try:
            return self._last_gemini_usage.get(user_id)
        except Exception:
            return None

    def get_last_eleven_tts_usage(self, user_id: str) -> Optional[dict]:
        try:
            return self._last_eleven_tts_usage.get(user_id)
        except Exception:
            return None



    # ---------- Misc ----------
    def _clean_system_phrase(self, text: str) -> str:
        """
        Normalize any 'not found in system' style phrases to a friendlier alternative.
        """
        if not text:
            return text

        patterns = [
            r"not\s+found\s+in\s+(?:the\s+)?system",
            r"not\s+available\s+in\s+(?:the\s+)?system",
        ]

        for pat in patterns:
            text = re.sub(pat, "not available at the moment", text, flags=re.IGNORECASE)

        return self._normalize_order_unit_terms(text)

    def _recover_malformed_template_call(self, text: str) -> str:
        """
        If the model returns a malformed template tool call string, try to parse the
        embedded cart and render the template locally so the user still gets a reply.
        """
        if not text:
            return text

        target = None
        if "vn_order_draft_template" in text:
            target = "vn_order_draft_template"
        elif "order_draft_template" in text:
            target = "order_draft_template"

        def _parse_payload(payload_str: str, *, log_errors: bool) -> Optional[Any]:
            try:
                return json.loads(payload_str)
            except Exception:
                pass
            try:
                return ast.literal_eval(payload_str)
            except Exception as e:
                if log_errors:
                    logger.warning("malformed_call.parse_failed", error=str(e))
                return None

        def _extract_braced_payload(source: str) -> Optional[Tuple[Optional[str], str]]:
            def _extract_first_braced(start_at: int) -> Optional[str]:
                brace_start = source.find("{", start_at)
                if brace_start == -1:
                    return None
                depth = 0
                start_idx = None
                for pos in range(brace_start, len(source)):
                    ch = source[pos]
                    if ch == "{":
                        depth += 1
                        if depth == 1:
                            start_idx = pos
                    elif ch == "}":
                        if depth:
                            depth -= 1
                            if depth == 0 and start_idx is not None:
                                return source[start_idx : pos + 1]
                return None

            for match in re.finditer(r"(cart|draft)\s*=", source):
                payload = _extract_first_braced(match.end())
                if payload:
                    return match.group(1), payload

            if target:
                for fname in ("vn_order_draft_template", "order_draft_template"):
                    idx = source.find(fname)
                    if idx == -1:
                        continue
                    payload = _extract_first_braced(idx)
                    if payload:
                        return None, payload

            return None

        def _looks_like_order_draft(payload: Any) -> bool:
            if not isinstance(payload, dict):
                return False
            if payload.get("cart") or payload.get("draft"):
                return True
            basket = payload.get("basket")
            if isinstance(basket, dict) and isinstance(basket.get("items"), list):
                return True
            totals = payload.get("totals")
            if isinstance(totals, dict) and any(
                k in totals for k in ("grand_total", "subtotal", "discount_total", "profit_total")
            ):
                return True
            items = payload.get("items")
            if isinstance(items, list):
                for it in items:
                    if isinstance(it, dict) and (it.get("name") or it.get("sku")) and (
                        it.get("qty") or it.get("quantity") or it.get("line_total") or it.get("final_price")
                    ):
                        return True
            return False

        def _render_payload(payload: dict, payload_key: Optional[str]) -> Optional[str]:
            if not isinstance(payload, dict):
                return None
            data = payload.get("cart") or payload.get("draft") or payload
            try:
                if target == "vn_order_draft_template":
                    return vn_order_draft_template({"draft": data})
                if payload_key == "draft":
                    return order_draft_template(
                        draft=data,
                        ok=payload.get("ok"),
                        errors=payload.get("errors"),
                        warnings=payload.get("warnings"),
                    )
                return order_draft_template(
                    cart=data,
                    ok=payload.get("ok"),
                    errors=payload.get("errors"),
                    warnings=payload.get("warnings"),
                )
            except Exception as e:
                logger.warning(
                    "malformed_call.render_failed",
                    target=target or "order_draft_template",
                    error=str(e),
                )
                return None

        if "Malformed function call" in text or target:
            payload_info = _extract_braced_payload(text)
            if payload_info:
                payload_key, payload_str = payload_info
                payload = _parse_payload(payload_str, log_errors=True)
                if isinstance(payload, dict):
                    fixed = _render_payload(payload, payload_key)
                    if fixed:
                        logger.warning("malformed_call.recovered", target=target or "order_draft_template")
                        return fixed

        stripped = text.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            payload = _parse_payload(stripped, log_errors=False)
            if _looks_like_order_draft(payload):
                fixed = _render_payload(payload, payload_key=None)
                if fixed:
                    logger.warning("order_draft.json_recovered", target=target or "order_draft_template")
                    return fixed

        return text

    def _explicit_catalog_intent(self, text: str) -> bool:
        """
        Pure string heuristic: does the user clearly ask for catalog / list / rate list?
        This is checked on EVERY message (text or transcribed audio).
        """
        if not text:
            return False

        t = text.strip().lower()

        # Very loose list, you can tweak over time
        phrases = [
            "catalog", "catalogue", "katalog",
            "product list", "productlist",
            "price list", "pricelist",
            "rate list", "ratelist",
            "send rates", "send rate list",
            "send list", "list send",
            "send catalog", "catalog send",
            "product catalogue", "product catalog",
            "items ki list", "items ka list",
            "all products", "all items", "full catalog", "full catalogue",
            "send all products", "send all items",
            "product message", "products message",
        ]

        return any(p in t for p in phrases)

    def _is_product_overview_intent(self, text: str) -> bool:
        """
        Concrete-specific intent: user asks specifically about concrete grades/mixes.
        General product questions (what products do you have?) are handled by the LLM
        agent which has full catalog knowledge across all categories.
        """
        if not text:
            return False
        t = text.strip().lower()
        needles = [
            "what concrete do you have",
            "what concrete",
            "concrete do you have",
            "concrete grades",
            "what grades",
            "what mixes",
            "ready mix",
            "ready-mix",
        ]
        return any(n in t for n in needles)

    def _project_usecase_menu_text(self) -> str:
        return (
            "For concrete, what are you building?\n"
            "A slab, foundation, columns/beams, driveway, or something else?\n\n"
            "Or if you're looking for *cement, drymix, aggregates, or DIY products*, just let me know!"
        )

    def _parse_project_usecase_choice(self, text: str) -> Optional[int]:
        if not text:
            return None
        t = text.strip().lower()
        # Numeric choice
        m = re.match(r"^\s*([1-6])\b", t)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
        # Text choice
        if any(k in t for k in ("slab", "floor")):
            return 1
        if any(k in t for k in ("foundation", "foundations", "footing", "footings")):
            return 2
        if any(k in t for k in ("column", "columns", "beam", "beams")):
            return 3
        if any(k in t for k in ("driveway", "decor", "decorative", "finish", "finishing")):
            return 4
        if any(k in t for k in ("tank", "wet", "water", "bath", "basement")):
            return 5
        if "else" in t or "other" in t:
            return 6
        return None

    async def _handle_project_usecase_reply(self, user_id: str, user_text: str, *, reply_to_message_id: Optional[str]) -> str:
        choice = self._parse_project_usecase_choice(user_text or "")
        if not choice:
            msg = "What are you building? A slab, foundation, columns/beams, driveway, or something else?"
            self._send_text_once(user_id, msg, reply_to_message_id=reply_to_message_id)
            return msg

        if choice == 6:
            try:
                self.session_helper.set_onboarding_status(user_id, None)
            except Exception:
                pass
            msg = (
                "No problem! We cover more than just concrete.\n\n"
                "We also offer:\n"
                "• *ECOCem™* bag cement (Castle, Phoenix, Walcrete, Wallcem)\n"
                "• *ECODrymix™* premixed mortars (renders, plasters, tile adhesives, grouts)\n"
                "• *ECOSand™* & Coarse Aggregates\n"
                "• *QuickMix® DIY* repair & craft products\n\n"
                "What are you looking for?"
            )
            self._send_text_once(user_id, msg, reply_to_message_id=reply_to_message_id)
            return msg

        title_map = {
            1: "house slabs",
            2: "foundations",
            3: "columns & beams",
            4: "driveways / decorative work",
            5: "water tanks / wet areas",
        }
        usecase_title = title_map.get(choice, "your project")

        if choice == 1:
            body = (
                "• G25 (standard for residential slabs)\n"
                "• Optional upgrade: G30 (improved durability)\n"
                "• *FibreBuild* (SKU26) – fibre reinforced for crack resistance"
            )
        elif choice == 2:
            body = (
                "• G25 or G30 (house foundations)\n"
                "• *EcoBuild* (SKU16) – eco-friendly option with lower CO₂"
            )
        elif choice == 3:
            body = (
                "• G30 – G35 (structural columns/beams)\n"
                "• *SuperBuild* (SKU20) – high compressive strength"
            )
        elif choice == 4:
            body = (
                "• G25–G30 for structural driveways\n"
                "• *DecoBuild* (SKU18) – aesthetic concrete (Line, Print, Stone, Exposed)\n"
                "• *FairBuild* (SKU23) – refined visual finish, no painting needed"
            )
        elif choice == 5:
            body = (
                "• G30 with waterproofing admixture\n"
                "• *CoolBuild* (SKU21) – reduced cracking risk"
            )
        else:
            body = (
                "• G25–G30 (most residential applications)\n"
                "• *EcoBuild* (SKU16) – eco-friendly option"
            )

        # Min order from knowledge (5 m³)
        min_line = "\n\nMinimum order: 5 m³."
        msg = (
            f"For {usecase_title} we recommend:\n\n"
            f"{body}"
            f"{min_line}\n\n"
            "How many m³ do you need?"
        )

        # Set expectation so a bare number reply is interpreted as volume.
        try:
            self.session_helper.set_expected_reply(user_id, "volume_m3")
        except Exception:
            pass

        try:
            self.session_helper.set_onboarding_status(user_id, None)
        except Exception:
            pass

        self._send_text_once(user_id, msg, reply_to_message_id=reply_to_message_id)
        return msg


    def _is_trivial_greeting(self, msg: str) -> bool:
        if not msg:
            return False
        m = msg.strip().lower()
        m = "".join(ch for ch in m if ch.isalnum() or ch.isspace())
        return m in TRIVIAL_GREETS or (len(m) <= 8 and any(g in m for g in TRIVIAL_GREETS))

    def is_goodbye_message(self, message: str) -> bool:
        return bool(GOODBYE_RE.search((message or "").strip()))

   