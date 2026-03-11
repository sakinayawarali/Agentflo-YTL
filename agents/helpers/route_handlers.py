# route_handlers.py
import os
import json
import requests
import logging
import tempfile
import base64
import urllib.parse
import re
import asyncio
from flask import jsonify, make_response, request
from utils.logging import logger
from agents.evaluation.conversation_evalutor import evaluate_conversation_turn
import time
import threading
from agents.audio.utils import trim_trailing_silence, sniff_audio_mime, is_audio_too_small
from agents.audio import VoiceNoteTranscriber
from agents.buffering.message_buffer import MessageBufferStore
from google.cloud import tasks_v2
from google.api_core import exceptions as gcp_exceptions
import datetime
from datetime import timedelta
from google.protobuf import timestamp_pb2
from typing import Optional
from agents.helpers.vn_s3_upload import (
    store_voice_note_to_s3,
    store_image_to_s3,
    upload_bytes_to_s3,
)
from agents.helpers.billing_logger import (
    TENANT_ID,
    TENANT_NAME,
    AGENT_ID,
    AGENT_NAME,
    generate_billing_event_v2,
    send_billing_event_fire_and_forget,
)
from agents.tools.order_draft_tools import get_cart, send_product_catalogue
from agents.tools.cart_tools import agentflo_cart_tool
from agents.tools.templates import order_draft_template, MULTI_MESSAGE_DELIMITER
from agents.helpers.invoice_ocr import (
    build_invoice_payload_from_image,
    verify_extracted_invoice,
)
from agents.tools.api_tools import update_customer_name, unwrap_tool_response
from agents.helpers.inbound_store import InboundStore  # <-- NEW
# from agents.helpers.message_buffer import MessageBufferStore

_ASKS_LOCATION_RE = re.compile(
    r"("
    r"delivery location|site location|project location|location pin|location so i can"
    r"|share your location|send your location|share.*pin|provide.*location"
    r"|where.{0,30}deliver|where.{0,30}project|where.{0,30}site|where.{0,30}located"
    r"|need.{0,30}location|need.{0,30}address|need.{0,30}pin"
    r"|check.{0,30}deliver|check.{0,30}nearest.{0,30}plant"
    r"|tap.{0,20}button.{0,20}location|tap.{0,20}location"
    r"|send.{0,20}pin|drop.{0,20}pin|share.{0,20}location"
    r"|delivery address|site address|project address"
    r"|which.{0,20}area|what.{0,20}area.{0,20}deliver"
    r"|nearest.{0,20}plant|closest.{0,20}plant"
    r"|\[SEND_LOCATION_PIN\]"
    r")",
    re.IGNORECASE,
)

# Optional: count WhatsApp status webhooks (sent/delivered/read) as session "activity"
SESSION_TOUCH_ON_STATUS = os.getenv("SESSION_TOUCH_ON_STATUS", "false").lower() == "true"

# WhatsApp creds (used to mark inbound messages as read)
WA_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WA_ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")
WA_GRAPH_URL = (
    f"https://graph.facebook.com/v23.0/{WA_PHONE_NUMBER_ID}/messages" if WA_PHONE_NUMBER_ID else None
)

# Read receipts (Cloud API): mark inbound messages as read
READ_RECEIPTS_ENABLED = os.getenv("READ_RECEIPTS_ENABLED", "true").lower() == "true"

# Voice reaction UX toggle + timing: 👂 (listening) then ✅ (tick) on voice notes
VM_REACTION_ENABLED = os.getenv("VM_REACTION", "true").lower() == "true"
VM_REACTION_SWAP_SEC = 3.5

# --- CLOUD TASKS CONFIG ---
PROJECT_ID = os.getenv("GOOGLE_CLOUD_PROJECT")
QUEUE_LOCATION = os.getenv("BUFFER_QUEUE_LOCATION", "us-central1")
QUEUE_ID = os.getenv("BUFFER_QUEUE_ID", "message-buffer-queue")
# The full URL of your deployed agent, e.g., https://my-agent.run.app/tasks/drain-buffer
DRAIN_URL = os.getenv("BUFFER_DRAIN_URL")
class RouteHandler:
    def __init__(self, verify_token: str):
        self.verify_token = verify_token
        self.transport = (os.getenv("WHATSAPP_TRANSPORT", "meta") or "meta").strip().lower()
        self.is_twilio = self.transport == "twilio"
        self.twilio_account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        self.twilio_auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        self.twilio_from_number = os.getenv("TWILIO_WHATSAPP_FROM") or os.getenv("TWILIO_FROM")
        self.twilio_profile_fallback = os.getenv("TWILIO_PROFILE_FALLBACK", "Unknown")

        # Lazy imports to avoid circular dependencies.
        from .adk_helper import ADKHelper
            
        from .image_helper import WhatsAppImageHelper
        from .order_helper import OrderHelper

        self.adk_helper = ADKHelper()
        self.audio_transcriber = VoiceNoteTranscriber()
        self.image_helper = WhatsAppImageHelper()
        self.order_helper = OrderHelper()

        # NEW: inbound idempotency + message buffering
        self.inbound_store = InboundStore()
        
        self.message_buffer = MessageBufferStore()
        # Initialize Cloud Tasks Client
        # try:
        #     self.tasks_client = tasks_v2.CloudTasksClient()
        #     self.parent_queue = self.tasks_client.queue_path(PROJECT_ID, QUEUE_LOCATION, QUEUE_ID)
        # except Exception as e:
        #     logger.error(f"Failed to init Cloud Tasks: {e}")
        #     self.tasks_client = None
        
        # Dedupe unified billing emissions per inbound message_id
        self._emitted_billing_ids = set()

    def _sanitize_wa_number(self, num: str) -> str:
        if not num:
            return ""
        n = str(num)
        n = n.replace("whatsapp:", "").strip()
        return n.lstrip("+")

    def _mask_number(self, num: str) -> str:
        if not num:
            return ""
        s = str(num)
        if len(s) <= 4:
            return s
        return f"...{s[-4:]}"

    def _normalize_invoice_payload(self, payload: dict) -> dict:
        def _as_list(val):
            if val is None:
                return []
            if isinstance(val, list):
                items = val
            else:
                items = [val]
            out = []
            for item in items:
                text = str(item).strip() if item is not None else ""
                if text:
                    out.append(text)
            return out

        def _normalize_phone(val: str) -> Optional[str]:
            digits = re.sub(r"\\D", "", val or "")
            if not digits:
                return None
            if digits.startswith("92"):
                return digits
            if digits.startswith("0") and len(digits) >= 11:
                return "92" + digits[1:]
            if digits.startswith("3") and len(digits) == 10:
                return "92" + digits
            return digits

        def _normalize_date(val: str) -> Optional[str]:
            if not val:
                return None
            raw = str(val)
            match = re.search(r"(\d{2})[/-](\d{2})[/-](\d{4})", raw)
            if match:
                dd, mm, yyyy = match.groups()
                return f"{yyyy}-{mm}-{dd}"
            match = re.search(r"(\d{4})[/-](\d{2})[/-](\d{2})", raw)
            if match:
                yyyy, mm, dd = match.groups()
                return f"{yyyy}-{mm}-{dd}"
            return None

        def _normalize_invoice_number(val: str) -> Optional[str]:
            raw = str(val or "")
            s = re.sub(r"\s+", "", raw).upper()
            if not s:
                return None
            if "INV" in s:
                return s
            s = s.replace("IN/", "INV").replace("IN-", "INV").replace("IN_", "INV")
            match = re.search(r"IN(?=\d)", s)
            if match and "INV" not in s:
                s = s[:match.start()] + "INV" + s[match.start() + 2:]
            return s

        tenant_id = (os.getenv("TENANT_ID") or "").strip()
        invoice_numbers = [
            n for n in (_normalize_invoice_number(x) for x in _as_list(payload.get("invoice_numbers"))) if n
        ]
        store_codes = [s.replace(" ", "").upper() for s in _as_list(payload.get("store_codes"))]
        delivery_dates = [
            d for d in (_normalize_date(x) for x in _as_list(payload.get("delivery_dates"))) if d
        ]
        mobile_number = _normalize_phone(payload.get("mobile_number") or "")

        return {
            "tenant_id": tenant_id,
            "mobile_number": mobile_number,
            "invoice_numbers": invoice_numbers,
            "store_codes": store_codes,
            "delivery_dates": delivery_dates,
        }

    def _parse_invoice_verification_response(self, raw: str | dict) -> tuple[bool, str, dict]:
        if not raw:
            return False, "Empty response from verification API.", {}
        if isinstance(raw, dict) and "success" in raw and "data" in raw:
            if not raw.get("success"):
                err = raw.get("error") or {}
                msg = err.get("message") or "Invoice verification failed."
                return False, msg, {}
            raw = raw.get("data") or {}
        if isinstance(raw, str) and raw.strip().startswith("Error:"):
            return False, raw.strip(), {}
        try:
            data = json.loads(raw) if isinstance(raw, str) else (raw or {})
        except json.JSONDecodeError:
            return False, "Invalid response from verification API.", {}

        success = False
        if isinstance(data, dict):
            if data.get("success") is True:
                success = True
            else:
                status = str(data.get("status") or "").lower()
                if status in {"success", "verified", "valid", "ok"}:
                    success = True
        message = ""
        if isinstance(data, dict):
            message = str(data.get("message") or data.get("error") or "")
        return success, message, data if isinstance(data, dict) else {}

    def _extract_external_user_id(self, payload: dict) -> Optional[str]:
        if not isinstance(payload, dict):
            return None
        data = payload.get("data") or payload.get("customer") or payload
        if not isinstance(data, dict):
            return None
        for key in ("user_id", "customer_id", "id"):
            val = data.get(key)
            if val:
                return str(val)
        return None

    def _handle_invoice_onboarding_image(
        self,
        user_id: str,
        *,
        media_id: Optional[str],
        media_url: Optional[str],
        mime_type: Optional[str],
        inbound_key: Optional[str],
        reply_to_message_id: Optional[str],
    ) -> bool:
        # YTL Cement demo: invoice onboarding is disabled (no authentication).
        if (os.getenv("TENANT_ID") or "").strip().lower() == "ytl":
            return False

        def _normalize_user_phone(val: str) -> str:
            digits = re.sub(r"\D", "", val or "")
            if not digits:
                return ""
            if digits.startswith("92"):
                return f"+{digits}"
            if digits.startswith("0") and len(digits) >= 11:
                return "+92" + digits[1:]
            if digits.startswith("3") and len(digits) == 10:
                return "+92" + digits
            return f"+{digits}"

        onboarding_status = self.adk_helper.session_helper.get_onboarding_status(user_id)
        user_mobile = _normalize_user_phone(user_id)
        logger.info(
            "invoice.onboarding.start",
            user_id=user_id,
            inbound_key=inbound_key,
            media_id=media_id,
            has_media_url=bool(media_url),
            mime_type=mime_type,
            transport=("twilio" if self.is_twilio else "meta"),
            status=onboarding_status,
            user_mobile=user_mobile,
        )

        # Billing context for invoice verification (image inbound + agent reply)
        billing_user_text = (
            f"[invoice_image] media_id={media_id or ''}; has_url={bool(media_url)}; mime={mime_type or ''}"
        )
        billing_conversation_id = f"{user_id}-verification"
        s3_key = None

        def _emit_invoice_billing(agent_text: str, *, user_text: Optional[str] = None, s3_key: Optional[str] = None) -> None:
            if not inbound_key:
                return

            nonlocal billing_conversation_id, billing_user_text

            payload_text = user_text or billing_user_text or "[invoice_image]"

            try:
                base_key = f"{inbound_key}::base"
                if base_key not in self._emitted_billing_ids:
                    base_event = generate_billing_event_v2(
                        tenant_id=TENANT_ID,
                        conversation_id=billing_conversation_id,
                        msg_type=self._billing_msg_type("image"),
                        message_id=inbound_key,
                        role="user",
                        channel="whatsapp",
                        conversation_text=payload_text,
                        gemini_usage=None,
                        eleven_tts_usage=None,
                        s3_key=s3_key,
                    )
                    send_billing_event_fire_and_forget(base_event)
                    self._emitted_billing_ids.add(base_key)
            except Exception as e:
                logger.warning("billing.invoice.base_event_failed", user_id=user_id, error=str(e))

            try:
                rated_key = f"{inbound_key}::rated"
                if rated_key not in self._emitted_billing_ids:
                    rated_event = generate_billing_event_v2(
                        tenant_id=TENANT_ID,
                        conversation_id=billing_conversation_id,
                        msg_type=self._billing_msg_type("image"),
                        message_id=inbound_key,
                        role="assistant",
                        channel="whatsapp",
                        conversation_text=agent_text or "",
                        gemini_usage=None,
                        eleven_tts_usage=None,
                    )
                    send_billing_event_fire_and_forget(rated_event)
                    self._emitted_billing_ids.add(rated_key)
            except Exception as e:
                logger.warning("billing.invoice.rated_event_failed", user_id=user_id, error=str(e))

        self.adk_helper.session_helper.inc_onboarding_attempts(user_id)
        self.adk_helper.session_helper.set_onboarding_status(
            user_id, "verifying_invoice", reason="image_received"
        )

        def _set_awaiting(reason: str) -> None:
            self.adk_helper.session_helper.set_onboarding_status(user_id, "awaiting_invoice", reason=reason)

        image_path = None
        try:
            if self.is_twilio:
                img_path, _ctype = self._download_twilio_media(
                    media_url,
                    message_sid=inbound_key,
                    media_content_type=mime_type,
                )
                if not img_path:
                    logger.warning("invoice.onboarding.download_failed", user_id=user_id, media_url=media_url)
                    response_text = "Image download failed. Please resend a clear invoice photo."
                    self.adk_helper._send_text_once(
                        user_id,
                        response_text,
                        reply_to_message_id=reply_to_message_id,
                    )
                    _set_awaiting("download_failed")
                    _emit_invoice_billing(response_text, s3_key=s3_key)
                    return True
                image_path = img_path
                # Upload Twilio invoice image to S3
                try:
                    with open(img_path, "rb") as f:
                        payload = f.read()
                    ct = _ctype or "image/jpeg"
                    conv_id = billing_conversation_id or self._get_conversation_id(user_id)
                    s3_key, _ = upload_bytes_to_s3(
                        payload,
                        content_type=ct,
                        user_phone=user_id,
                        conversation_id=conv_id,
                        msg_id=inbound_key or "invoice",
                        key_prefix="images",
                        base_dir="images",
                    )
                except Exception as e:
                    logger.warning("invoice.twilio.s3_upload_failed", user_id=user_id, error=str(e))
            else:
                if media_url and media_url.startswith("http"):
                    image_path = self.image_helper.download_image(media_url)
                    # Attempt S3 upload for direct URL cases
                    try:
                        with open(image_path, "rb") as f:
                            payload = f.read()
                        conv_id = billing_conversation_id or self._get_conversation_id(user_id)
                        s3_key, _ = upload_bytes_to_s3(
                            payload,
                            content_type=mime_type or "image/jpeg",
                            user_phone=user_id,
                            conversation_id=conv_id,
                            msg_id=inbound_key or "invoice",
                            key_prefix="images",
                            base_dir="images",
                        )
                    except Exception as e:
                        logger.warning("invoice.meta.url_s3_upload_failed", user_id=user_id, error=str(e))
                elif media_id:
                    media_url = self.image_helper.get_media_url(media_id)
                    image_path = self.image_helper.download_image(media_url)
                    # Upload invoice image to S3 for auditing/billing (parity with voice notes)
                    try:
                        s3_key = store_image_to_s3(
                            user_id,
                            media_id,
                            inbound_key or "",
                            billing_conversation_id,
                        )
                    except Exception as e:
                        logger.warning("invoice.image.s3_upload_failed", user_id=user_id, error=str(e))
                else:
                    logger.warning("invoice.onboarding.missing_media_id", user_id=user_id)
                    response_text = "Invoice image missing. Please resend a clear invoice photo."
                    self.adk_helper._send_text_once(
                        user_id,
                        response_text,
                        reply_to_message_id=reply_to_message_id,
                    )
                    _set_awaiting("missing_media_id")
                    _emit_invoice_billing(response_text, s3_key=s3_key)
                    return True

            if not image_path:
                logger.warning("invoice.onboarding.no_image_path", user_id=user_id)
                response_text = "Invoice image missing. Please resend a clear invoice photo."
                self.adk_helper._send_text_once(
                    user_id,
                    response_text,
                    reply_to_message_id=reply_to_message_id,
                )
                _set_awaiting("missing_image_path")
                _emit_invoice_billing(response_text, s3_key=s3_key)
                return True

            extraction = build_invoice_payload_from_image(image_path, mobile_number=user_mobile or None)
            missing = extraction.get("missing") or []
            payload = extraction.get("payload") or {}

            try:
                billing_user_text = json.dumps(
                    {
                        "invoice_payload": payload,
                        "missing": missing,
                        "invoice_type": extraction.get("invoice_type"),
                    },
                    ensure_ascii=True,
                )
                if len(billing_user_text) > 900:
                    billing_user_text = billing_user_text[:900]
            except Exception as e:
                logger.warning("billing.invoice.payload_serialize_failed", user_id=user_id, error=str(e))

            logger.info(
                "invoice.ocr.text_preview",
                user_id=user_id,
                ocr_len=len(extraction.get("ocr_text") or ""),
                ocr_preview=(extraction.get("ocr_text") or "")[:1200],
                invoice_type=extraction.get("invoice_type"),
                ocr_error=extraction.get("error"),
            )

            logger.info(
                "invoice.verify.normalized",
                user_id=user_id,
                invoice_type=extraction.get("invoice_type"),
                missing=",".join(missing),
                ocr_error=extraction.get("error"),
                ocr_len=len(extraction.get("ocr_text") or ""),
            )

            logger.info(
                "invoice.verify.payload_built",
                user_id=user_id,
                payload=payload,
                missing=missing,
            )

            if extraction.get("error") and not payload:
                logger.warning("invoice.extract.failed", user_id=user_id, error=extraction.get("error"))
                response_text = "Invoice read nahin ho saki. Baraye meherbani clear photo dubara bhej dein."
                self.adk_helper._send_text_once(
                    user_id,
                    response_text,
                    reply_to_message_id=reply_to_message_id,
                )
                _set_awaiting("ocr_failed")
                _emit_invoice_billing(response_text, s3_key=s3_key)
                return True

            if missing:
                logger.warning(
                    "invoice.verify.missing_fields",
                    user_id=user_id,
                    missing=",".join(missing),
                )
                human_missing = ", ".join(missing)
                response_text = (
                    f"Invoice ki kuch details nahi mil saki ({human_missing}). Baraye meherbani clear photo dubara bhej dein."
                )
                self.adk_helper._send_text_once(
                    user_id,
                    response_text,
                    reply_to_message_id=reply_to_message_id,
                )
                _set_awaiting("missing_fields")
                _emit_invoice_billing(response_text, s3_key=s3_key)
                return True

            logger.info(
                "invoice.verify.request",
                user_id=user_id,
                payload_json=json.dumps(payload, ensure_ascii=True),
            )
            api_result = verify_extracted_invoice(
                tenant_id=payload.get("tenant_id") or "",
                mobile_number=payload.get("mobile_number"),
                invoice_type=payload.get("invoice_type") or "",
                invoice_number=payload.get("invoice_number"),
                store_codes=payload.get("store_codes"),
            )
            logger.info(
                "invoice.verify.response",
                user_id=user_id,
                response_preview=(str(api_result)[:1200] if api_result else ""),
                response_len=len(str(api_result)) if api_result else 0,
            )
            success, message, data = self._parse_invoice_verification_response(api_result)
            logger.info(
                "invoice.verify.parsed",
                user_id=user_id,
                success=success,
                message=message,
                parsed_json=json.dumps(data or {}, ensure_ascii=True),
            )
            if success:
                self.adk_helper.session_helper.set_auth_status(user_id, True)
                self.adk_helper.session_helper.set_onboarding_status(user_id, None)
                self.adk_helper.session_helper.clear_onboarding_invoice(user_id, slot=1)
                self.adk_helper.session_helper.mark_onboarding_verified(user_id)

                ext_id = self._extract_external_user_id(data)
                if ext_id:
                    self.adk_helper.set_external_user_id(user_id, ext_id)

                store_code = (payload.get("store_codes") or [None])[0]
                if store_code:
                    meta = {
                        "store_code": store_code,
                        "storecode": store_code,
                    }
                    self.adk_helper._persist_customer_metadata(user_id, meta)

                self.adk_helper.set_name_change_state(
                    user_id,
                    True,
                    require_explicit_name=True,
                )
                logger.info("invoice.verify.name_capture_started", user_id=user_id)

                success_text = "Shukriya! Aapki invoice verify ho gai hai!"
                name_capture_text = (
                    "Baraye meherbani *apna poora naam likh dein* "
                    "takay main save kar loon."
                )
                self.adk_helper._send_text_once(
                    user_id,
                    success_text,
                    reply_to_message_id=reply_to_message_id,
                )
                self.adk_helper._send_text_once(
                    user_id,
                    name_capture_text,
                )
                _emit_invoice_billing(
                    f"{success_text}\n{name_capture_text}",
                    s3_key=s3_key,
                )
                return True

            reason = message or "verification_failed"
            self.adk_helper.session_helper.clear_onboarding_invoice(user_id, slot=1)
            self.adk_helper.session_helper.set_onboarding_status(
                user_id, "awaiting_invoice", reason=reason
            )
            logger.warning(
                "invoice.verify.reset",
                user_id=user_id,
                reason=reason,
            )
            # Custom handling for missing store code
            if "store code is required" in reason.lower():
                response_text = "Dubara clear photo bhejain invoice ki; store code nahi mila."
            else:
                response_text = self.adk_helper._get_onboarding_invoice_prompt()

            self.adk_helper._send_text_once(
                user_id,
                response_text,
                reply_to_message_id=reply_to_message_id,
            )
            _emit_invoice_billing(response_text, s3_key=s3_key)
            return True

        except Exception as e:
            logger.warning("invoice.onboarding.unexpected_error", user_id=user_id, error=str(e))
            response_text = "Invoice read nahin ho saki. Baraye meherbani clear photo dubara bhej dein."
            self.adk_helper._send_text_once(
                user_id,
                response_text,
                reply_to_message_id=reply_to_message_id,
            )
            _set_awaiting("unexpected_error")
            _emit_invoice_billing(response_text, s3_key=s3_key)
            return True
        finally:
            if image_path and os.path.exists(image_path):
                try:
                    os.remove(image_path)
                except Exception:
                    pass

    def _log_twilio_request(self, label: str, form: Optional[dict] = None, raw_payload: Optional[dict] = None) -> None:
        form = form or {}
        from_raw = form.get("WaId") or form.get("From") or ""
        to_raw = form.get("To") or ""
        logger.info(
            label,
            method=request.method,
            content_type=request.content_type,
            content_length=request.content_length,
            has_signature=bool(request.headers.get("X-Twilio-Signature")),
            form_keys=list(form.keys()),
            from_=self._mask_number(from_raw),
            to_=self._mask_number(to_raw),
            message_sid=form.get("MessageSid") or form.get("SmsMessageSid"),
            num_media=form.get("NumMedia"),
            raw_present=bool(raw_payload),
        )

    def _is_twilio_whatsapp_inbound(self, form: Optional[dict]) -> bool:
        form = form or {}
        from_raw = (form.get("From") or "").lower()
        to_raw = (form.get("To") or "").lower()
        if from_raw.startswith("whatsapp:") or to_raw.startswith("whatsapp:"):
            return True
        if form.get("WaId"):
            return True
        channel = (form.get("Channel") or form.get("MessageChannel") or "").lower()
        return channel == "whatsapp"

    def _get_twilio_form_data(self):
        """
        Parse Twilio webhook payloads in two situations:
        1) Standard Twilio POST (application/x-www-form-urlencoded) → request.form / request.values
        2) API Gateway / proxy case where the original form body is base64-encoded inside a JSON
           envelope (event['body'] + event['isBase64Encoded']).

        Returns:
            (form_dict, raw_payload_for_logging)
        """
        form = request.form or request.values or {}
        raw_payload = {}

        if form:
            raw_payload = {k: form.get(k) for k in form.keys()}
            return form, raw_payload

        # Fallback: attempt to parse a base64-encoded body (AWS/API Gateway style)
        try:
            raw_body = request.get_data() or b""
            if not raw_body:
                return {}, {}

            # Try JSON envelope first
            try:
                body_json = json.loads(raw_body.decode("utf-8"))
            except Exception:
                body_json = None

            if isinstance(body_json, dict) and "body" in body_json:
                body_content = body_json.get("body") or ""
                if body_json.get("isBase64Encoded", False):
                    try:
                        body_content = base64.b64decode(body_content).decode("utf-8")
                    except Exception as e:
                        logger.warning("twilio.webhook.b64_decode_failed", error=str(e))
                        body_content = ""

                parsed_qs = urllib.parse.parse_qs(body_content)
                flat_form = {k: (v[0] if len(v) == 1 else v) for k, v in parsed_qs.items()}

                raw_payload = {
                    "event_envelope": {k: body_json.get(k) for k in ("isBase64Encoded", "path", "httpMethod")},
                    "decoded_body": body_content,
                    "form": flat_form,
                }
                return flat_form, raw_payload

            # If no JSON envelope, treat raw body as query string form data
            body_str = raw_body.decode("utf-8", errors="ignore")
            parsed_qs = urllib.parse.parse_qs(body_str)
            flat_form = {k: (v[0] if len(v) == 1 else v) for k, v in parsed_qs.items()}
            if flat_form:
                raw_payload = {"form": flat_form}
                return flat_form, raw_payload

        except Exception as e:
            logger.warning("twilio.webhook.form_parse_failed", error=str(e))

        logger.warning(
            "twilio.webhook.no_form",
            method=request.method,
            content_type=request.content_type,
            content_length=request.content_length,
        )
        return {}, {}

    def _normalize_twilio_webhook(self):
        """
        Convert Twilio form-encoded webhook into the normalized 'value' dict
        that the existing handler expects.
        """
        form, raw_payload = self._get_twilio_form_data()
        if not form:
            logger.warning("twilio.webhook.empty_form")
            return None, {}

        if not raw_payload:
            raw_payload = {k: form.get(k) for k in form.keys()}

        logger.info("twilio.webhook.raw", payload=raw_payload)

        message_sid = form.get("MessageSid") or form.get("SmsMessageSid")
        account_sid = form.get("AccountSid")
        if account_sid and not self.twilio_account_sid:
            self.twilio_account_sid = account_sid
        elif account_sid and self.twilio_account_sid and account_sid != self.twilio_account_sid:
            logger.warning(
                "twilio.webhook.account_mismatch",
                configured=self.twilio_account_sid,
                inbound=account_sid,
            )
        from_raw = form.get("WaId") or form.get("From") or ""
        to_raw = form.get("To") or ""
        profile_name = form.get("ProfileName") or self.twilio_profile_fallback or "Unknown"
        body = form.get("Body", "") or ""

        # Optional: try to enrich profile/from using ChannelMetadata payload
        channel_meta_raw = form.get("ChannelMetadata")
        if channel_meta_raw:
            try:
                channel_meta = json.loads(channel_meta_raw)
                meta_ctx = (channel_meta.get("data") or {}).get("context") or {}
                profile_from_meta = meta_ctx.get("ProfileName") or channel_meta.get("ProfileName")
                waid_from_meta = meta_ctx.get("WaId")
                if profile_from_meta and profile_name == self.twilio_profile_fallback:
                    profile_name = profile_from_meta
                if waid_from_meta and not from_raw:
                    from_raw = waid_from_meta
            except Exception as e:
                logger.warning("twilio.webhook.channelmeta_parse_failed", error=str(e))

        try:
            num_media = int(form.get("NumMedia", "0") or 0)
        except Exception:
            num_media = 0

        ts_raw = form.get("Timestamp") or form.get("SentTimestamp") or ""
        try:
            ts_float = float(ts_raw)
            if ts_float > 10_000_000_000:  # millis
                ts_float = ts_float / 1000.0
        except Exception:
            ts_float = time.time()

        msg = {
            "id": message_sid or f"twilio-{int(time.time() * 1000)}",
            "from": self._sanitize_wa_number(from_raw),
            "timestamp": ts_float,
            "to": self._sanitize_wa_number(to_raw),
        }

        replied_to = form.get("OriginalRepliedMessageSid") or form.get("QuotedMessageSid")
        if replied_to:
            msg["context"] = {"id": replied_to}

        order_details_raw = form.get("OrderDetails") or form.get("order")

        if num_media > 0:
            media_url = form.get("MediaUrl0")
            media_type = (form.get("MediaContentType0") or "").lower()
            if media_type.startswith("audio"):
                msg["type"] = "audio"
                msg["audio"] = {"id": message_sid or media_url, "url": media_url, "mime_type": media_type}
                if body:
                    msg["text"] = {"body": body}
            elif media_type.startswith("image"):
                msg["type"] = "image"
                msg["image"] = {"id": message_sid or media_url, "url": media_url, "mime_type": media_type}
                if body:
                    msg["text"] = {"body": body}
            else:
                msg["type"] = "text"
                msg["text"] = {"body": body or "[Unsupported media type]"}
        elif order_details_raw:
            try:
                order_data = json.loads(order_details_raw or "{}")
            except Exception:
                order_data = {}
            msg["type"] = "order"
            msg["order"] = order_data
        elif form.get("Latitude") and form.get("Longitude"):
            try:
                lat = float(form.get("Latitude"))
                lon = float(form.get("Longitude"))
                msg["type"] = "location"
                msg["location"] = {
                    "latitude": lat,
                    "longitude": lon,
                    "name": form.get("Address") or form.get("Label") or "",
                    "address": form.get("Address") or "",
                }
            except Exception:
                msg["type"] = "text"
                msg["text"] = {"body": body}
        else:
            msg["type"] = "text"
            msg["text"] = {"body": body}

        value = {
            "messages": [msg],
            "contacts": [{"profile": {"name": profile_name}}],
            "provider": "twilio",
        }
        return value, raw_payload

    def handle_twilio_status_post(self):
        """
        Twilio status callback: map delivery/read to session touch (parity with Meta statuses).
        """
        form = request.form or request.values or {}
        if not form:
            return make_response("", 200)

        self._log_twilio_request("twilio.status.hit", form=form)
        status = (form.get("MessageStatus") or "").lower()
        to_raw = form.get("To") or ""
        user_id = self._sanitize_wa_number(to_raw)

        if status in ("delivered", "read", "sent") and user_id:
            try:
                if SESSION_TOUCH_ON_STATUS:
                    self.adk_helper.session_helper.touch(user_id, source="agent")
            except Exception as e:
                logger.warning("twilio.status.touch_error", user_id=user_id, error=str(e))

        logger.info("twilio.status.callback", status=status, user_id=user_id, payload={k: form.get(k) for k in form.keys()})
        return make_response("", 200)

    def _download_twilio_media(
        self,
        media_url: str,
        *,
        message_sid: Optional[str] = None,
        media_content_type: Optional[str] = None,
    ):
        """
        Download Twilio media URL to a temp file. Returns (path, content_type).
        """
        if not media_url:
            return None, None

        def _extract_account_sid(url: str) -> str:
            match = re.search(r"/Accounts/([^/]+)/", url or "")
            return match.group(1) if match else ""

        def _extract_media_sid(url: str) -> str:
            match = re.search(r"/Media/([^/?]+)", url or "")
            return match.group(1) if match else ""

        resource_account_sid = _extract_account_sid(media_url) or self.twilio_account_sid or ""
        auth_account_sid = resource_account_sid or self.twilio_account_sid
        auth_token = self.twilio_auth_token
        auth = (auth_account_sid, auth_token) if auth_account_sid and auth_token else None
        if auth_account_sid and resource_account_sid and auth_account_sid != resource_account_sid:
            logger.info(
                "twilio.media.account_mismatch",
                auth_account_sid=auth_account_sid,
                resource_account_sid=resource_account_sid,
            )

        def _suffix_for_type(content_type: str) -> str:
            ctype = (content_type or "").lower()
            if "ogg" in ctype:
                return ".ogg"
            if "mp3" in ctype or "mpeg" in ctype:
                return ".mp3"
            if "wav" in ctype:
                return ".wav"
            if "image" in ctype:
                return ".jpg"
            return ".bin"

        def _save_response(resp):
            ctype = (resp.headers.get("Content-Type") or media_content_type or "").lower()
            suffix = _suffix_for_type(ctype)
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                path = f.name
            return path, ctype

        def _try_download(url: str):
            try:
                with requests.get(url, auth=auth, stream=True, timeout=60) as resp:
                    if 200 <= resp.status_code < 300:
                        return _save_response(resp), resp.status_code
                    body = resp.text[:300] if resp.text else ""
                    logger.warning(
                        "twilio.media.http_failed",
                        status=resp.status_code,
                        url=url,
                        account_sid=auth_account_sid,
                        body=body,
                    )
                    return (None, None), resp.status_code
            except Exception as e:
                logger.error("twilio.media.download_error", error=str(e), url=url, account_sid=auth_account_sid)
                return (None, None), None

        if not auth:
            logger.error(
                "twilio.media.missing_creds",
                have_account_sid=bool(auth_account_sid),
                have_auth_token=bool(auth_token),
            )
            return None, None

        message_sid = message_sid or ""
        media_sid = _extract_media_sid(media_url)

        (path, ctype), status = _try_download(media_url)
        if not path and status == 404:
            try:
                time.sleep(0.6)
            except Exception:
                pass
            (path, ctype), status = _try_download(media_url)
        if path:
            return path, ctype

        def _resolve_media_url_from_message() -> Optional[str]:
            if not message_sid:
                return None
            url = f"https://api.twilio.com/2010-04-01/Accounts/{resource_account_sid}/Messages/{message_sid}/Media.json"
            try:
                r = requests.get(url, auth=auth, timeout=20)
                if not (200 <= r.status_code < 300):
                    logger.warning(
                        "twilio.media.list_failed",
                        status=r.status_code,
                        message_sid=message_sid,
                        account_sid=resource_account_sid,
                        body=(r.text or "")[:300],
                    )
                    return None
                data = r.json() or {}
                items = data.get("media") or []
                if not items:
                    logger.warning(
                        "twilio.media.list_empty",
                        message_sid=message_sid,
                        account_sid=resource_account_sid,
                    )
                    return None
                chosen = None
                if media_sid:
                    for item in items:
                        if item.get("sid") == media_sid:
                            chosen = item
                            break
                if not chosen:
                    chosen = items[0]
                uri = chosen.get("uri") or ""
                if not uri:
                    return None
                if uri.endswith(".json"):
                    uri = uri[: -len(".json")]
                resolved = f"https://api.twilio.com{uri}"
                return resolved
            except Exception as e:
                logger.warning(
                    "twilio.media.list_error",
                    error=str(e),
                    message_sid=message_sid,
                    account_sid=resource_account_sid,
                )
                return None

        fallback_url = _resolve_media_url_from_message()
        if fallback_url and fallback_url != media_url:
            (path, ctype), _status = _try_download(fallback_url)
            if path:
                logger.info(
                    "twilio.media.fallback_downloaded",
                    message_sid=message_sid,
                    media_sid=media_sid,
                    account_sid=resource_account_sid,
                )
                return path, ctype

        logger.error(
            "twilio.media.download_failed",
            status=status,
            media_url=media_url,
            message_sid=message_sid,
            media_sid=media_sid,
            account_sid=resource_account_sid,
        )
        return None, None

    def _get_conversation_id(self, user_id: str) -> str:
        """
        Return a stable conversation id for billing/analytics.

        Preferred source:
        - Active conversation_id from SessionStore (tenants/{tenant}/agent_id/{agent_id}/users/{user_id}/conversations/meta).

        Fallbacks (legacy):
        - user_id + current session_id
        - user_id alone if all else fails.
        """
        try:
            # NEW: ask SessionStore for active conversation id
            conv_id = self.adk_helper.session_helper.get_active_conversation_id(user_id)
            if conv_id:
                return conv_id

            # Fallback to legacy session-based id
            session_id = self.adk_helper.get_current_session_id(user_id)
            if session_id:
                return f"{user_id}-{session_id}"

            return user_id
        except Exception as e:
            logger.warning("conversation_id.id_failed", user_id=user_id, error=str(e))
            # Last-resort fallback
            try:
                session_id = self.adk_helper.get_current_session_id(user_id)
                if session_id:
                    return f"{user_id}-{session_id}"
            except Exception:
                pass
            return user_id

    def _billing_msg_type(self, raw_type: str) -> str:
        """
        Normalize billing msg_type to the supported set.
        """
        normalized = (raw_type or "").strip().lower()
        direct = {
            "text": "text",
            "audio": "audio",
            "image": "image",
            "order": "order",
            "location": "location",
            "interactive": "interactive",
        }
        if normalized in direct:
            return direct[normalized]

        if "order" in normalized:
            return "order"
        if "audio" in normalized or "voice" in normalized:
            return "audio"
        if "image" in normalized or "photo" in normalized or "pic" in normalized:
            return "image"
        if "location" in normalized:
            return "location"
        if "interactive" in normalized or "button" in normalized:
            return "interactive"

        logger.info("billing.msg_type.coerced", raw=raw_type, coerced="text")
        return "text"

    def _send_location_request(self, user_id: str, reply_to_message_id: Optional[str] = None) -> bool:
        """
        Send a WhatsApp interactive location-request message so the user can share a pin.
        Currently enabled only for Meta transport and YTL tenant.
        """
        if self.is_twilio:
            return False

        tenant = (TENANT_ID or "").strip().lower()
        if tenant != "ytl":
            return False

        if not (WA_GRAPH_URL and WA_ACCESS_TOKEN and user_id):
            logger.warning(
                "wa.location_request.skip",
                reason="missing_creds_or_params",
                have_url=bool(WA_GRAPH_URL),
                have_token=bool(WA_ACCESS_TOKEN),
                have_to=bool(user_id),
            )
            return False

        body_text = "Please tap the button to share your site location pin."

        payload = {
            "messaging_product": "whatsapp",
            "to": user_id,
            "type": "interactive",
            "interactive": {
                "type": "location_request_message",
                "body": {"text": body_text},
                "action": {"name": "send_location"},
            },
        }

        if reply_to_message_id:
            payload["context"] = {"message_id": reply_to_message_id}

        headers = {
            "Authorization": f"Bearer {WA_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(WA_GRAPH_URL, headers=headers, json=payload, timeout=15)
            if 200 <= resp.status_code < 300:
                logger.info("wa.location_request.sent", user_id=user_id)
                return True
            logger.warning(
                "wa.location_request.fail",
                user_id=user_id,
                status=resp.status_code,
                body=(resp.text or "")[:300] if resp.text else "",
            )
            return False
        except Exception as e:
            logger.error("wa.location_request.error", user_id=user_id, error=str(e))
            return False

    # --- WhatsApp message reactions (non-blocking helper) ---
    def _send_reaction(self, to_number: str, message_id: str, emoji: str) -> bool:
        """
        Best-effort reaction sender. Does not raise; logs outcome.
        """
        if self.is_twilio:
            return False
        if not (WA_GRAPH_URL and WA_ACCESS_TOKEN and to_number and message_id and emoji):
            logger.warning(
                "wa.reaction.skip",
                reason="missing_creds_or_params",
                have_url=bool(WA_GRAPH_URL),
                have_token=bool(WA_ACCESS_TOKEN),
                have_to=bool(to_number),
                have_msg=bool(message_id),
                have_emoji=bool(emoji),
            )
            return False

        payload = {
            "messaging_product": "whatsapp",
            "to": to_number,
            "type": "reaction",
            "reaction": {"message_id": message_id, "emoji": emoji},
        }
        headers = {
            "Authorization": f"Bearer {WA_ACCESS_TOKEN}",
            "Content-Type": "application/json",
        }

        try:
            resp = requests.post(WA_GRAPH_URL, headers=headers, json=payload, timeout=8)
            if 200 <= resp.status_code < 300:
                logger.info("wa.reaction.sent", user_id=to_number, message_id=message_id, emoji=emoji)
                return True
            logger.warning(
                "wa.reaction.fail",
                user_id=to_number,
                message_id=message_id,
                emoji=emoji,
                status=resp.status_code,
                body=resp.text[:300],
            )
            return False
        except Exception as e:
            logger.error("wa.reaction.error", error=str(e), user_id=to_number, message_id=message_id, emoji=emoji)
            return False

    def _kickoff_voice_reaction(self, user_id: str, inbound_key: str):
        """
        Fire-and-forget: 👂 immediately, then ✅ after VM_REACTION_SWAP_SEC.
        Never blocks the main webhook path.
        """
        if self.is_twilio:
            return
        if not VM_REACTION_ENABLED:
            return
        if not (user_id and inbound_key):
            return

        def _run_sequence():
            try:
                self._send_reaction(user_id, inbound_key, "👂")
                try:
                    time.sleep(VM_REACTION_SWAP_SEC)
                except Exception:
                    pass
                self._send_reaction(user_id, inbound_key, "✅")
            except Exception as e:
                logger.warning("wa.reaction.thread_error", error=str(e), user_id=user_id, message_id=inbound_key)

        try:
            threading.Thread(target=_run_sequence, daemon=True).start()
        except Exception as e:
            logger.warning("wa.reaction.spawn_failed", error=str(e), user_id=user_id, message_id=inbound_key)


    # --- NEW: mark inbound message as read on the Business number ---
    def _mark_read(self, message_id: str):
        """
        Marks a user inbound message as READ (Cloud API read receipts) and shows typing indicator.
        The typing indicator will auto-dismiss after 25 seconds or when you send a response.
        Controlled by READ_RECEIPTS_ENABLED (default true).
        """
        if self.is_twilio:
            return
        if not (WA_GRAPH_URL and WA_ACCESS_TOKEN and message_id):
            logger.warning(
                "wa.mark_read_typing.skip",
                reason="missing_creds_or_msgid",
                have_url=bool(WA_GRAPH_URL),
                have_token=bool(WA_ACCESS_TOKEN),
                have_msg=bool(message_id),
            )
            return

        try:
            payload = {
                "messaging_product": "whatsapp",
                "status": "read",
                "message_id": message_id,
                "typing_indicator": {"type": "text"},  # This triggers the typing dots
            }
            headers = {
                "Authorization": f"Bearer {WA_ACCESS_TOKEN}",
                "Content-Type": "application/json",
            }

            resp = requests.post(WA_GRAPH_URL, headers=headers, json=payload, timeout=8)

            if 200 <= resp.status_code < 300:
                logger.info("wa.mark_read_typing.success", message_id=message_id)
            else:
                logger.warning(
                    "wa.mark_read_typing.fail",
                    message_id=message_id,
                    status=resp.status_code,
                    body=resp.text[:500],
                )
        except Exception as e:
            logger.error("wa.mark_read_typing.error", error=str(e), message_id=message_id)
    
    def _schedule_buffer_drain(self, user_id: str, generation: int):
        """
        Schedules a task to run in 3 seconds.
        We pass the 'generation' ID. If the generation changes before the task runs,
        the task will know it's obsolete and exit.
        """
        if not self.tasks_client or not DRAIN_URL:
            logger.error("Cloud Tasks not configured. Skipping buffer.")
            return

        # 5 Second Buffer Window
        run_at = datetime.datetime.now(datetime.timezone.utc) + timedelta(seconds=5)
        timestamp = timestamp_pb2.Timestamp()
        timestamp.FromDatetime(run_at)

        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": DRAIN_URL,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({
                    "user_id": user_id,
                    "generation": generation
                }).encode()
            },
            "schedule_time": timestamp
        }

        try:
            self.tasks_client.create_task(request={"parent": self.parent_queue, "task": task})
            logger.info(f"Buffered task scheduled for {user_id} (Gen: {generation})")
        except Exception as e:
            logger.error(f"Failed to schedule task: {e}")

    def handle_drain_buffer(self):
        data = request.get_json() or {}
        user_id = data.get("user_id")
        task_generation = data.get("generation")

        logger.info(f"Drain task woke up for {user_id} (Gen: {task_generation})")

        # NEW: Get all context
        combined_text, reply_to_id, conversation_id, session_id, buffered_message_id = self.message_buffer.pop_all(
            user_id, 
            task_generation
        )

        if not combined_text:
            logger.info("Buffer generation mismatch or empty. Newer task exists. Aborting.")
            return make_response("Obsolete", 200)

        logger.info(f"Processing buffered chunk: {combined_text.replace(chr(10), ' ')}")

        # NEW: Restore conversation context before processing
        if conversation_id:
            try:
                # Touch the conversation so it doesn't expire
                self.adk_helper.session_helper.touch_conversation(user_id, source="user")
                logger.info(
                    "drain.conversation.restored",
                    user_id=user_id,
                    conversation_id=conversation_id,
                )
            except Exception as e:
                logger.warning("drain.conversation.touch_failed", error=str(e))

        # Process with restored context
        agent_response = None
        try:
            buffer_inbound_key = f"buffer:{user_id}:{task_generation}"
            agent_response = self.adk_helper.handle_message(
                combined_text,
                user_id,
                is_voice_input=False,
                inbound_key=buffer_inbound_key,
                reply_to_message_id=reply_to_id
            )
        except Exception as e:
            logger.error(f"Error processing buffered message: {e}")
            return make_response("Processed", 200)

        # Emit billing for drained buffered text (base + rated)
        if not buffered_message_id:
            logger.warning(
                "billing.buffered_text.skip_missing_message_id",
                user_id=user_id,
                generation=task_generation,
            )
            return make_response("Processed", 200)

        try:
            base_key = f"{buffered_message_id}::base"
            if base_key not in self._emitted_billing_ids:
                base_event = generate_billing_event_v2(
                    tenant_id=TENANT_ID,
                    conversation_id=self._get_conversation_id(user_id),
                    msg_type=self._billing_msg_type("text"),
                    message_id=buffered_message_id,
                    role="user",
                    channel="whatsapp",
                    conversation_text=combined_text,
                    gemini_usage=None,
                    eleven_tts_usage=None,
                )
                send_billing_event_fire_and_forget(base_event)
                self._emitted_billing_ids.add(base_key)
        except Exception as e:
            logger.warning("billing.buffered_text.base_event_failed", error=str(e), user_id=user_id, generation=task_generation)

        try:
            rated_key = f"{buffered_message_id}::rated"
            if rated_key not in self._emitted_billing_ids:
                rated_event = generate_billing_event_v2(
                    tenant_id=TENANT_ID,
                    conversation_id=self._get_conversation_id(user_id),
                    msg_type=self._billing_msg_type("text"),
                    message_id=buffered_message_id,
                    role="assistant",
                    channel="whatsapp",
                    conversation_text=agent_response or "",
                    gemini_usage=self.adk_helper.get_last_gemini_usage(user_id),
                    eleven_tts_usage=None,
                )
                send_billing_event_fire_and_forget(rated_event)
                self._emitted_billing_ids.add(rated_key)
        except Exception as e:
            logger.warning("billing.buffered_text.rated_event_failed", error=str(e), user_id=user_id, generation=task_generation)

        return make_response("Processed", 200)

    async def _send_sample_invoice_vn_async(
        self,
        user_id: str,
        reply_to_message_id: Optional[str] = None
    ) -> bool:
        """
        Send a cached voice note to the user explaining the sample invoice.
        Bypasses Gemini text prep to save background-thread CPU time.
        """
        try:
            vn_script = (
                "acha theek hai bhai ye dekh lain meine sample invoice bhej dee hai "
                "bilkul issi tarhan say aapne tasweer lay kr bhejni hogi takkay mein "
                "aapka store code sahi say dekh sakoon"
            )
            cache_key = "sample_invoice_vn_v1"

            try:
                cached_vn = await asyncio.to_thread(self._get_cached_onboarding_vn, cache_key)
            except Exception as e:
                logger.warning("sample_invoice_vn.cache_get_error", user_id=user_id, error=str(e))
                cached_vn = None

            if cached_vn:
                sent = await self.adk_helper._upload_and_send_audio(
                    user_id,
                    cached_vn["audio_bytes"],
                    voice=cached_vn.get("is_voice", True),
                    mp3=cached_vn.get("is_mp3", False),
                    meta=cached_vn.get("meta", {}),
                    reply_to_message_id=reply_to_message_id,
                )
                if sent:
                    return True

            # Cache miss: Generate audio directly via TTS (Skip Gemini to avoid CPU throttling)
            preferred_bytes, meta, mp3_bytes = await self.adk_helper._tts_get_audio_bytes(vn_script)

            audio_bytes = preferred_bytes or mp3_bytes
            is_voice = bool(preferred_bytes)
            is_mp3 = bool(mp3_bytes and not preferred_bytes)
            meta_to_use = meta or ({"mime": "audio/mpeg", "path": "http_or_stream_mp3"} if mp3_bytes else {})

            if audio_bytes:
                await asyncio.to_thread(
                    self._cache_onboarding_vn,
                    cache_key,
                    audio_bytes,
                    meta_to_use,
                    is_voice,
                    is_mp3,
                )
                sent = await self.adk_helper._upload_and_send_audio(
                    user_id,
                    audio_bytes,
                    voice=is_voice,
                    mp3=is_mp3,
                    meta=meta_to_use,
                    reply_to_message_id=reply_to_message_id,
                )
                return sent

        except Exception as e:
            logger.error("sample_invoice_vn.error", user_id=user_id, error=str(e))
        return False

    async def _send_onboarding_vn_async(
        self,
        user_id: str,
        *,
        step: int = 1,
        reply_to_message_id: Optional[str] = None
    ):
        """
        Send a voice note explaining the onboarding process.
        Uses cached VN if available, otherwise generates and caches it.
        
        Args:
            user_id: WhatsApp user ID
            step: Onboarding step (kept for compatibility; single-invoice flow uses 1)
            reply_to_message_id: Message ID to reply to
        """
        try:
            if not os.getenv("ONBOARDING_VN_ENABLED", "true").lower() == "true":
                logger.info("onboarding.vn.disabled", user_id=user_id)
                return

            vn_script = (
                "Aap hamare system mein verified nahi hain. "
                "Baraye meherbani apni invoice ki clear photo bhej dein. "
                "Sirf ek invoice ki tasveer chahiye hogi, verification ke baad aap asaani se order de sakte hain."
            )
            cache_key = "onboarding_vn_invoice"

            logger.info(
                "onboarding.vn.start",
                user_id=user_id,
                step=step,
                cache_key=cache_key,
                script_len=len(vn_script),
            )

            try:
                cached_vn = await asyncio.to_thread(
                    self._get_cached_onboarding_vn,
                    cache_key
                )
            except Exception as e:
                logger.warning(
                    "onboarding.vn.cache_get_error",
                    user_id=user_id,
                    error=str(e),
                )
                cached_vn = None

            if cached_vn:
                logger.info(
                    "onboarding.vn.using_cache",
                    user_id=user_id,
                    step=step,
                    cache_key=cache_key,
                    size=len(cached_vn["audio_bytes"]),
                )

                try:
                    sent = await self.adk_helper._upload_and_send_audio(
                        user_id,
                        cached_vn["audio_bytes"],
                        voice=cached_vn.get("is_voice", True),
                        mp3=cached_vn.get("is_mp3", False),
                        meta=cached_vn.get("meta", {}),
                        reply_to_message_id=reply_to_message_id,
                    )

                    if sent:
                        logger.info("onboarding.vn.sent_from_cache", user_id=user_id, step=step)
                        return True
                    else:
                        logger.warning(
                            "onboarding.vn.cache_send_failed",
                            user_id=user_id,
                            step=step,
                        )
                except Exception as e:
                    logger.warning(
                        "onboarding.vn.cache_send_exception",
                        user_id=user_id,
                        error=str(e),
                    )

            try:
                tts_text = await self.adk_helper._prepare_vn_text(vn_script)
                preferred_bytes, meta, mp3_bytes = await self.adk_helper._tts_get_audio_bytes(tts_text)

                audio_bytes = preferred_bytes or mp3_bytes
                is_voice = bool(preferred_bytes)
                is_mp3 = bool(mp3_bytes and not preferred_bytes)
                meta_to_use = meta or ({"mime": "audio/mpeg", "path": "http_or_stream_mp3"} if mp3_bytes else {})

                if audio_bytes:
                    await asyncio.to_thread(
                        self._cache_onboarding_vn,
                        cache_key,
                        audio_bytes,
                        meta_to_use,
                        is_voice,
                        is_mp3,
                    )

                    sent = await self.adk_helper._upload_and_send_audio(
                        user_id,
                        audio_bytes,
                        voice=is_voice,
                        mp3=is_mp3,
                        meta=meta_to_use,
                        reply_to_message_id=reply_to_message_id,
                    )

                    if sent:
                        logger.info("onboarding.vn.sent_generated", user_id=user_id, step=step)
                        return True
                    else:
                        logger.warning(
                            "onboarding.vn.generated_send_failed",
                            user_id=user_id,
                            step=step,
                        )
                else:
                    logger.warning(
                        "onboarding.vn.generate_failed",
                        user_id=user_id,
                        step=step,
                    )
            except Exception as e:
                logger.error(
                    "onboarding.vn.generate_error",
                    user_id=user_id,
                    step=step,
                    error=str(e),
                )

        except Exception as e:
            logger.error("onboarding.vn.error", user_id=user_id, error=str(e))
        return False

    async def _send_onboarding_sequence(
        self,
        user_id: str,
        rejection_msg: str,
        *,
        step: int = 1,
        reply_to_message_id: Optional[str] = None,
        vn_timeout: float = 45.0,          
    ) -> None:
        """
        Send the unverified-user sequence in the correct order:
        1. Onboarding VN (Wait for it)
        2. Unverified text
        3. MP4/GIF (Video)
        4. Sample Invoice VN
        5. Sample Invoice Image
        """
        def _send_media_sequence_in_thread():
            async def _async_runner():
                try:
                    # 1. Send the Video (GIF/MP4)
                    await self.adk_helper._send_greeting_gif(user_id)
                    
                    # Wait briefly for pacing
                    await asyncio.sleep(1.5)
                    
                    # 2. Send the second Voice Note
                    await self._send_sample_invoice_vn_async(user_id, reply_to_message_id=reply_to_message_id)
                    
                    # Wait briefly before image
                    await asyncio.sleep(1.5)
                    
                    # 3. Send the Sample Image
                    await self.adk_helper._send_sample_invoice_image(user_id, reply_to_message_id=reply_to_message_id)
                    
                except Exception as e:
                    logger.error("onboarding_media_sequence_bg_thread.error", error=str(e))

            def _runner():
                asyncio.run(_async_runner())

            threading.Thread(target=_runner, daemon=True).start()

        # Start the primary Onboarding VN in a cancellable future
        vn_task = asyncio.ensure_future(
            self._send_onboarding_vn_async(
                user_id, step=step, reply_to_message_id=reply_to_message_id
            )
        )

        try:
            # Wait up to vn_timeout seconds for the initial VN to finish
            vn_sent = await asyncio.wait_for(asyncio.shield(vn_task), timeout=vn_timeout)

            # ── NORMAL PATH ─────────────────────────────────────────────────────
            logger.info("onboarding.sequence.vn_in_time", user_id=user_id)
            self.adk_helper._send_text_once(
                user_id, rejection_msg, reply_to_message_id=reply_to_message_id
            )
            if vn_sent:
                _send_media_sequence_in_thread()

        except asyncio.TimeoutError:
            # ── TIMEOUT PATH ─────────────────────────────────────────────────────
            logger.info(
                "onboarding.sequence.vn_timeout_send_text_first",
                user_id=user_id,
                timeout=vn_timeout,
            )
            self.adk_helper._send_text_once(
                user_id, rejection_msg, reply_to_message_id=reply_to_message_id
            )
            
            # Fire the sequence. The first VN will arrive whenever it finishes processing.
            _send_media_sequence_in_thread()

            def _on_vn_done(task):
                try:
                    if task.cancelled() or task.exception():
                        return
                    logger.info("onboarding.sequence.late_vn_done", user_id=user_id)
                except Exception as e:
                    logger.error(
                        "onboarding.sequence.on_vn_done_error", user_id=user_id, error=str(e)
                    )

            vn_task.add_done_callback(_on_vn_done)

        except Exception as e:
            logger.warning("onboarding.sequence.error", user_id=user_id, error=str(e))
            self.adk_helper._send_text_once(
                user_id, rejection_msg, reply_to_message_id=reply_to_message_id
            )

    async def _send_vn_async(
        self,
        user_id: str,
        text: str,
        *,
        reply_to_message_id: Optional[str] = None,
    ) -> bool:
        """
        Best-effort short VN sender for utility prompts (name capture, confirmations).
        Disabled by default via NAME_CAPTURE_VN_ENABLED=false.
        """
        try:
            if os.getenv("NAME_CAPTURE_VN_ENABLED", "false").lower() != "true":
                return False
            if not text or not text.strip():
                return False

            tts_text = await self.adk_helper._prepare_vn_text(text)
            preferred_bytes, meta, mp3_bytes = await self.adk_helper._tts_get_audio_bytes(tts_text)
            audio_bytes = preferred_bytes or mp3_bytes
            if not audio_bytes:
                return False
            return await self.adk_helper._upload_and_send_audio(
                user_id,
                audio_bytes,
                voice=bool(preferred_bytes),
                mp3=bool(mp3_bytes and not preferred_bytes),
                meta=meta or {},
                reply_to_message_id=reply_to_message_id,
            )
        except Exception as e:
            logger.warning("name_change.vn_send_failed", user_id=user_id, error=str(e))
            return False

    async def _maybe_handle_name_change(
        self,
        user_id: str,
        text: str,
        *,
        replied_to_id: Optional[str],
        name: str,
        inbound_key: Optional[str],
        results: list,
    ) -> bool:
        """
        Checks whether text (from a typed message OR a transcribed voice note)
        is part of the name-change flow.

        Returns True if the flow handled this message (caller should `continue`).
        """
        _NAME_CHANGE_TRIGGERS = [
            "naam change", "naam badal", "apna naam", "mera naam",
            "name change", "naam update", "naam edit",
        ]
        _CONFIRM_UNCHANGED = {
            "haan", "han", "ji", "sahi", "theek", "ok", "okay",
            "yes", "correct", "bilkul", "ha", "haa",
        }

        def _clean_known_name(val: Optional[str]) -> Optional[str]:
            if val is None:
                return None
            s = str(val).strip()
            if not s:
                return None
            if s.lower() in {"unknown", "unknown customer", "none", "null", "n/a", "na"}:
                return None
            return s

        def _is_plausible_name(candidate: Optional[str]) -> bool:
            if not candidate:
                return False
            c = str(candidate).strip()
            if not c:
                return False
            parts = [p for p in c.replace("-", " ").split() if p]
            if not parts or len(parts) > 3:
                return False
            blocked = {
                "catalog", "order", "cart", "checkout", "confirm", "place", "bhejo", "send",
                "price", "rate", "promotion", "offer", "invoice", "help",
                "hello", "hi", "salam", "assalam", "aoa", "bro", "bhai",
                "madam", "sir", "yes", "no", "ok", "okay", "theek", "sahi",
            }
            if any(p.lower() in blocked for p in parts):
                return False
            if sum(len(p) for p in parts) < 3:
                return False
            return True

        in_flow = self.adk_helper.get_name_change_state(user_id)
        require_explicit_name = (
            self.adk_helper.get_name_change_require_explicit(user_id) if in_flow else False
        )
        text_l = (text or "").lower()

        # ----------------------------------------------------------------
        # Not in flow and no trigger word → nothing to do
        # ----------------------------------------------------------------
        if not in_flow and not any(t in text_l for t in _NAME_CHANGE_TRIGGERS):
            return False

        # ----------------------------------------------------------------
        # Trigger detected — start the flow
        # ----------------------------------------------------------------
        if not in_flow:
            stored_meta  = self.adk_helper._get_stored_customer_metadata(user_id)
            current_name = _clean_known_name(stored_meta.get("customer_name"))

            if current_name:
                prompt_msg = (
                    f"System mein aapka ye naam aa raha hai: {current_name}. "
                    "Ye sahi hai ya change kroon?"
                )
            else:
                prompt_msg = (
                    "System mein aapka koi naam stored nahi hai. "
                    "Apna naam likhein aur main save kar deta hoon."
                )

            self.adk_helper.set_name_change_state(
                user_id,
                True,
                require_explicit_name=False,
            )
            self.adk_helper._send_text_once(
                user_id, prompt_msg, reply_to_message_id=replied_to_id,
            )
            try:
                await self._send_vn_async(user_id, prompt_msg, reply_to_message_id=replied_to_id)
            except Exception as e:
                logger.warning("name_change.vn_failed", user_id=user_id, error=str(e))

            results.append({"name": name, "message": text,
                            "flow": "name_change_initiated", "current_name": current_name})
            return True

        # ----------------------------------------------------------------
        # Already in flow — user is replying
        # ----------------------------------------------------------------

        # First: try to pull a name out of whatever they said.
        # This handles "haan mera naam sakina nahi ahmed hai" → "Ahmed"
        # as well as a plain "Ahmed Khan".
        stored_meta = self.adk_helper._get_stored_customer_metadata(user_id)
        current_name = _clean_known_name(stored_meta.get("customer_name"))
        extracted_name = self.adk_helper.extract_name_from_text(text)
        cleaned = _clean_known_name(extracted_name)
        new_name = cleaned if _is_plausible_name(cleaned) else None

        # If no name found and reply looks like plain confirmation -> no change
        # Only valid for manual name-change when a current name already exists.
        if (
            not new_name
            and (not require_explicit_name)
            and current_name
            and any(w in text_l.split() for w in _CONFIRM_UNCHANGED)
        ):
            self.adk_helper.set_name_change_state(user_id, False)
            confirm_msg = "Theek hai, naam same rakha gaya!"
            self.adk_helper._send_text_once(
                user_id, confirm_msg, reply_to_message_id=replied_to_id,
            )
            try:
                await self._send_vn_async(user_id, confirm_msg, reply_to_message_id=replied_to_id)
            except Exception as e:
                logger.warning("name_change.vn_failed", user_id=user_id, error=str(e))
            results.append({"name": name, "message": text,
                            "flow": "name_change_confirmed_unchanged"})
            return True

        if new_name:
            self.adk_helper._persist_customer_metadata(user_id, {"customer_name": new_name})
            try:
                phone = str(user_id).strip()
                api_result = update_customer_name(phone=phone, contact_name=new_name)
                ok, _data, err = unwrap_tool_response(api_result, system_name="update_customer_name")
                if not ok:
                    logger.warning(
                        "name_change.backend_sync_failed",
                        user_id=user_id,
                        phone=phone,
                        error=err,
                    )
                else:
                    logger.info(
                        "name_change.backend_sync_ok",
                        user_id=user_id,
                        phone=phone,
                    )
            except Exception as e:
                logger.warning("name_change.backend_sync_exception", user_id=user_id, error=str(e))
            self.adk_helper.set_name_change_state(user_id, False)
            if require_explicit_name:
                session_id = self.adk_helper._get_cached_session_id(user_id)
                try:
                    await asyncio.to_thread(send_product_catalogue, user_id, session_id)
                except Exception as e:
                    logger.warning("name_capture.catalog_send_failed", user_id=user_id, error=str(e))

                greet_msg = f"Assalamualaikum {new_name} bhai, ab aap order laga sakte hain!"
                self.adk_helper._send_text_once(
                    user_id, greet_msg, reply_to_message_id=replied_to_id,
                )
                results.append(
                    {
                        "name": name,
                        "message": text,
                        "flow": "name_capture_complete",
                        "new_customer_name": new_name,
                    }
                )
            else:
                confirm_msg = f"Theek hai! Aapka naam update ho gaya: {new_name}"
                self.adk_helper._send_text_once(
                    user_id, confirm_msg, reply_to_message_id=replied_to_id,
                )
                try:
                    await self._send_vn_async(user_id, confirm_msg, reply_to_message_id=replied_to_id)
                except Exception as e:
                    logger.warning("name_change.vn_failed", user_id=user_id, error=str(e))
                results.append({"name": name, "message": text,
                                "flow": "name_change_complete", "new_customer_name": new_name})
            return True

        # Couldn't parse anything useful — ask again
        if require_explicit_name:
            retry_msg = (
                "Maafi, naam samajh nahi aaya. "
                "Baraye meherbani sirf apna poora naam likhein, jaise: Ahmed Khan"
            )
        else:
            retry_msg = (
                "Maafi, naam samajh nahi aaya. "
                "Baraye meherbani sirf apna naam likhein, jaise: Ahmed Khan"
            )
        self.adk_helper._send_text_once(
            user_id, retry_msg, reply_to_message_id=replied_to_id,
        )
        try:
            await self._send_vn_async(user_id, retry_msg, reply_to_message_id=replied_to_id)
        except Exception as e:
            logger.warning("name_change.vn_failed", user_id=user_id, error=str(e))
        results.append(
            {
                "name": name,
                "message": text,
                "flow": "name_capture_retry" if require_explicit_name else "name_change_retry",
            }
        )
        return True

    def _get_cached_onboarding_vn(self, cache_key: str) -> Optional[dict]:
        """
        Retrieve cached onboarding VN from Firestore.
        
        Args:
            cache_key: Cache key (e.g., "onboarding_vn_step_1")
        
        Returns:
            dict with audio_bytes, meta, is_voice, is_mp3 if found, else None
        """
        try:
            # Get cache TTL (default 60 days)
            cache_ttl = int(os.getenv("ONBOARDING_VN_CACHE_TTL_DAYS", "60")) * 86400
            
            doc_ref = self.adk_helper.db.collection("onboarding_vn_cache").document(cache_key)
            doc = doc_ref.get()
            
            if not doc.exists:
                logger.info("onboarding.vn.cache_miss", cache_key=cache_key)
                return None
            
            data = doc.to_dict() or {}
            
            # Check if cache is still valid
            cached_at = data.get("cached_at", 0)
            now = time.time()
            
            if (now - cached_at) > cache_ttl:
                logger.info(
                    "onboarding.vn.cache_expired",
                    cache_key=cache_key,
                    age_days=round((now - cached_at) / 86400, 1),
                )
                # Delete expired cache
                try:
                    doc_ref.delete()
                except Exception:
                    pass
                return None
            
            # Decode base64 audio
            audio_b64 = data.get("audio_base64")
            if not audio_b64:
                logger.warning("onboarding.vn.cache_invalid", cache_key=cache_key, reason="no_audio")
                return None
            
            try:
                audio_bytes = base64.b64decode(audio_b64)
            except Exception as e:
                logger.warning(
                    "onboarding.vn.cache_decode_failed",
                    cache_key=cache_key,
                    error=str(e),
                )
                return None
            
            logger.info(
                "onboarding.vn.cache_hit",
                cache_key=cache_key,
                size=len(audio_bytes),
                age_hours=round((now - cached_at) / 3600, 1),
            )
            
            return {
                "audio_bytes": audio_bytes,
                "meta": data.get("meta", {}),
                "is_voice": data.get("is_voice", True),
                "is_mp3": data.get("is_mp3", False),
            }
            
        except Exception as e:
            logger.error(
                "onboarding.vn.cache_get_error",
                cache_key=cache_key,
                error=str(e),
            )
            return None


    def _cache_onboarding_vn(
        self,
        cache_key: str,
        audio_bytes: bytes,
        meta: dict,
        is_voice: bool,
        is_mp3: bool,
    ) -> None:
        """
        Cache onboarding VN to Firestore.
        
        Args:
            cache_key: Cache key (e.g., "onboarding_vn_step_1")
            audio_bytes: Audio file bytes
            meta: Metadata dict
            is_voice: Whether it's a voice note
            is_mp3: Whether it's MP3 format
        """
        try:
            # Encode audio to base64
            audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
            
            cache_data = {
                "audio_base64": audio_b64,
                "meta": meta or {},
                "is_voice": is_voice,
                "is_mp3": is_mp3,
                "cached_at": time.time(),
                "size_bytes": len(audio_bytes),
                "tenant_id": self.adk_helper.tenant_id,
            }
            
            doc_ref = self.adk_helper.db.collection("onboarding_vn_cache").document(cache_key)
            doc_ref.set(cache_data)
            
            logger.info(
                "onboarding.vn.cached",
                cache_key=cache_key,
                size=len(audio_bytes),
                is_voice=is_voice,
                is_mp3=is_mp3,
            )
            
        except Exception as e:
            logger.error(
                "onboarding.vn.cache_set_error",
                cache_key=cache_key,
                error=str(e),
            )

    def handle_webhook_get(self):
        mode = request.args.get("hub.mode")
        token = request.args.get("hub.verify_token")
        challenge = request.args.get("hub.challenge")

        if mode == "subscribe" and token == self.verify_token:
            print("Webhook verified successfully!")
            return make_response(challenge, 200)
        else:
            print("Webhook verification failed.")
            return make_response("Verification token mismatch", 403)

    async def handle_webhook_post(self):
        """
        Optimized webhook handler with onboarding VN support.
        """
        results = []
        try:
            value = {}
            if self.is_twilio:
                form = request.form or request.values or {}
                self._log_twilio_request("twilio.webhook.hit", form=form)
                if form.get("MessageStatus"):
                    return self.handle_twilio_status_post()
                value, raw_payload = self._normalize_twilio_webhook()
                if value is None:
                    return make_response("", 400)
                logger.info("webhook.payload", payload=raw_payload)
                message_sid = form.get("MessageSid") or form.get("SmsMessageSid")
                if message_sid and self._is_twilio_whatsapp_inbound(form):
                    account_sid = form.get("AccountSid") or ""
                    if account_sid and self.twilio_account_sid and account_sid != self.twilio_account_sid:
                        logger.warning(
                            "twilio.typing.skip",
                            reason="account_mismatch",
                            configured=self.twilio_account_sid,
                            inbound=account_sid,
                            message_sid=message_sid,
                        )
                    else:
                        self.adk_helper.send_typing_indicator(message_sid)
            else:
                data = request.get_json(silent=True) or {}
                logger.info("webhook.payload", payload=data)

                # Meta/Cloud API webhooks should contain the standard "entry[0].changes[0]" envelope.
                # When the payload is missing this (e.g. health checks, misrouted requests),
                # avoid raising a KeyError and just ack with 200.
                if not isinstance(data, dict) or "entry" not in data:
                    logger.warning(
                        "webhook.unexpected_payload",
                        has_entry=isinstance(data, dict) and "entry" in data,
                        keys=list(data.keys()) if isinstance(data, dict) else None,
                    )
                    return make_response("", 200)

                try:
                    entry = data.get("entry") or []
                    changes = (entry[0].get("changes") or [])[0]
                except Exception as e:
                    logger.error("webhook.entry_parse_error", error=str(e), payload=data)
                    return make_response("", 200)

                value = changes.get("value", {}) if isinstance(changes, dict) else {}

            # ---- Status updates ----
            if "statuses" in value:
                try:
                    if SESSION_TOUCH_ON_STATUS:
                        for st in value.get("statuses", []):
                            to_user = st.get("recipient_id")
                            if to_user:
                                self.adk_helper.session_helper.touch(to_user, source="agent")
                    return make_response("", 200)
                except Exception as e:
                    logger.warning("status.touch.error", error=str(e))
                    return make_response("", 200)

            messages = value.get("messages", [])
            name = value.get("contacts", [{}])[0].get("profile", {}).get("name", "Unknown")
            try:
                msg_types = [m.get("type") for m in messages]
                logger.info(
                    "inbound.messages.summary",
                    count=len(messages),
                    types=msg_types,
                    raw_present=bool(messages),
                )
            except Exception:
                pass

            for message in messages:
                try:
                    phone_number = message["from"].lstrip("+")
                    user_id = phone_number
                    mtype = message.get("type")
                    inbound_key = message.get("id")
                    replied_to_id = (message.get("context") or {}).get("id")
                    logger.info(
                        "msg.inbound",
                        user_id=user_id,
                        inbound_key=inbound_key,
                        msg_type=mtype,
                        has_context=bool(replied_to_id),
                    )

                    # For Cloud API users, seed Firestore with the WhatsApp profile name
                    # the first time we ever see this user_id.
                    try:
                        if not self.is_twilio:
                            self.adk_helper.maybe_store_whatsapp_profile_name(user_id, name)
                    except Exception as e:
                        logger.warning("whatsapp_name.seed_failed", user_id=user_id, error=str(e))

                    # --- Inbound idempotency + staleness guard ---
                    wa_ts = message.get("timestamp")
                    try:
                        msg_ts = float(wa_ts)
                    except (TypeError, ValueError):
                        msg_ts = None

                    if not self.inbound_store.claim_message(user_id, inbound_key):
                        logger.info(
                            "inbound.duplicate_skip",
                            user_id=user_id,
                            inbound_key=inbound_key,
                        )
                        continue

                    stale_threshold = int(os.getenv("INBOUND_STALE_THRESHOLD", "7200"))
                    if msg_ts is not None:
                        now_ts = time.time()
                        age_sec = now_ts - msg_ts
                        if age_sec > stale_threshold:
                            logger.info(
                                "inbound.stale_drop",
                                user_id=user_id,
                                inbound_key=inbound_key,
                                msg_ts=msg_ts,
                                now=now_ts,
                                age_sec=round(age_sec, 1),
                                age_minutes=round(age_sec / 60, 1),
                            )
                            self.inbound_store.mark_stale(user_id, inbound_key)
                            continue

                    # --- Mark inbound as read (Cloud API read receipts) ---
                    if not self.is_twilio and READ_RECEIPTS_ENABLED:
                        self._mark_read(inbound_key)

                    # --------------------------------------------------
                    # NO AUTHENTICATION (YTL demo)
                    # --------------------------------------------------
                    # All users can chat immediately. Profile bootstrap (Firestore -> local customers.json -> ask name)
                    # is handled inside ADKHelper.

                    # --------------------------------------------------
                    # NAME CHANGE FLOW GUARD (legacy)
                    # --------------------------------------------------
                    # For YTL we use the simpler name prompt in ADKHelper. Keep this guard only for non-YTL tenants.
                    if (os.getenv("TENANT_ID") or "").strip().lower() != "ytl":
                        if self.adk_helper.get_name_change_state(user_id) and mtype not in {"text", "audio"}:
                            reminder = "Before we continue, please send your full name as a text or voice note."
                            self.adk_helper._send_text_once(
                                user_id,
                                reminder,
                                reply_to_message_id=replied_to_id,
                            )
                            results.append(
                                {
                                    "name": name,
                                    "message": "name_capture_pending_blocked_non_text",
                                    "blocked_type": mtype,
                                }
                            )
                            continue

                    # --------------------------------------------------
                    # TEXT  (buffered + batched)
                    # --------------------------------------------------
                    content_to_buffer = None
                    if mtype == "text":
                        text_message = message["text"]["body"]

                        logger.info(
                            "msg.text.received",
                            user_id=user_id,
                            text=text_message,
                            inbound_key=inbound_key,
                            reply_to=replied_to_id,
                        )

                        # ← NAME CHANGE: intercept before buffering/agent
                        if await self._maybe_handle_name_change(
                            user_id, text_message,
                            replied_to_id=replied_to_id,
                            name=name,
                            inbound_key=inbound_key,
                            results=results,
                        ):
                            continue

                        # ===================================================================
                        # Check for IMMEDIATE processing triggers
                        # ===================================================================
                        buffer_enabled = os.getenv("MESSAGE_BUFFER_ENABLED", "true").lower() == "true"
                        should_process_immediately = False
                        immediate_reason = None

                        if not buffer_enabled:
                            should_process_immediately = True
                            immediate_reason = "buffer_disabled"
                            logger.info("msg.text.buffer_disabled_immediate", user_id=user_id)
                        elif self.adk_helper.is_goodbye_message(text_message):
                            should_process_immediately = True
                            immediate_reason = "farewell"
                            logger.info("msg.text.goodbye_immediate", user_id=user_id, text=text_message)
                        elif self.adk_helper._is_trivial_greeting(text_message):
                            should_process_immediately = True
                            immediate_reason = "greeting"
                            logger.info("msg.text.greeting_immediate", user_id=user_id, text=text_message)

                        # ===================================================================
                        # IMMEDIATE PROCESSING (goodbye/greeting/buffer_disabled)
                        # ===================================================================
                        if should_process_immediately:
                            try:
                                # If user is ending the chat, clear cart so next session starts clean.
                                # No-op if cart already empty.
                                if immediate_reason == "farewell":
                                    try:
                                        cart_snapshot = get_cart(user_id) or {}
                                        cart_items = (
                                            cart_snapshot.get("items")
                                            or cart_snapshot.get("skus")
                                            or []
                                        )
                                        if isinstance(cart_items, list) and cart_items:
                                            store_id_for_clear = str(
                                                cart_snapshot.get("store_id") or user_id
                                            ).strip() or str(user_id)
                                            resp = agentflo_cart_tool(
                                                {
                                                    "user_id": user_id,
                                                    "store_id": store_id_for_clear,
                                                    "operations": [{"op": "CLEAR_CART"}],
                                                }
                                            ) or {}
                                            logger.info(
                                                "cart.cleared_on_goodbye",
                                                user_id=user_id,
                                                store_id=store_id_for_clear,
                                                ok=bool(isinstance(resp, dict) and resp.get("ok")),
                                            )
                                    except Exception as _e:
                                        logger.warning(
                                            "cart.clear_on_goodbye_failed",
                                            user_id=user_id,
                                            error=str(_e),
                                        )

                                agent_response = self.adk_helper.handle_message(
                                    text_message,
                                    user_id,
                                    is_voice_input=False,
                                    inbound_key=inbound_key,
                                    reply_to_message_id=replied_to_id,
                                )

                                results.append({
                                    "name": name,
                                    "message": text_message,
                                    "reply_to": replied_to_id,
                                    "Agent": agent_response,
                                    "immediate": immediate_reason,
                                })

                                try:
                                    base_event = generate_billing_event_v2(
                                        tenant_id=TENANT_ID,
                                        conversation_id=self._get_conversation_id(user_id),
                                        msg_type=self._billing_msg_type(mtype),
                                        message_id=inbound_key,
                                        role="user",
                                        channel="whatsapp",
                                        conversation_text=text_message,
                                        gemini_usage=None,
                                        eleven_tts_usage=None,
                                    )
                                    send_billing_event_fire_and_forget(base_event)
                                except Exception as e:
                                    logger.warning(f"billing.{immediate_reason}.base_failed", error=str(e))

                                try:
                                    rated_event = generate_billing_event_v2(
                                        tenant_id=TENANT_ID,
                                        conversation_id=self._get_conversation_id(user_id),
                                        msg_type=self._billing_msg_type(mtype),
                                        message_id=inbound_key,
                                        role="assistant",
                                        channel="whatsapp",
                                        conversation_text=agent_response,
                                        gemini_usage=self.adk_helper.get_last_gemini_usage(user_id),
                                        eleven_tts_usage=None,
                                    )
                                    send_billing_event_fire_and_forget(rated_event)
                                except Exception as e:
                                    logger.warning(f"billing.{immediate_reason}.rated_failed", error=str(e))

                                continue

                            except Exception as e:
                                logger.error(f"msg.{immediate_reason}.error", user_id=user_id, error=str(e), exc_info=True)

                        # ===================================================================
                        # DIRECT TEXT HANDLING (BUFFERING DISABLED FOR YTL DEMO)
                        # ===================================================================
                        try:
                            agent_response = self.adk_helper.handle_message(
                                text_message,
                                user_id,
                                is_voice_input=False,
                                inbound_key=inbound_key,
                                reply_to_message_id=replied_to_id,
                            )

                            # YTL (Meta): if agent asks for delivery/site location, also send the interactive location button.
                            try:
                                tenant = (TENANT_ID or "").strip().lower()
                                if (not self.is_twilio) and tenant == "ytl" and isinstance(agent_response, str) and _ASKS_LOCATION_RE.search(agent_response):
                                    last_sent = 0.0
                                    try:
                                        last_sent = float(self.adk_helper.session_helper.get_last_location_request_at(user_id))
                                    except Exception:
                                        last_sent = 0.0
                                    cooldown_sec = float(os.getenv("LOCATION_REQUEST_COOLDOWN_SEC", "0") or 0)
                                    if (time.time() - last_sent) >= cooldown_sec:
                                        if self._send_location_request(user_id, replied_to_id):
                                            try:
                                                self.adk_helper.session_helper.mark_location_request_sent(user_id)
                                            except Exception:
                                                pass
                            except Exception as e:
                                logger.warning("wa.location_request.auto_failed", user_id=user_id, error=str(e))

                            # Billing: base event
                            try:
                                base_key = f"{inbound_key}::base"
                                if base_key not in self._emitted_billing_ids:
                                    base_event = generate_billing_event_v2(
                                        tenant_id=TENANT_ID,
                                        conversation_id=self._get_conversation_id(user_id),
                                        msg_type=self._billing_msg_type(mtype),
                                        message_id=inbound_key,
                                        role="user",
                                        channel="whatsapp",
                                        conversation_text=text_message,
                                        gemini_usage=None,
                                        eleven_tts_usage=None,
                                    )
                                    send_billing_event_fire_and_forget(base_event)
                                    self._emitted_billing_ids.add(base_key)
                            except Exception as e:
                                logger.warning("billing.text.fallback.base_event_failed", error=str(e))

                            # Billing: rated event
                            try:
                                rated_key = f"{inbound_key}::rated"
                                if rated_key not in self._emitted_billing_ids:
                                    rated_event = generate_billing_event_v2(
                                        tenant_id=TENANT_ID,
                                        conversation_id=self._get_conversation_id(user_id),
                                        msg_type=self._billing_msg_type(mtype),
                                        message_id=inbound_key,
                                        role="assistant",
                                        channel="whatsapp",
                                        conversation_text=agent_response,
                                        gemini_usage=self.adk_helper.get_last_gemini_usage(user_id),
                                        eleven_tts_usage=None,
                                    )
                                    send_billing_event_fire_and_forget(rated_event)
                                    self._emitted_billing_ids.add(rated_key)
                            except Exception as e:
                                logger.warning("billing.text.fallback.rated_event_failed", error=str(e))

                            results.append({
                                "name": name,
                                "message": text_message,
                                "reply_to": replied_to_id,
                                "Agent": agent_response,
                                "immediate": "buffer_fallback",
                            })
                        except Exception as fallback_err:
                            logger.error("msg.text.fallback_failed", user_id=user_id, error=str(fallback_err))
                            fallback = "Bhai thora masla ho gaya, ek dafa phir try kar lein."
                            try:
                                self.adk_helper._send_text_once(user_id, fallback, reply_to_message_id=replied_to_id)
                            except Exception:
                                pass
                        continue

                    # --------------------------------------------------
                    # AUDIO
                    # --------------------------------------------------
                    elif mtype == "audio":
                        media = message.get("audio") or {}
                        media_id = media.get("id")
                        media_url = media.get("url") or media_id
                        mime_type = media.get("mime_type")
                        logger.info(
                            "msg.audio.in",
                            user_id=user_id,
                            media_id=media_id,
                            media_url=media_url,
                            reply_to=replied_to_id,
                            transport=("twilio" if self.is_twilio else "meta"),
                        )

                        if not self.is_twilio:
                            self._kickoff_voice_reaction(user_id, inbound_key)

                        conv_id = self._get_conversation_id(user_id)
                        s3_key = None
                        transcribed_text = ""

                        if self.is_twilio:
                            media_path, dl_ctype = self._download_twilio_media(
                                media_url,
                                message_sid=inbound_key,
                                media_content_type=mime_type,
                            )
                            if not media_path:
                                logger.warning("msg.audio.download_failed", user_id=user_id)
                                self.adk_helper._send_text_once(
                                    user_id,
                                    "I received your voice message but couldn't download it. Could you please try again?",
                                    reply_to_message_id=replied_to_id,
                                )
                                results.append({"error": "Audio download failed", "user_id": user_id})
                                continue
                            try:
                                transcribed_text = self.audio_transcriber.transcribe_audio(media_path)
                            finally:
                                try:
                                    os.remove(media_path)
                                except Exception:
                                    pass
                        else:
                            s3_key = store_voice_note_to_s3(user_id, media_id, inbound_key, conv_id)
                            _vn_result = self.audio_transcriber.transcribe_whatsapp_vn(media_id)
                            transcribed_text, vn_lang_code = _vn_result if isinstance(_vn_result, tuple) else (_vn_result, "ur")
                            if transcribed_text:
                                content_to_buffer = transcribed_text
                                logger.info(f"VN Transcribed for buffer: {transcribed_text}", lang=vn_lang_code)

                        if not transcribed_text or transcribed_text.strip() == "":
                            logger.warning("msg.audio.transcribe_empty", user_id=user_id)
                            self.adk_helper._send_text_once(
                                user_id,
                                "I received your voice message but couldn't understand what you said. Could you please try again?",
                                reply_to_message_id=replied_to_id,
                            )
                            results.append({"error": "Empty transcription", "user_id": user_id})
                            continue

                        logger.info("msg.audio.transcribed", user_id=user_id, text=transcribed_text)

                        # ← NAME CHANGE: transcribed text is just a string — same logic as text path
                        if await self._maybe_handle_name_change(
                            user_id, transcribed_text,
                            replied_to_id=replied_to_id,
                            name=name,
                            inbound_key=inbound_key,
                            results=results,
                        ):
                            continue

                        start_time = time.perf_counter()

                        agent_response = self.adk_helper.handle_message(
                            transcribed_text,
                            user_id,
                            is_voice_input=True,
                            inbound_key=inbound_key,
                            reply_to_message_id=replied_to_id,
                            vn_lang_code=vn_lang_code,
                        )

                        response_time_ms = int((time.perf_counter() - start_time) * 1000)

                        try:
                            time.sleep(0.5)
                            eleven_usage = self.adk_helper.get_last_eleven_tts_usage(user_id)
                            evaluate_conversation_turn(
                                user_id=user_id,
                                conversation_id=self._get_conversation_id(user_id),
                                user_message=transcribed_text,
                                agent_response=agent_response,
                                message_type="audio",
                                tools_used=None,
                                response_time_ms=response_time_ms,
                                gemini_usage=self.adk_helper.get_last_gemini_usage(user_id),
                                eleven_tts_usage=eleven_usage,
                            )
                        except Exception as e:
                            logger.warning("evaluation.failed", error=str(e))

                        logger.info("msg.audio.agent_done", user_id=user_id, text=agent_response)

                        try:
                            base_key = f"{inbound_key}::base"
                            if base_key not in self._emitted_billing_ids:
                                base_event = generate_billing_event_v2(
                                    tenant_id=TENANT_ID,
                                    conversation_id=self._get_conversation_id(user_id),
                                    msg_type=self._billing_msg_type(mtype),
                                    message_id=inbound_key,
                                    role="user",
                                    channel="whatsapp",
                                    conversation_text=transcribed_text,
                                    gemini_usage=None,
                                    eleven_tts_usage=None,
                                    s3_key=s3_key,
                                )
                                send_billing_event_fire_and_forget(base_event)
                                self._emitted_billing_ids.add(base_key)
                        except Exception as e:
                            logger.warning("billing.voice.base_event_failed", error=str(e))

                        try:
                            from time import perf_counter, sleep

                            start_wait = perf_counter()
                            max_wait = 20.0
                            eleven_usage = None
                            gemini_usage = None

                            while (perf_counter() - start_wait) < max_wait:
                                if gemini_usage is None:
                                    gemini_usage = self.adk_helper.get_last_gemini_usage(user_id)
                                if eleven_usage is None:
                                    eleven_usage = self.adk_helper.get_last_eleven_tts_usage(user_id)
                                if gemini_usage and gemini_usage.get("enabled") and (
                                    eleven_usage is not None or (perf_counter() - start_wait) > 10.0
                                ):
                                    break
                                sleep(0.35)

                            if eleven_usage is None and isinstance(agent_response, str) and agent_response.strip():
                                try:
                                    chars = len(agent_response)
                                    eleven_usage = {
                                        "enabled": True,
                                        "model": getattr(self.adk_helper, "eleven_model_id", "unknown"),
                                        "voice_id": getattr(self.adk_helper, "eleven_voice_id", ""),
                                        "request_id": "",
                                        "latency_ms": 0,
                                        "input_characters": chars,
                                        "pricing": {
                                            "unit": "1K_characters",
                                            "price_per_1k_characters_usd": 22.0,
                                        },
                                    }
                                except Exception:
                                    eleven_usage = None

                            rated_key = f"{inbound_key}::rated"
                            if rated_key not in self._emitted_billing_ids:
                                rated_event = generate_billing_event_v2(
                                    tenant_id=TENANT_ID,
                                    conversation_id=self._get_conversation_id(user_id),
                                    msg_type=self._billing_msg_type(mtype),
                                    message_id=inbound_key,
                                    role="assistant",
                                    channel="whatsapp",
                                    conversation_text=agent_response,
                                    gemini_usage=gemini_usage,
                                    eleven_tts_usage=eleven_usage,
                                )
                                send_billing_event_fire_and_forget(rated_event)
                                self._emitted_billing_ids.add(rated_key)
                        except Exception as e:
                            logger.warning("billing.voice.rated_event_failed", error=str(e))

                        results.append(
                            {
                                "name": name,
                                "message": transcribed_text,
                                "reply_to": replied_to_id,
                                "Agent": agent_response,
                            }
                        )


                    # --------------------------------------------------
                    # IMAGE
                    # --------------------------------------------------
                    elif mtype == "image":
                        media = message.get("image") or {}
                        media_id = media.get("id")
                        media_url = media.get("url") or media_id
                        mime_type = media.get("mime_type")
                        logger.info(
                            "msg.image.in",
                            user_id=user_id,
                            media_id=media_id,
                            media_url=media_url,
                            reply_to=replied_to_id,
                            transport=("twilio" if self.is_twilio else "meta"),
                        )
                        s3_key = None
                        try:
                            conv_id_for_image = self._get_conversation_id(user_id)
                        except Exception as e:
                            logger.warning("conversation_id.image_failed", user_id=user_id, error=str(e))
                            conv_id_for_image = user_id

                        # YTL demo: no authentication / no invoice onboarding.
                        if (os.getenv("TENANT_ID") or "").strip().lower() != "ytl" and not self.adk_helper.session_helper.get_auth_status(user_id):
                            handled = self._handle_invoice_onboarding_image(
                                user_id,
                                media_id=media_id,
                                media_url=media_url,
                                mime_type=mime_type,
                                inbound_key=inbound_key,
                                reply_to_message_id=replied_to_id,
                            )
                            if handled:
                                continue
                            logger.info(
                                "invoice.image.not_handled",
                                user_id=user_id,
                                media_id=media_id,
                                inbound_key=inbound_key,
                            )

                        image_prompt = "The user has sent an image. Here's the order information extracted from the image:"
                        if self.is_twilio:
                            img_path, _ctype = self._download_twilio_media(
                                media_url,
                                message_sid=inbound_key,
                                media_content_type=mime_type,
                            )
                            if not img_path:
                                self.adk_helper._send_text_once(
                                    user_id,
                                    "Image download failed. Could you resend the picture?",
                                    reply_to_message_id=replied_to_id,
                                )
                                results.append({"error": "Image download failed", "user_id": user_id})
                                continue
                            # Upload Twilio image to S3
                            try:
                                with open(img_path, "rb") as f:
                                    payload = f.read()
                                ct = _ctype or "image/jpeg"
                                s3_key, _ = upload_bytes_to_s3(
                                    payload,
                                    content_type=ct,
                                    user_phone=user_id,
                                    conversation_id=conv_id_for_image,
                                    msg_id=inbound_key or "image",
                                    key_prefix="images",
                                    base_dir="images",
                                )
                            except Exception as e:
                                logger.warning("msg.image.twilio.s3_upload_failed", user_id=user_id, error=str(e))
                            try:
                                image_order_json = self.image_helper.inference_img_struct_output(image_prompt, img_path)
                            finally:
                                try:
                                    os.remove(img_path)
                                except Exception:
                                    pass
                        else:
                            image_order_json = self.image_helper.get_order_from_image_structured_output(media_id)
                            try:
                                s3_key = store_image_to_s3(
                                    user_id,
                                    media_id,
                                    inbound_key or "",
                                    conv_id_for_image,
                                )
                            except Exception as e:
                                logger.warning("msg.image.s3_upload_failed", user_id=user_id, error=str(e))
                        agent_prompt = f"{image_prompt}\n{image_order_json}"

                        logger.info("msg.image.order_json", user_id=user_id, media_id=media_id, text=image_order_json)

                        start_time = time.perf_counter()

                        agent_response = self.adk_helper.handle_message(
                            agent_prompt,
                            user_id,
                            is_voice_input=False,
                            inbound_key=inbound_key,
                            reply_to_message_id=replied_to_id,
                        )

                        response_time_ms = int((time.perf_counter() - start_time) * 1000)

                        try:
                            evaluate_conversation_turn(
                                user_id=user_id,
                                conversation_id=conv_id_for_image,
                                user_message=f"[IMAGE ORDER] {image_order_json[:200]}",
                                agent_response=agent_response,
                                message_type="image",
                                tools_used=["image_processing"],
                                response_time_ms=response_time_ms,
                                gemini_usage=self.adk_helper.get_last_gemini_usage(user_id),
                                eleven_tts_usage=None,
                            )
                        except Exception as e:
                            logger.warning("evaluation.failed", error=str(e))
                        logger.info("agent_and_text.sent", user_id=user_id)

                        # Billing for image → text
                        try:
                            base_key = f"{inbound_key}::base"
                            if base_key not in self._emitted_billing_ids:
                                base_event = generate_billing_event_v2(
                                    tenant_id=TENANT_ID,
                                    conversation_id=conv_id_for_image,
                                    msg_type=self._billing_msg_type(mtype),
                                    message_id=inbound_key,
                                    role="user",
                                    channel="whatsapp",
                                    conversation_text=agent_prompt,
                                    gemini_usage=None,
                                    eleven_tts_usage=None,
                                    s3_key=s3_key,
                                )
                                send_billing_event_fire_and_forget(base_event)
                                self._emitted_billing_ids.add(base_key)
                        except Exception as e:
                            logger.warning("billing.image.base_event_failed", error=str(e))

                        try:
                            rated_key = f"{inbound_key}::rated"
                            if rated_key not in self._emitted_billing_ids:
                                rated_event = generate_billing_event_v2(
                                    tenant_id=TENANT_ID,
                                    conversation_id=self._get_conversation_id(user_id),
                                    msg_type=self._billing_msg_type(mtype),
                                    message_id=inbound_key,
                                    role="assistant",
                                    channel="whatsapp",
                                    conversation_text=agent_response,
                                    gemini_usage=self.adk_helper.get_last_gemini_usage(user_id),
                                    eleven_tts_usage=None,
                                )
                                send_billing_event_fire_and_forget(rated_event)
                                self._emitted_billing_ids.add(rated_key)
                        except Exception as e:
                            logger.warning("billing.image.rated_event_failed", error=str(e))

                        results.append(
                            {
                                "name": name,
                                "message": agent_prompt,
                                "reply_to": replied_to_id,
                                "Agent": agent_response,
                            }
                        )

                    # --------------------------------------------------
                    # LOCATION
                    # --------------------------------------------------
                    elif mtype == "location":
                        loc = message.get("location", {}) or {}
                        lat = loc.get("latitude")
                        lon = loc.get("longitude")
                        name_or_address = loc.get("name") or ""
                        addr = loc.get("address") or ""

                        logger.info(
                            "msg.location.in",
                            user_id=user_id,
                            lat=lat,
                            lon=lon,
                            name=name_or_address,
                            addr=addr,
                            reply_to=replied_to_id,
                        )

                        # Persist location
                        try:
                            if lat is not None and lon is not None:
                                self.adk_helper.save_user_location(
                                    user_id,
                                    float(lat),
                                    float(lon),
                                    name_or_address,
                                    addr,
                                )
                        except Exception as e:
                            logger.warning("msg.location.save_failed", user_id=user_id, error=str(e))

                        # Nudge agent with location
                        try:
                            prompt = f"[USER_LOCATION]\nlat={lat}, lon={lon}\n[/USER_LOCATION]\nUser shared location."
                            agent_response = self.adk_helper.handle_message(
                                prompt,
                                user_id,
                                is_voice_input=False,
                                inbound_key=inbound_key,
                                reply_to_message_id=replied_to_id,
                            )
                            logger.info("msg.location.agent_done", user_id=user_id, text=agent_response)
                        except Exception as e:
                            logger.warning("msg.location.agent_error", user_id=user_id, error=str(e))

                        # Billing for location
                        try:
                            base_key = f"{inbound_key}::base"
                            if base_key not in self._emitted_billing_ids:
                                base_event = generate_billing_event_v2(
                                    tenant_id=TENANT_ID,
                                    conversation_id=self._get_conversation_id(user_id),
                                    msg_type=self._billing_msg_type(mtype),
                                    message_id=inbound_key,
                                    role="user",
                                    channel="whatsapp",
                                    conversation_text=f"location: {lat},{lon}",
                                    gemini_usage=None,
                                    eleven_tts_usage=None,
                                )
                                send_billing_event_fire_and_forget(base_event)
                                self._emitted_billing_ids.add(base_key)
                        except Exception as e:
                            logger.warning("billing.location.base_event_failed", error=str(e))

                        try:
                            rated_key = f"{inbound_key}::rated"
                            if rated_key not in self._emitted_billing_ids:
                                rated_event = generate_billing_event_v2(
                                    tenant_id=TENANT_ID,
                                    conversation_id=self._get_conversation_id(user_id),
                                    msg_type=self._billing_msg_type(mtype),
                                    message_id=inbound_key,
                                    role="assistant",
                                    channel="whatsapp",
                                    conversation_text=agent_response,
                                    gemini_usage=self.adk_helper.get_last_gemini_usage(user_id),
                                    eleven_tts_usage=None,
                                )
                                send_billing_event_fire_and_forget(rated_event)
                                self._emitted_billing_ids.add(rated_key)
                        except Exception as e:
                            logger.warning("billing.location.rated_event_failed", error=str(e))

                        results.append(
                            {
                                "name": name,
                                "message": f"location: {lat},{lon}",
                                "reply_to": replied_to_id,
                                "Agent": "location_ack_via_agent_only",
                            }
                        )

                    # --------------------------------------------------
                    # ORDER (catalog → draft)
                    # --------------------------------------------------
                    elif mtype == "order":
                        logger.info(
                            "msg.order.in",
                            user_id=user_id,
                            order=message.get("order"),
                            reply_to=replied_to_id,
                            transport=("twilio" if self.is_twilio else "meta"),
                        )

                        order_payload = message.get("order") or {}
                        try:
                            order_draft = self.order_helper.convert_order_json_to_order_draft(order_payload, user_id=user_id)
                        except Exception as e:
                            logger.error("webhook.order.draft_build_failed", user_id=user_id, error=str(e))
                            self.adk_helper._send_text_once(
                                user_id,
                                "jee thora sa time lag raha hai, main dobara check karti hoon.",
                                reply_to_message_id=replied_to_id,
                            )
                            results.append(
                                {
                                    "name": name,
                                    "message": "catalog_order_failed",
                                    "reply_to": replied_to_id,
                                    "error": str(e),
                                }
                            )
                            continue

                        # Update the cart via agentflo_cart_tool (merge items from WhatsApp order into current cart)
                        try:
                            cart_snapshot = get_cart(user_id) or {}
                        except Exception as e:
                            logger.warning("webhook.order.cart_prefetch_failed", user_id=user_id, error=str(e))
                            cart_snapshot = {}

                        def _is_bad_store(v: str) -> bool:
                            """Mirrors cart_tools.is_placeholder_store_id; avoids import overhead here."""
                            import re as _re
                            t = v.strip().lower()
                            if not t or t in {"unknown", "none", "null", "n/a", "na", "-", "0"}:
                                return True
                            if t.startswith("unknown"):
                                return True
                            if _re.fullmatch(r"\+?\d{10,15}", t):  # phone number fallback
                                return True
                            return False

                        cart_store_id = str(cart_snapshot.get("store_id") or "").strip()
                        if _is_bad_store(cart_store_id):
                            cart_store_id = ""
                        draft_store_id = str(getattr(order_draft, "store_id", "") or "").strip()
                        if _is_bad_store(draft_store_id):
                            draft_store_id = ""
                        store_id = cart_store_id or draft_store_id or user_id

                        operations = []
                        order_items = getattr(order_draft, "items", []) or getattr(order_draft, "skus", [])

                        def _get_val(obj, field_name):
                            if isinstance(obj, dict):
                                return obj.get(field_name)
                            return getattr(obj, field_name, None)

                        try:
                            item_preview = []
                            for item in order_items[:5]:
                                sku_val = _get_val(item, "sku_code")
                                qty_val = _get_val(item, "qty")
                                item_preview.append({"sku": sku_val, "qty": qty_val})
                            logger.info(
                                "webhook.order.draft_summary",
                                user_id=user_id,
                                item_count=len(order_items),
                                items_preview=item_preview,
                                store_id=store_id,
                            )
                        except Exception:
                            pass

                        # Resolve product names: local products.json first, then Meta catalog API.
                        _resolved_names: dict = {}
                        import re as _re
                        _PLACEHOLDER_NAME_RE = _re.compile(r"^(SKU\s*[\w/-]+|Catalog Item \d+)$", _re.IGNORECASE)

                        # 1) Local products.json lookup (fast, always available)
                        try:
                            import json as _json
                            _products_path = os.path.join(
                                os.path.dirname(os.path.abspath(__file__)), "..", "..", "data", "products.json"
                            )
                            if os.path.exists(_products_path):
                                with open(_products_path, "r", encoding="utf-8") as _f:
                                    _local_products = _json.load(_f).get("products", [])
                                for _p in _local_products:
                                    _sku = (_p.get("sku_code") or _p.get("sku") or "").strip().upper()
                                    _pname = (_p.get("product_name") or "").strip()
                                    _fullname = (_p.get("name") or "").strip()
                                    if _sku and (_pname or _fullname):
                                        _resolved_names[_sku] = _fullname or _pname
                                logger.info("webhook.order.local_names_loaded", count=len(_resolved_names))
                        except Exception as _e:
                            logger.warning("webhook.order.local_names_failed", error=str(_e))

                        # 2) Meta catalog API lookup (supplement for any remaining unknowns)
                        try:
                            from agents.tools.catalog_search import lookup_names_by_retailer_ids
                            _ids_to_resolve = []

                            for _it in order_items:
                                _rid = _get_val(_it, "product_retailer_id") or _get_val(_it, "sku_code")
                                _existing_name = _get_val(_it, "name")
                                _is_placeholder = (
                                    not _existing_name
                                    or _PLACEHOLDER_NAME_RE.match(str(_existing_name).strip())
                                )
                                _rid_upper = str(_rid).strip().upper() if _rid else ""
                                if _rid and _is_placeholder and _rid_upper not in _resolved_names:
                                    _ids_to_resolve.append(str(_rid).strip())
                            if _ids_to_resolve:
                                _catalog_id_from_order = str(order_payload.get("catalog_id") or "").strip() or None
                                _meta_names = lookup_names_by_retailer_ids(
                                    _ids_to_resolve,
                                    catalog_id_override=_catalog_id_from_order,
                                )
                                _resolved_names.update(_meta_names)
                                logger.info(
                                    "webhook.order.names_resolved",
                                    user_id=user_id,
                                    requested=_ids_to_resolve,
                                    meta_resolved=_meta_names,
                                )
                        except Exception as _e:
                            logger.warning("webhook.order.meta_name_lookup_failed", user_id=user_id, error=str(_e))

                        for item in order_items:
                            sku_code = _get_val(item, "sku_code")
                            if not sku_code:
                                continue
                            qty_val = _get_val(item, "qty") or 0
                            try:
                                qty_int = int(qty_val)
                            except Exception:
                                continue
                            if qty_int <= 0:
                                continue

                            op = {
                                "op": "ADD_ITEM",
                                "sku_code": sku_code,
                                "qty": qty_int,
                                "merge_strategy": "INCREMENT",
                            }
                            _rid_key = str(_get_val(item, "product_retailer_id") or sku_code).strip()
                            _raw_name = _get_val(item, "name")
                            _name_is_placeholder = (
                                not _raw_name
                                or _PLACEHOLDER_NAME_RE.match(str(_raw_name).strip())
                            )
                            name_val = (
                                (None if _name_is_placeholder else _raw_name)
                                or _resolved_names.get(_rid_key)
                                or _resolved_names.get(_rid_key.upper())
                                or _resolved_names.get(sku_code.upper() if sku_code else "")
                                or sku_code
                            )
                            if name_val:
                                op["name"] = str(name_val).strip()
                            retailer_id_val = _get_val(item, "product_retailer_id")
                            if retailer_id_val:
                                op["product_retailer_id"] = retailer_id_val
                            for field_name in [
                                "price",
                                "base_price",
                                "final_price",
                                "discount_value",
                                "discount_pct",
                                "line_total",
                                "profit",
                                "profit_margin",
                            ]:
                                field_val = _get_val(item, field_name)
                                if field_val is not None:
                                    op[field_name] = field_val
                            operations.append(op)

                        cart_update_ok = False
                        try:
                            resp = agentflo_cart_tool(
                                {"user_id": user_id, "store_id": store_id, "operations": operations}
                            ) or {}
                            cart_update_ok = bool(isinstance(resp, dict) and resp.get("ok"))
                            if isinstance(resp, dict) and resp.get("warnings"):
                                logger.warning(
                                    "webhook.order.cart_update_warnings",
                                    user_id=user_id,
                                    warnings=resp.get("warnings"),
                                )
                            if not cart_update_ok:
                                logger.warning(
                                    "webhook.order.cart_update_failed",
                                    user_id=user_id,
                                    errors=resp.get("errors") if isinstance(resp, dict) else None,
                                    warnings=resp.get("warnings") if isinstance(resp, dict) else None,
                                )
                            else:
                                logger.info(
                                    "webhook.order.cart_update_ok",
                                    user_id=user_id,
                                    op_count=len(operations),
                                )
                        except Exception as e:
                            logger.error("webhook.order.cart_update_exception", user_id=user_id, error=str(e))
                        if not operations:
                            logger.warning("webhook.order.no_operations", user_id=user_id, store_id=store_id)

                        start_time = time.perf_counter()
                        agent_prompt = ""
                        agent_response = ""
                        used_llm = False
                        tools_used = ["agentflo_cart_tool", "catalog_processing", "order_draft_template"]

                        template_cart = resp.get("cart") if isinstance(resp, dict) else None
                        if not isinstance(template_cart, dict):
                            try:
                                template_cart = get_cart(user_id, store_id=store_id) or get_cart(user_id) or {}
                            except Exception as e:
                                logger.warning("webhook.order.cart_refetch_failed", user_id=user_id, error=str(e))
                                template_cart = {}

                        template_messages: list[str] = []
                        if isinstance(template_cart, dict):
                            try:
                                rendered = order_draft_template(cart=template_cart) or ""
                                if isinstance(rendered, str) and rendered.strip():
                                    if MULTI_MESSAGE_DELIMITER in rendered:
                                        template_messages = [
                                            part.strip()
                                            for part in rendered.split(MULTI_MESSAGE_DELIMITER)
                                            if isinstance(part, str) and part.strip()
                                        ]
                                    else:
                                        template_messages = [rendered.strip()]
                            except Exception as e:
                                logger.warning("webhook.order.template_render_failed", user_id=user_id, error=str(e))

                        sent_template = False
                        sent_template_messages: list[str] = []
                        if template_messages:
                            for idx, msg_part in enumerate(template_messages):
                                sent = self.adk_helper._send_text_once(
                                    user_id,
                                    msg_part,
                                    reply_to_message_id=replied_to_id if idx == 0 else None,
                                    inbound_key=inbound_key,
                                )
                                sent_template = sent_template or sent
                                if sent:
                                    sent_template_messages.append(msg_part)
                            if sent_template:
                                try:
                                    self.adk_helper.remember_order_confirmation_context(
                                        user_id,
                                        sent_template_messages or template_messages,
                                        source="catalog_order_webhook",
                                    )
                                except Exception as e:
                                    logger.warning(
                                        "webhook.order.confirm_context_capture_failed",
                                        user_id=user_id,
                                        error=str(e),
                                    )
                                agent_prompt = (
                                    "Context: Native WhatsApp catalog order event. "
                                    "Cart synced and rendered via order_draft_template."
                                )
                                agent_response = "\n\n".join(template_messages)

                        # Fallback only if template render/send failed.
                        if not sent_template:
                            tools_used = ["agentflo_cart_tool", "catalog_processing"]

                            item_summary_lines = []
                            for item in order_items[:5]:
                                name_val = (
                                    _get_val(item, "name")
                                    or _get_val(item, "display_name")
                                    or _get_val(item, "product_name")
                                    or _get_val(item, "sku_code")
                                )
                                qty_raw = _get_val(item, "qty") or _get_val(item, "quantity") or 0
                                try:
                                    qty_int = int(qty_raw)
                                except Exception:
                                    qty_int = 0
                                qty_part = f"x{qty_int}" if qty_int > 0 else ""
                                if name_val:
                                    item_summary_lines.append(f"{name_val} {qty_part}".strip())
                                elif qty_int > 0:
                                    item_summary_lines.append(f"{qty_int} units (name missing)")
                            if not item_summary_lines:
                                item_summary_lines.append("Items were selected from the WhatsApp catalog; confirm the cart contents.")
                            items_summary_text = "\n".join(f"- {line}" for line in item_summary_lines)

                            agent_prompt = (
                                "Context: Native WhatsApp store order event (no new greeting). "
                                "I've already synced the cart with the items below.\n"
                                f"Selected items:\n{items_summary_text}\n"
                                "Instructions: Do NOT greet or re-introduce yourself. "
                                "1. You MUST explicitly list the items and quantities added to the cart from the list above. "
                                "2. Ask if they want to proceed to checkout. "
                                "If they want to place the order, use confirmOrderDraftTool to send the confirmation interface. "
                                "Keep it to one short WhatsApp-style message."
                            )

                            used_llm = True
                            agent_response = self.adk_helper.handle_message(
                                agent_prompt,
                                user_id,
                                is_voice_input=False,
                                inbound_key=inbound_key,
                                reply_to_message_id=replied_to_id,
                                disable_catalog=True,
                            )

                            lower_response = (agent_response or "").strip().lower()
                            if self.adk_helper._is_trivial_greeting(agent_response or "") or lower_response.startswith(
                                ("assalam", "aoa", "salam", "hello", "hi")
                            ):
                                logger.warning(
                                    "webhook.order.agent_greeting_retry",
                                    user_id=user_id,
                                    response_preview=(agent_response or "")[:120],
                                )
                                strict_prompt = agent_prompt + (
                                    "\nReminder: Do NOT greet. Start by confirming the items above and ask if they want to checkout."
                                )
                                agent_response = self.adk_helper.handle_message(
                                    strict_prompt,
                                    user_id,
                                    is_voice_input=False,
                                    inbound_key=inbound_key,
                                    reply_to_message_id=replied_to_id,
                                )
                            if "Samajh nahi aaya" in (agent_response or ""):
                                logger.warning(
                                    "webhook.order.agent_fallback",
                                    user_id=user_id,
                                    response=agent_response,
                                )

                        response_time_ms = int((time.perf_counter() - start_time) * 1000)
                        gemini_usage = self.adk_helper.get_last_gemini_usage(user_id) if used_llm else None

                        try:
                            evaluate_conversation_turn(
                                user_id=user_id,
                                conversation_id=self._get_conversation_id(user_id),
                                user_message="[CATALOG ORDER] User selected items from native WhatsApp catalog",
                                agent_response=agent_response,
                                message_type="order",
                                tools_used=tools_used,
                                response_time_ms=response_time_ms,
                                gemini_usage=gemini_usage,
                                eleven_tts_usage=None,
                            )
                        except Exception as e:
                            logger.warning("evaluation.failed", error=str(e))
                        logger.info("agent_and_text.sent", user_id=user_id, used_llm=used_llm)

                        # Billing for catalog order prompt
                        try:
                            base_key = f"{inbound_key}::base"
                            if base_key not in self._emitted_billing_ids:
                                base_event = generate_billing_event_v2(
                                    tenant_id=TENANT_ID,
                                    conversation_id=self._get_conversation_id(user_id),
                                    msg_type=self._billing_msg_type(mtype),
                                    message_id=inbound_key,
                                    role="user",
                                    channel="whatsapp",
                                    conversation_text=agent_prompt,
                                    gemini_usage=None,
                                    eleven_tts_usage=None,
                                )
                                send_billing_event_fire_and_forget(base_event)
                                self._emitted_billing_ids.add(base_key)
                        except Exception as e:
                            logger.warning("billing.order.base_event_failed", error=str(e))

                        try:
                            rated_key = f"{inbound_key}::rated"
                            if rated_key not in self._emitted_billing_ids:
                                rated_event = generate_billing_event_v2(
                                    tenant_id=TENANT_ID,
                                    conversation_id=self._get_conversation_id(user_id),
                                    msg_type=self._billing_msg_type(mtype),
                                    message_id=inbound_key,
                                    role="assistant",
                                    channel="whatsapp",
                                    conversation_text=agent_response,
                                    gemini_usage=gemini_usage,
                                    eleven_tts_usage=None,
                                )
                                send_billing_event_fire_and_forget(rated_event)
                                self._emitted_billing_ids.add(rated_key)
                        except Exception as e:
                            logger.warning("billing.order.rated_event_failed", error=str(e))

                        results.append(
                            {
                                "name": name,
                                "message": "catalog_order_received",
                                "reply_to": replied_to_id,
                                "Agent": agent_response,
                            }
                        )

                    # --------------------------------------------------
                    # INTERACTIVE (buttons, flows, etc.)
                    # --------------------------------------------------
                    elif mtype == "interactive":
                        interactive_data = message.get("interactive", {}) or {}
                        itype = interactive_data.get("type")
                        logger.info(
                            "msg.interactive.in",
                            user_id=user_id,
                            interactive_type=itype,
                            raw=interactive_data,
                            reply_to=replied_to_id,
                        )

                        # ---------- BUTTON REPLY (YES/NO CONFIRM) ----------
                        if itype == "button_reply":
                            button = interactive_data.get("button_reply", {}) or {}
                            button_id = button.get("id")
                            button_title = button.get("title")

                            from agents.tools.order_draft_tools import (
                                _button_already_clicked,
                                _mark_button_clicked,
                            )

                            # Idempotency: block repeated clicks
                            if _button_already_clicked(user_id, button_id):
                                logger.info(
                                    "msg.interactive.button.duplicate",
                                    user_id=user_id,
                                    button_id=button_id,
                                )
                                self.adk_helper._send_text_once(
                                    user_id,
                                    "You have already responded to these buttons. If you need to change anything in the order, just tell me in a message.",
                                    reply_to_message_id=replied_to_id,
                                )
                                results.append(
                                    {
                                        "name": name,
                                        "message": "duplicate_button_click_blocked",
                                        "button_id": button_id,
                                    }
                                )
                                continue

                            # Mark as clicked
                            _mark_button_clicked(user_id, button_id)

                            # Map button IDs to synthetic text
                            if button_id == "ORDER_CONFIRM_YES":
                                # Treat as an explicit confirmation in English so the agent continues in English only.
                                synthetic_text = "yes, please confirm and place this order"
                                # YTL: after explicit order confirmation, prompt for address + site location pin via interactive message.
                                try:
                                    self._send_location_request(user_id, replied_to_id)
                                except Exception as e:
                                    logger.warning("wa.location_request.trigger_failed", user_id=user_id, error=str(e))
                            elif button_id == "ORDER_CONFIRM_NO":
                                synthetic_text = "no, this order is not final yet, I want to change the items"
                            else:
                                synthetic_text = button_title or "button press"

                            logger.info(
                                "msg.interactive.button.mapped",
                                user_id=user_id,
                                button_id=button_id,
                                synthetic_text=synthetic_text,
                            )

                            agent_response = self.adk_helper.handle_message(
                                synthetic_text,
                                user_id,
                                is_voice_input=False,
                                inbound_key=inbound_key,
                                reply_to_message_id=replied_to_id,
                            )

                            # Billing: base + rated
                            try:
                                base_key = f"{inbound_key}::base"
                                if base_key not in self._emitted_billing_ids:
                                    base_event = generate_billing_event_v2(
                                        tenant_id=TENANT_ID,
                                        conversation_id=self._get_conversation_id(user_id),
                                        msg_type=self._billing_msg_type(mtype),
                                        message_id=inbound_key,
                                        role="user",
                                        channel="whatsapp",
                                        conversation_text=synthetic_text,
                                        gemini_usage=None,
                                        eleven_tts_usage=None,
                                    )
                                    send_billing_event_fire_and_forget(base_event)
                                    self._emitted_billing_ids.add(base_key)
                            except Exception as e:
                                logger.warning("billing.interactive.base_event_failed", error=str(e))

                            try:
                                rated_key = f"{inbound_key}::rated"
                                if rated_key not in self._emitted_billing_ids:
                                    rated_event = generate_billing_event_v2(
                                        tenant_id=TENANT_ID,
                                        conversation_id=self._get_conversation_id(user_id),
                                        msg_type=self._billing_msg_type(mtype),
                                        message_id=inbound_key,
                                        role="assistant",
                                        channel="whatsapp",
                                        conversation_text=agent_response,
                                        gemini_usage=self.adk_helper.get_last_gemini_usage(user_id),
                                        eleven_tts_usage=None,
                                    )
                                    send_billing_event_fire_and_forget(rated_event)
                                    self._emitted_billing_ids.add(rated_key)
                            except Exception as e:
                                logger.warning("billing.interactive.rated_event_failed", error=str(e))

                            results.append(
                                {
                                    "name": name,
                                    "message": synthetic_text,
                                    "reply_to": replied_to_id,
                                    "Agent": agent_response,
                                }
                            )

                        # ---------- NATIVE FLOW (NFM) REPLY ----------
                        elif itype == "nfm_reply":
                            nfm = interactive_data.get("nfm_reply", {}) or {}
                            response_json_raw = nfm.get("response_json") or "{}"

                            try:
                                flow_resp = json.loads(response_json_raw)
                            except Exception as e:
                                logger.error("flow.nfm_reply.json_error", user_id=user_id, error=str(e))
                                self.adk_helper._send_text_once(
                                    user_id,
                                    "Flow response parse nahi ho rahi bhai. Please try again.",
                                    reply_to_message_id=replied_to_id,
                                )
                                results.append({"error": "flow_nfm_parse_error", "user_id": user_id})
                                continue

                            payload_data = flow_resp.get("data", {}) or {}
                            confirmed = payload_data.get("confirmed")
                            next_action = payload_data.get("next_action")

                            logger.info(
                                "flow.nfm_reply.parsed",
                                user_id=user_id,
                                confirmed=confirmed,
                                next_action=next_action,
                            )

                            if confirmed is True or next_action == "place_order":
                                synthetic_text = "haan bhai, order final hai, place kar do"
                                # YTL: after Flow-based confirmation, also prompt for site location pin.
                                try:
                                    self._send_location_request(user_id, replied_to_id)
                                except Exception as e:
                                    logger.warning("wa.location_request.trigger_failed_flow", user_id=user_id, error=str(e))

                                agent_response = self.adk_helper.handle_message(
                                    synthetic_text,
                                    user_id,
                                    is_voice_input=False,
                                    inbound_key=inbound_key,
                                    reply_to_message_id=replied_to_id,
                                )
                                
                                results.append({
                                    "name": name,
                                    "message": synthetic_text,
                                    "Agent": agent_response,
                                    "flow_action": "order_confirmed",
                                })

                            elif confirmed is False or next_action == "edit_cart":
                                synthetic_text = "nahi, abhi aur changes karne hain cart mein"
                                agent_response = self.adk_helper.handle_message(
                                    synthetic_text,
                                    user_id,
                                    is_voice_input=False,
                                    inbound_key=inbound_key,
                                    reply_to_message_id=replied_to_id,
                                )
                                
                                results.append({
                                    "name": name,
                                    "message": synthetic_text,
                                    "Agent": agent_response,
                                    "flow_action": "order_cancelled_edit",
                                })

                            else:
                                logger.warning(
                                    "flow.nfm_reply.unexpected_state",
                                    user_id=user_id,
                                    confirmed=confirmed,
                                    next_action=next_action,
                                )
                                self.adk_helper._send_text_once(
                                    user_id,
                                    "Flow se confirmation clear nahi aa rahi bhai.",
                                    reply_to_message_id=replied_to_id,
                                )
                                results.append({"error": "flow_unexpected_state", "user_id": user_id})

                        # ---------- OTHER INTERACTIVE TYPES ----------
                        else:
                            logger.warning(
                                "msg.interactive.unsupported",
                                user_id=user_id,
                                interactive_type=itype,
                            )
                            self.adk_helper._send_text_once(
                                user_id,
                                "Sorry, I couldn't understand this interactive reply.",
                                reply_to_message_id=replied_to_id,
                            )
                            results.append({"error": "Unsupported interactive type", "user_id": user_id})

                    # --------------------------------------------------
                    # UNSUPPORTED TYPES
                    # --------------------------------------------------
                    else:
                        logger.warning("msg.unsupported", user_id=user_id, mtype=mtype)
                        self.adk_helper._send_text_once(
                            user_id,
                            "Sorry, I can only process text, voice, images, and orders.",
                            reply_to_message_id=replied_to_id,
                        )
                        results.append({"error": "Unsupported message type", "user_id": user_id, "mtype": mtype})

                except (KeyError, IndexError, TypeError, ValueError) as per_message_err:
                    logger.error("webhook.message.error", error=str(per_message_err), exc_info=True)
                    results.append({"error": "per-message failure", "detail": str(per_message_err)})
                    continue

            if self.is_twilio:
                return make_response("", 200)
            return make_response(jsonify({"results": results}), 200)

        except (KeyError, IndexError, TypeError, ValueError) as e:
            logger.error("webhook.error", error=str(e), exc_info=True)
            return make_response("", 200)
