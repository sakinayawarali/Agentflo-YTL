import os
import time
from typing import Optional

from agents.helpers.adk.audio_utils import _too_small_for_safety
from agents.helpers.firestore_utils import get_tenant_id, user_root
from utils.logging import logger


class JobsMixin:
    # ---------- Task helpers (used by /tasks/tts-send) ----------
    def vn_job_already_sent(self, job_id: str, user_id: str) -> bool:
        if not (job_id and user_id):
            return False
        try:
            try:
                base_ref = self._user_root(user_id)  # type: ignore[attr-defined]
            except Exception:
                tenant = getattr(self, "tenant_id", get_tenant_id())
                base_ref = user_root(self.db, user_id, tenant_id=tenant)
            ref = base_ref.collection("vn_jobs").document(self._sha(job_id))
            doc = ref.get()
            data = (doc.to_dict() or {}) if doc.exists else {}
            return bool(data.get("sent"))
        except Exception as e:
            logger.warning(f"vn_job_already_sent check failed (will send): {e}")
            return False

    def mark_vn_job_sent(self, job_id: str, user_id: str) -> None:
        if not (job_id and user_id):
            return
        try:
            try:
                base_ref = self._user_root(user_id)  # type: ignore[attr-defined]
            except Exception:
                tenant = getattr(self, "tenant_id", get_tenant_id())
                base_ref = user_root(self.db, user_id, tenant_id=tenant)
            ref = base_ref.collection("vn_jobs").document(self._sha(job_id))
            ref.set({"sent": True, "ts": time.time()}, merge=True)
        except Exception as e:
            logger.warning(f"mark_vn_job_sent failed: {e}")

    async def _gen_and_send_vn(self, to_number: str, text: str) -> None:
        """
        Utility used by the Cloud Tasks endpoint. Sends a VN (no reply_to threading).
        Now always translates text to Arabic before TTS.
        """
        try:
            # 1) Clean system phrase
            cleaned = self._clean_system_phrase(text or "")

            # 3) Shrink if too long
            tts_text = self._shrink_text_for_tts(cleaned, self.tts_max_chars)

            t0 = time.perf_counter()
            preferred_bytes, meta, mp3_bytes = await self._tts_get_audio_bytes(tts_text)
            elapsed = time.perf_counter() - t0
            logger.info(f"TTS generation took {elapsed:.2f}s", user_id=to_number, meta=meta)

            if preferred_bytes and not _too_small_for_safety(len(tts_text), preferred_bytes, kbps=24):
                sent = await self._upload_and_send_audio(
                    to_number,
                    preferred_bytes,
                    voice=True,
                    mp3=False,
                    meta=meta,
                    reply_to_message_id=None,
                )
                if sent:
                    logger.info("vn.sent.ok (tasks)", user_id=to_number)
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

            if self.audio_fallback_to_mp3 and mp3_bytes and not _too_small_for_safety(len(tts_text), mp3_bytes, kbps=24):
                sent = await self._upload_and_send_audio(
                    to_number,
                    mp3_bytes,
                    voice=False,
                    mp3=True,
                    meta={"mime": "audio/mpeg"},
                    reply_to_message_id=None,
                )
                if sent:
                    logger.info("audio.sent.ok (tasks mp3 fallback)", user_id=to_number)
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

            logger.warning("TTS generation failed or too small; sending text fallback. (tasks)", user_id=to_number)
            self._send_text_once(to_number, text)
        except Exception as e:
            logger.error("task.vn.error -> text fallback", error=str(e), user_id=to_number)
            self._send_text_once(to_number, text)
