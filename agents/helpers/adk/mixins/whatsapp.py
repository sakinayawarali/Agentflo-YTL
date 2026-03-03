# import hashlib
# import os
# import re
# import tempfile
# import time
# from typing import List, Optional

# import aiohttp
# from agents.audio.utils import sniff_audio_mime
# from utils.logging import logger


# class WhatsAppMixin:
#     async def _upload_and_send_audio(
#         self,
#         to_number: str,
#         audio_bytes: bytes,
#         *,
#         voice: bool,
#         mp3: bool = False,
#         meta: Optional[dict] = None,
#         reply_to_message_id: Optional[str] = None
#     ) -> bool:
#         """
#         Uploads file to WhatsApp media endpoint and sends message.
#         Uses meta['mime'] first (if present) to determine whether to set voice/PTT.
#         If meta not provided or ambiguous, falls back to byte-sniffing.

#         If reply_to_message_id is provided, the outbound message will appear
#         as a quoted reply to that WAMID in the WhatsApp UI.
#         """
#         tmp_suffix = ".mp3" if mp3 else ".ogg"
#         # meta may contain 'mime' or 'path'
#         mime_from_meta = None
#         if meta and isinstance(meta, dict):
#             mime_from_meta = meta.get("mime")

#         # decide mime to upload
#         if mime_from_meta:
#             ctype = mime_from_meta
#         else:
#             # fallback to sniffing bytes
#             sniff = _sniff_audio_mime(audio_bytes)
#             ctype = "audio/mpeg" if "mpeg" in sniff else "audio/ogg; codecs=opus"
#             # force mp3 extension if sniff says mpeg
#             if "mpeg" in sniff:
#                 tmp_suffix = ".mp3"
#                 mp3 = True

#         # ensure PTT/voice only if the mime indicates OGG/Opus
#         voice_flag = False
#         if mime_from_meta:
#             if "ogg" in (mime_from_meta or "") or "opus" in (mime_from_meta or ""):
#                 voice_flag = True
#             else:
#                 voice_flag = False
#         else:
#             if "ogg" in ctype or "opus" in ctype:
#                 voice_flag = True
#             else:
#                 voice_flag = False

#         # normalized content type for upload (WhatsApp expects common types)
#         upload_content_type = "audio/mpeg" if mp3 or "mpeg" in ctype else "audio/ogg"

#         try:
#             headers = {"Authorization": f"Bearer {self.whatsapp_token}"}
#             with tempfile.NamedTemporaryFile(delete=False, suffix=tmp_suffix, prefix="audio-") as f:
#                 f.write(audio_bytes)
#                 path = f.name

#             try:
#                 async with aiohttp.ClientSession() as session:
#                     with open(path, "rb") as fh:
#                         data = aiohttp.FormData()
#                         data.add_field("file", fh, filename=os.path.basename(path), content_type=upload_content_type)
#                         data.add_field("messaging_product", "whatsapp")
#                         async with session.post(self.whatsapp_media_url, headers=headers, data=data, timeout=90) as resp:
#                             if resp.status not in (200, 201):
#                                 txt = await resp.text()
#                                 logger.error(f"Audio upload failed {resp.status}: {txt}")
#                                 return False
#                             js = await resp.json()
#                             media_id = js.get("id")

#                 payload = {
#                     "messaging_product": "whatsapp",
#                     "to": to_number,
#                     "type": "audio",
#                     "audio": {"id": media_id},
#                 }
#                 if voice_flag:
#                     payload["audio"]["voice"] = True

#                 if reply_to_message_id:
#                     payload["context"] = {"message_id": reply_to_message_id}

#                 r = self.http_session.post(self.whatsapp_api_url, json=payload, timeout=20)
#                 if r.status_code in (200, 201):
#                     logger.info("wa.audio.sent", to=to_number, mode=("voice" if voice_flag else "audio"))
#                     return True
#                 logger.error(f"WA audio send failed {r.status_code}: {r.text}")
#                 return False
#             finally:
#                 try:
#                     os.unlink(path)
#                 except Exception:
#                     pass
#         except Exception as e:
#             logger.error(f"Upload/send audio exception: {e}")
#             return False

#     def _chunk_whatsapp_text(self, text: str) -> List[str]:
#         """
#         Split a WhatsApp text into limit-safe chunks favoring sentence/newline boundaries.
#         Keeps SKU lines intact; only falls back to word/limit slicing if a single unit
#         is longer than the limit.
#         """
#         if not text:
#             return []

#         raw_limit = max(1, int(self.whatsapp_text_limit or 4096))
#         limit = min(raw_limit, 4000)  # preempt the 4096 cap with a safer ceiling

#         if len(text) <= limit:
#             return [text]

#         def split_units(s: str) -> List[str]:
#             """
#             Break text on sentence endings or newlines while keeping delimiters attached.
#             """
#             units: List[str] = []
#             start = 0
#             for m in re.finditer(r'(?<=[.!?])\s+|\n', s):
#                 end = m.end()
#                 units.append(s[start:end])
#                 start = end
#             if start < len(s):
#                 units.append(s[start:])
#             return units

#         def split_long_unit(unit: str) -> List[str]:
#             """
#             Split a single over-limit unit on the nearest space; if none, hard-slice.
#             """
#             pieces: List[str] = []
#             text_left = unit
#             while len(text_left) > limit:
#                 cut = text_left.rfind(" ", 0, limit)
#                 if cut == -1:
#                     cut = limit
#                 pieces.append(text_left[:cut].rstrip())
#                 text_left = text_left[cut:].lstrip()
#             if text_left:
#                 pieces.append(text_left)
#             return pieces

#         chunks: List[str] = []
#         current = ""

#         for unit in split_units(text):
#             candidate = unit if not current else f"{current}{unit}"

#             if len(candidate) <= limit:
#                 current = candidate
#                 continue

#             if current:
#                 chunks.append(current.rstrip())
#                 current = ""

#             if len(unit) > limit:
#                 for piece in split_long_unit(unit):
#                     if len(piece) <= limit:
#                         if current and len(current) + len(piece) + 1 <= limit:
#                             current = f"{current} {piece}"
#                         elif current:
#                             chunks.append(current.rstrip())
#                             current = piece
#                         else:
#                             current = piece
#                     else:
#                         chunks.append(piece)
#                         current = ""
#                 continue

#             current = unit

#         if current:
#             chunks.append(current.rstrip())

#         return chunks

#     # ---------- Text send (with dedupe) ----------
#     def _send_text_once(self, to_number: str, message_body: str, reply_to_message_id: Optional[str] = None) -> bool:
#         """
#         Sends a text message with dedupe. If reply_to_message_id is provided,
#         this will appear as a quoted reply in the WhatsApp UI.
#         Also normalizes forbidden wording like 'system mein nahi mil raha'
#         → 'ye mere paas nahi hai'.
#         """
#         if not message_body:
#             return False
#         # Clean up phrasing before sending to the user
#         message_body = self._clean_system_phrase(message_body)

#         if not self.whatsapp_token or not self.phone_number_id:
#             logger.error("Missing WhatsApp credentials.")
#             return False

#         chunks = self._chunk_whatsapp_text(message_body)
#         if not chunks:
#             return False
#         if len(chunks) > 1:
#             logger.info("wa.text.chunked", to=to_number, parts=len(chunks), orig_len=len(message_body))

#         overall_success = True

#         for idx, chunk in enumerate(chunks):
#             if self.text_dedupe_enabled and not self._ok_to_send_text(to_number, chunk):
#                 logger.info("wa.text.skipped_duplicate", to=to_number, part=idx + 1)
#                 continue

#             payload = {
#                 "messaging_product": "whatsapp",
#                 "to": to_number,
#                 "type": "text",
#                 "text": {"body": chunk}
#             }
#             if reply_to_message_id and idx == 0:
#                 payload["context"] = {"message_id": reply_to_message_id}

#             try:
#                 r = self.http_session.post(self.whatsapp_api_url, json=payload, timeout=20)
#                 if r.status_code in (200, 201):
#                     logger.info("wa.text.sent", to=to_number, part=idx + 1, total=len(chunks))
#                     self._record_text_hash(to_number, chunk)
#                     continue
#                 overall_success = False
#                 logger.error(f"WA text send failed {r.status_code} (part {idx + 1}/{len(chunks)}): {r.text}")
#             except Exception as e:
#                 overall_success = False
#                 logger.error(f"WA text send exception (part {idx + 1}/{len(chunks)}): {e}")

#         return overall_success

#     def _msg_sha(self, text: str) -> str:
#         return hashlib.sha1((text or "").encode("utf-8")).hexdigest()

#     def _ok_to_send_text(self, user_id: str, text: str) -> bool:
#         try:
#             doc_ref = self._user_root(user_id)
#             doc = doc_ref.get()
#             if not doc.exists:
#                 return True
#             data = doc.to_dict() or {}
#             last_sha = data.get("last_text_sha")
#             last_ts = data.get("last_text_epoch")
#             now = time.time()
#             if last_sha and last_ts and last_sha == self._msg_sha(text) and (now - float(last_ts) < self.text_dedupe_sec):
#                 return False
#             return True
#         except Exception as e:
#             logger.warning(f"Text dedupe check failed (will send): {e}")
#             return True

#     def _record_text_hash(self, user_id: str, text: str):
#         try:
#             self._user_root(user_id).set(
#                 {"last_text_sha": self._msg_sha(text), "last_text_epoch": time.time()},
#                 merge=True
#             )
#         except Exception as e:
#             logger.warning(f"Text dedupe record failed: {e}")
