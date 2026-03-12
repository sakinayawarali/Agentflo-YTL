
from __future__ import annotations
import asyncio
import base64
import os
import random
import time
import re
from typing import Optional, Dict, Callable, Tuple, TYPE_CHECKING, Any
from google.cloud import firestore
from google.cloud.firestore import SERVER_TIMESTAMP
from utils.logging import logger
from agents.helpers.firestore_utils import get_agent_id

if TYPE_CHECKING:
    # These are ONLY for type checking (Pylance/mypy)
    from agents.audio.generation import TTSGenerator
    from agents.audio.processing import VoiceNoteProcessor
else:
    # At runtime, keep the names available for annotations without importing
    TTSGenerator = Any
    VoiceNoteProcessor = Any

# Runtime imports (use different names so you don't shadow the type names)
try:
    from agents.audio.generation import TTSGenerator as _TTSGenerator
    from agents.audio.processing import VoiceNoteProcessor as _VoiceNoteProcessor
except ImportError:
    _TTSGenerator = None
    _VoiceNoteProcessor = None


class GreetingVNCache:
    MAX_VARIANTS = 5
    CACHE_TTL_DAYS = 30

    def __init__(
        self,
        db: firestore.Client,
        tenant_id: str,
        tts_generator: Optional[TTSGenerator] = None,
        vn_processor: Optional[VoiceNoteProcessor] = None,
        send_audio_func: Optional[Callable] = None,
        get_metadata_func: Optional[Callable] = None,
        agent_id: Optional[str] = None,
    ):
        self.db = db
        self.tenant_id = tenant_id
        # Align VN cache path with main agent_id used elsewhere
        self.agent_id = (agent_id or get_agent_id()).strip()

        if tts_generator:
            self.tts_generator = tts_generator
        elif _TTSGenerator:
            self.tts_generator = _TTSGenerator()
        else:
            raise RuntimeError("TTSGenerator not available - cannot create GreetingVNCache")

        if vn_processor:
            self.vn_processor = vn_processor
        elif _VoiceNoteProcessor:
            self.vn_processor = _VoiceNoteProcessor(language="en")
        else:
            raise RuntimeError("VoiceNoteProcessor not available - cannot create GreetingVNCache")

        # YTL-specific tuning: reduce greeting variants to lower first-hit latency and memory.
        # Uses TENANT_ID/YTL_GREETING_VN_MAX_VARIANTS to keep the change tenant-scoped.
        tenant_env = (os.getenv("TENANT_ID") or "").strip().lower()
        if (self.tenant_id or "").lower() == "ytl" or tenant_env == "ytl":
            try:
                max_variants = int(os.getenv("YTL_GREETING_VN_MAX_VARIANTS", "1"))
            except Exception:
                max_variants = 1
            # Override instance-level MAX_VARIANTS so other tenants remain unchanged.
            self.MAX_VARIANTS = max(1, max_variants)
            logger.info(
                "greeting_cache.ytl_config_applied",
                tenant_id=self.tenant_id,
                max_variants=self.MAX_VARIANTS,
            )

        self._send_audio_func = send_audio_func
        self._get_metadata = get_metadata_func or (lambda user_id: {})
    
    def _user_cache_ref(self, user_id: str):
        """Get Firestore reference to user's greeting VNs collection."""
        return (
            self.db
            .collection("tenants").document(self.tenant_id)
            .collection("agent_id").document(self.agent_id)
            .collection("users").document(user_id)
            .collection("vn").document("greeting_vns")
            .collection("variants")
        )
    
    def _user_cache_meta_ref(self, user_id: str):
        """Get reference to metadata document."""
        return (
            self.db
            .collection("tenants").document(self.tenant_id)
            .collection("agent_id").document(self.agent_id)
            .collection("users").document(user_id)
            .collection("vn").document("greeting_vns")
        )
    
    # ========================================================================
    # PUBLIC API
    # ========================================================================
    
    async def send_greeting_vn(
        self,
        user_id: str,
        reply_to_message_id: Optional[str] = None,
    ) -> bool:
        """
        Send greeting VN from cache only.
        
        Args:
            user_id: WhatsApp user ID
            reply_to_message_id: Optional message ID to reply to
        
        Returns:
            True if sent successfully
        """
        try:
            # 1. Check if feature enabled
            if not self._is_enabled():
                return False
            
            # 2. Try cache first
            cached_vn = self._get_next_variant(user_id)
            metadata = self._get_metadata(user_id)

            # If old cache contains personalized names, wipe and regenerate generic variants.
            if cached_vn and self._script_contains_personal_identity(cached_vn.get("script", ""), metadata):
                logger.info("greeting_vn.cache.personalized.reset", user_id=user_id)
                self.clear_cache(user_id)
                cached_vn = None
            
            if cached_vn:
                logger.info("greeting_vn.using_cache", user_id=user_id)
                return await self._send_vn(
                    user_id,
                    cached_vn["audio_bytes"],
                    cached_vn["meta"],
                    reply_to_message_id,
                )
            
            # 3. Cache miss - populate cache first, then send only from cache.
            # Shield so VN generation survives request lifecycle (e.g. Cloud Run cancelling coroutines).
            logger.info("greeting_vn.cache_miss.populate_then_send", user_id=user_id)
            await asyncio.shield(self.populate_greeting_cache(user_id, force=False))
            cached_vn = self._get_next_variant(user_id)
            if not cached_vn:
                logger.warning("greeting_vn.cache_miss.still_empty", user_id=user_id)
                return False

            logger.info("greeting_vn.using_cache.post_populate", user_id=user_id)
            return await self._send_vn(
                user_id,
                cached_vn["audio_bytes"],
                cached_vn["meta"],
                reply_to_message_id,
            )
            
        except (Exception, asyncio.CancelledError) as e:
            logger.error(
                "greeting_vn.error",
                user_id=user_id,
                error=str(e),
                exc_info=not isinstance(e, asyncio.CancelledError),
                cancelled=isinstance(e, asyncio.CancelledError),
            )
            return False
    
    async def populate_greeting_cache(
        self,
        user_id: str,
        force: bool = False
    ) -> Dict[str, int]:
        """
        Populate greeting cache with MAX_VARIANTS greeting VNs.
        """
        start_ts = time.perf_counter()
        to_generate = 0  # so except block can safely reference it
        try:
            meta_ref = self._user_cache_meta_ref(user_id)
            meta_doc = meta_ref.get()
            
            # CRITICAL FIX: Initialize metadata FIRST if it doesn't exist
            if not meta_doc.exists:
                meta_ref.set({
                    "variant_count": 0,
                    "last_used_index": -1,
                    "created_at": SERVER_TIMESTAMP,
                    "updated_at": SERVER_TIMESTAMP,
                })
                current_count = 0
            else:
                # Force wipe if requested
                if force:
                    self.clear_cache(user_id)
                    meta_ref.set({
                        "variant_count": 0,
                        "last_used_index": -1,
                        "created_at": SERVER_TIMESTAMP,
                        "updated_at": SERVER_TIMESTAMP,
                    })
                    current_count = 0
                else:
                    meta_data = meta_doc.to_dict() or {}
                    current_count = meta_data.get("variant_count", 0)
            
            # Calculate how many to generate
            to_generate = self.MAX_VARIANTS - current_count

            # For YTL demo on Cloud Run (512MiB), avoid spawning too many
            # heavy TTS calls in parallel. Cap the batch size per populate call.
            if (self.tenant_id or "").lower() == "ytl":
                try:
                    max_batch = int(os.getenv("YTL_GREETING_VN_BATCH", "2"))
                except Exception:
                    max_batch = 2
                to_generate = min(to_generate, max_batch)
            if to_generate <= 0:
                elapsed = time.perf_counter() - start_ts
                logger.info(
                    "greeting_cache.already_full",
                    user_id=user_id,
                    count=current_count,
                    elapsed_sec=round(elapsed, 2),
                )
                return {"generated": 0, "skipped": self.MAX_VARIANTS, "failed": 0}
            
            # Get metadata for personalization
            metadata = self._get_metadata(user_id)
            
            generated = 0
            failed = 0

            async def _make_variant(i: int) -> bool:
                nonlocal metadata
                try:
                    # Generate unique script
                    script = await self._generate_script_with_llm(metadata)

                    # Process through VN pipeline
                    vn_text = self.vn_processor.shape_for_tts(script)
                    vn_text = self.vn_processor.shrink_if_needed(vn_text)

                    # Generate audio
                    audio_bytes, meta = await self._generate_vn(vn_text)

                    if audio_bytes:
                        # Save to cache (this increments variant_count)
                        self._add_variant(user_id, audio_bytes, script, meta)
                        logger.info(
                            "greeting_cache.variant_generated",
                            user_id=user_id,
                            index=current_count + i,
                        )
                        return True
                    logger.warning("greeting_cache.generation_failed", user_id=user_id)
                    return False
                except (Exception, asyncio.CancelledError) as e:
                    logger.error(
                        "greeting_cache.variant_error",
                        user_id=user_id,
                        error=str(e),
                        cancelled=isinstance(e, asyncio.CancelledError),
                    )
                    return False

            # Generate variants in parallel to reduce latency.
            tasks = [asyncio.create_task(_make_variant(i)) for i in range(to_generate)]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for r in results:
                if r is True:
                    generated += 1
                else:
                    failed += 1
            
            elapsed = time.perf_counter() - start_ts
            logger.info(
                "greeting_cache.population_complete",
                user_id=user_id,
                generated=generated,
                failed=failed,
                total_variants=current_count + generated,
                elapsed_sec=round(elapsed, 2),
                started_with=current_count,
                attempted=to_generate,
            )
            
            return {
                "generated": generated,
                "skipped": current_count,
                "failed": failed,
            }
        
        except (Exception, asyncio.CancelledError) as e:
            elapsed = time.perf_counter() - start_ts
            logger.error(
                "greeting_cache.populate_error",
                user_id=user_id,
                error=str(e),
                elapsed_sec=round(elapsed, 2),
                cancelled=isinstance(e, asyncio.CancelledError),
            )
            return {"generated": 0, "skipped": 0, "failed": to_generate}
    
    # ========================================================================
    # CACHE OPERATIONS
    # ========================================================================
    
    def _get_next_variant(self, user_id: str) -> Optional[Dict]:
        """Get next greeting variant from cache (rotating)."""
        try:
            meta_ref = self._user_cache_meta_ref(user_id)
            meta_doc = meta_ref.get()
            
            if not meta_doc.exists:
                return None
            
            meta_data = meta_doc.to_dict() or {}
            variant_count = meta_data.get("variant_count", 0)
            last_used_index = meta_data.get("last_used_index", -1)
            
            if variant_count == 0:
                return None
            
            # Rotate to next variant
            next_index = (last_used_index + 1) % variant_count
            
            # Get variant
            variants_ref = self._user_cache_ref(user_id)
            variant_doc = variants_ref.document(f"vn_{next_index}").get()
            
            if not variant_doc.exists:
                logger.warning(
                    "greeting_cache.variant_missing",
                    user_id=user_id,
                    index=next_index,
                )
                return None
            
            variant_data = variant_doc.to_dict()
            audio_b64 = variant_data.get("audio_base64", "")
            
            if not audio_b64:
                return None
            
            # Update last used index
            meta_ref.update({
                "last_used_index": next_index,
                "updated_at": SERVER_TIMESTAMP,
            })
            
            # Decode audio
            audio_bytes = base64.b64decode(audio_b64)
            
            logger.info(
                "greeting_cache.variant_retrieved",
                user_id=user_id,
                index=next_index,
                size_bytes=len(audio_bytes),
            )
            
            return {
                "audio_bytes": audio_bytes,
                "meta": variant_data.get("meta", {}),
                "script": variant_data.get("script", ""),
            }
        
        except Exception as e:
            logger.error("greeting_cache.get_variant_error", user_id=user_id, error=str(e))
            return None
    
    def _add_variant(self, user_id: str, audio_bytes: bytes, script: str, meta: dict) -> bool:
        """Add a single variant to the cache."""
        try:
            # Get current metadata
            meta_ref = self._user_cache_meta_ref(user_id)
            meta_doc = meta_ref.get()
            
            # CRITICAL FIX: Initialize metadata if it doesn't exist
            if not meta_doc.exists:
                meta_ref.set({
                    "variant_count": 0,
                    "last_used_index": -1,
                    "created_at": SERVER_TIMESTAMP,
                    "updated_at": SERVER_TIMESTAMP,
                })
                current_count = 0
            else:
                meta_data = meta_doc.to_dict() or {}
                current_count = meta_data.get("variant_count", 0)
            
            if current_count >= self.MAX_VARIANTS:
                logger.warning(
                    "greeting_cache.add_variant.cache_full",
                    user_id=user_id,
                    current_count=current_count,
                )
                return False
            
            # Encode audio
            audio_b64 = base64.b64encode(audio_bytes).decode("utf-8")
            
            # Create variant document
            variant_data = {
                "audio_base64": audio_b64,
                "meta": meta or {},
                "script": script,
                "created_at": SERVER_TIMESTAMP,
                "size_bytes": len(audio_bytes),
            }
            
            # Write to collection
            doc_id = f"vn_{current_count}"
            collection_ref = self._user_cache_ref(user_id)
            collection_ref.document(doc_id).set(variant_data)
            
            # Update metadata count
            meta_ref.update({
                "variant_count": current_count + 1,
                "updated_at": SERVER_TIMESTAMP,
            })
            
            logger.info(
                "greeting_cache.add_variant.success",
                user_id=user_id,
                doc_id=doc_id,
                new_count=current_count + 1,
            )
            
            return True
        
        except Exception as e:
            logger.error("greeting_cache.add_variant.error", user_id=user_id, error=str(e))
            return False
    
    def clear_cache(self, user_id: str) -> bool:
        """Clear all greeting VN variants for a user."""
        try:
            # Get variant count from metadata
            meta_ref = self._user_cache_meta_ref(user_id)
            meta_doc = meta_ref.get()
            
            if meta_doc.exists:
                meta_data = meta_doc.to_dict() or {}
                variant_count = meta_data.get("variant_count", 0)
                
                # Delete all variant documents
                variants_ref = self._user_cache_ref(user_id)
                for i in range(variant_count):
                    variants_ref.document(f"vn_{i}").delete()
            
            # Delete metadata
            meta_ref.delete()
            
            logger.info("greeting_cache.cleared", user_id=user_id)
            return True
        except Exception as e:
            logger.error("greeting_cache.clear_error", user_id=user_id, error=str(e))
            return False
    
    # ========================================================================
    # SCRIPT GENERATION (LLM - NOT HARDCODED!)
    # ========================================================================
    async def _generate_script_with_llm(self, metadata: dict) -> str:
        """
        Generate greeting script using LLM and Gemini API.
        """
        _ = metadata  # keep signature stable; script stays generic for cache reuse

        # YTL Cement demo: avoid calling Gemini HTTP APIs from Cloud Run.
        # Instead, reuse the same greeting template used for text, so that
        # the voice-note greeting matches the on-screen welcome.
        tenant = (os.getenv("TENANT_ID") or "").strip().lower()
        if tenant == "ytl" or not tenant:
            try:
                from agents.tools.templates import greeting_template as _greet
            except Exception:
                _greet = None  # type: ignore

            customer_name = ""
            if isinstance(metadata, dict):
                customer_name = (
                    metadata.get("customer_name")
                    or metadata.get("contact_name")
                    or ""
                )

            if _greet:
                try:
                    script = _greet(customer_name=customer_name)
                except Exception:
                    script = (
                        "Hi! Welcome to YTL Cement — Building Better since 1955. "
                        "I'm your product assistant here to help you choose the right concrete for your project."
                    )
            else:
                script = (
                    "Hi! Welcome to YTL Cement — Building Better since 1955. "
                    "I'm your product assistant here to help you choose the right concrete for your project."
                )

            logger.info("greeting_vn.static_ytl_script", script_preview=script[:80])
            return script

        try:
            # ✅ Use NEW SDK with strict Gemini (API key, non-Vertex)
            from google import genai
            from google.genai.types import HttpOptions

            api_key = (
                os.getenv("GEMINI_API_KEY")
                or os.getenv("GOOGLE_API_KEY")
                or os.getenv("GENAI_API_KEY")
            )
            if not api_key:
                raise ValueError("GEMINI_API_KEY/GOOGLE_API_KEY not found in environment")

            client = genai.Client(
                api_key=api_key,
                vertexai=False,
                http_options=HttpOptions(
                    baseUrl="https://generativelanguage.googleapis.com",
                    apiVersion="v1",
                ),
            )

            system_prompt = (
                "You are a friendly Pakistani saleswoman (sales agent named Ayesha) representing a brand. "
                "Write in Roman Urdu. Use 'bhai' when addressing the customer, but speak as a woman — "
                "use feminine Urdu verb forms (e.g. 'main bata rahi hoon', 'samjhati hoon'). "
                "Do not use any personal names or store names."
            )

            user_prompt = """Context:
- Message: Catalog has been sent

Write one greeting (2-4 sentences) in Roman Urdu.

Requirements:
- Start with "Salam"
- Address customer only as "bhai" (no person/store names)
- Clearly mention that the catalog has been sent
- Tell the customer they can place an order from it
- Say that if they need help, they can tell you and you will assist them
- Use warm, friendly Pakistani tone
- Speak as a woman (use feminine verbs like "main madad kar doon gi")
- Keep the response to 1-2 SHORT sentences maximum (under 140 characters total)
- NEVER end mid-sentence — always finish the thought before stopping
- No bullets, no numbering, no meta text
"""

            merged_prompt = f"{system_prompt}\n\n{user_prompt}"

            response = client.models.generate_content(
                model="gemini-2.5-flash",
                contents=merged_prompt,
                config={
                    "temperature": 0.7,
                    "max_output_tokens": 3000,
                },
            )

            script = (response.text or "").strip()

            # Post-clean: drop bullets/numbering and pick first non-empty line
            lines = [re.sub(r'^[\\s*\\-\\d\\.\\)]*', '', ln).strip() for ln in script.splitlines()]
            lines = [ln for ln in lines if ln]
            script = lines[0] if lines else ""

            if not script:
                raise ValueError("Empty greeting script after cleaning")

            logger.info("greeting_vn.llm_generated", script_preview=script[:80])
            return script

        except Exception as e:
            logger.error(f"Gemini API script generation failed: {e}")
            # Fallback to simple variations
            variations = [
                "Salam bhai, catalog bhej di hai. Aap us se order place kar sakte hain, madad chahiye ho to bata dein.",
                "Jee bhai, catalog share kar di hai. Jo item chahiye ho batain, main order mein madad kar doon gi.",
            ]
            return random.choice(variations)
    
    # ========================================================================
    # AUDIO GENERATION & SENDING
    # ========================================================================
    
    async def _generate_vn(self, script: str) -> Tuple[Optional[bytes], Dict]:
        """
        Generate VN audio from script.
        
        Returns:
            (audio_bytes, meta_dict) or (None, {})
        """
        try:
            logger.info("greeting_vn.tts_start", text_len=len(script))
            t0 = time.perf_counter()
            preferred_bytes, meta, mp3_bytes = await self.tts_generator.generate_audio(script)
            elapsed = time.perf_counter() - t0
            
            logger.info(
                "greeting_vn.tts_result",
                latency_sec=round(elapsed, 2),
                preferred_size=len(preferred_bytes) if preferred_bytes else 0,
                mp3_size=len(mp3_bytes) if mp3_bytes else 0,
                text_len=len(script),
            )
            
            # For greetings, send whatever we have without size rejection to avoid silent drops.
            if preferred_bytes:
                return preferred_bytes, meta or {}
            if mp3_bytes:
                return mp3_bytes, {"mime": "audio/mpeg", "is_mp3": True, "is_voice": False}

            logger.error(
                "greeting_vn.generation_none",
                text_len=len(script),
            )
            return None, {}
            
        except (Exception, asyncio.CancelledError) as e:
            logger.error(
                "greeting_vn.generation_exception",
                error=str(e),
                cancelled=isinstance(e, asyncio.CancelledError),
            )
            return None, {}
    
    async def _send_vn(
        self,
        user_id: str,
        audio_bytes: bytes,
        meta: Dict,
        reply_to_message_id: Optional[str],
    ) -> bool:
        """Send VN to user."""
        if not self._send_audio_func:
            logger.error("greeting_cache.no_send_func")
            return False
        
        try:
            is_voice = meta.get("is_voice", True)
            is_mp3 = meta.get("is_mp3", False)

            logger.info(
                "greeting_vn.send_start",
                user_id=user_id,
                bytes=len(audio_bytes) if audio_bytes else 0,
                voice=is_voice,
                mp3=is_mp3,
            )

            sent = await self._send_audio_func(
                user_id,
                audio_bytes,
                voice=is_voice,
                mp3=is_mp3,
                meta=meta,
                reply_to_message_id=reply_to_message_id,
            )

            logger.info("greeting_vn.send_result", user_id=user_id, sent=bool(sent))
            return bool(sent)
            
        except Exception as e:
            logger.error("greeting_vn.send_exception", error=str(e))
            return False
    
    # ========================================================================
    # UTILITIES
    # ========================================================================
    
    def _is_enabled(self) -> bool:
        """Check if greeting VN feature is enabled."""
        return os.getenv("CATALOG_GREETING_VN_ENABLED", "true").lower() == "true"

    def _script_contains_personal_identity(self, script: str, metadata: Dict[str, Any]) -> bool:
        """
        Detect if a cached script contains customer/store identity tokens.
        Used to purge old personalized variants after moving to generic cache policy.
        """
        if not isinstance(script, str) or not script.strip():
            return False
        if not isinstance(metadata, dict):
            return False

        script_l = script.lower()
        raw_candidates = []
        for key in ("customer_name", "contact_name", "retailer_name", "owner_name", "store_name", "store_name_en"):
            val = metadata.get(key)
            if isinstance(val, str) and val.strip():
                cleaned = re.sub(r"\s+", " ", val).strip().lower()
                raw_candidates.append(cleaned)
                first = cleaned.split(" ", 1)[0]
                if first:
                    raw_candidates.append(first)

        ignore = {
            "bhai",
            "jee",
            "ji",
            "bhai ji",
            "store",
            "shop",
            "mart",
            "dukaan",
            "aapki",
            "apki",
        }
        for cand in {c for c in raw_candidates if c}:
            if cand in ignore or len(cand) < 3:
                continue
            if re.search(rf"\b{re.escape(cand)}\b", script_l):
                return True
        return False
    
    def _too_small(self, text: str, audio_bytes: bytes, kbps: int = 8) -> bool:
        """Check if audio is suspiciously small for the text length."""
        if not audio_bytes or not text:
            return True
        
        text_len = len(text)
        audio_size = len(audio_bytes)
        
        # Rough heuristic: expect ~kbps per second of speech
        # Average speech: ~3 chars per second
        expected_seconds = text_len / 3.0
        expected_bytes = (kbps * 1000 / 8) * expected_seconds * 0.3  # 30% threshold
        
        return audio_size < expected_bytes
    
    # ========================================================================
    # ADMIN / DEBUG
    # ========================================================================
    
    def get_cache_status(self, user_id: str) -> Dict:
        """Get cache status for monitoring."""
        try:
            meta_ref = self._user_cache_meta_ref(user_id)
            meta_doc = meta_ref.get()
            
            if not meta_doc.exists:
                return {
                    "exists": False,
                    "variant_count": 0,
                    "cache_age_days": None,
                    "last_used_at": None,
                }
            
            data = meta_doc.to_dict() or {}
            variant_count = data.get("variant_count", 0)
            created_at = data.get("created_at")
            last_used_at = data.get("last_used_at")
            
            # Calculate age
            if created_at:
                created_ts = created_at.timestamp() if hasattr(created_at, 'timestamp') else created_at
                now = time.time()
                cache_age_days = (now - created_ts) / 86400
            else:
                cache_age_days = None
            
            return {
                "exists": True,
                "variant_count": variant_count,
                "cache_age_days": round(cache_age_days, 1) if cache_age_days else None,
                "last_used_at": last_used_at,
                "is_expired": cache_age_days > self.CACHE_TTL_DAYS if cache_age_days else False,
            }
            
        except Exception as e:
            logger.error("greeting_cache.status_error", user_id=user_id, error=str(e))
            return {
                "exists": False,
                "variant_count": 0,
                "cache_age_days": None,
                "last_used_at": None,
                "error": str(e),
            }