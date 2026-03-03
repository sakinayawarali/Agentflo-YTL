"""
Voice note transcription using Gemini 2.5 Flash.
Handles:
- WhatsApp voice note download and transcription
- Enhanced audio preprocessing (noise reduction, normalization)
- Product catalog awareness for accurate transcription
- Two-pass transcription with correction
- Fuzzy matching for common brand mishearings
- Multi-script support (Urdu script, Roman Urdu, English)
"""
import os
import re
import tempfile
from google.genai import types
from google.genai.types import HttpOptions, Part
from google import genai
import requests
from dotenv import load_dotenv
import subprocess
from difflib import get_close_matches
from typing import Tuple, Optional

from utils.logging import logger

load_dotenv()
class VoiceNoteTranscriber:
    """
    Transcribes WhatsApp voice notes using Gemini 2.5 Flash with enhanced accuracy.
    
    Features:
    - Fetch media from WhatsApp → download → re-encode to 16kHz mono WAV with noise reduction
    - Product catalog integration for accurate brand name recognition
    - Two-pass transcription with correction
    - Fuzzy matching for common mishearings
    - Supports two output modes:
        - script="urdu": Urdu script output (with English brand names)
        - script="roman": Roman Urdu / Hinglish (Latin letters only)
    - Domain-aware prompt for FMCG brands, orders, and quantities
    """

    # FIXED: Updated to stable model name
    DEFAULT_MODEL = "gemini-2.5-flash"
    
    # Product catalog - update this list with your actual products
    PRODUCT_CATALOG = [
        "Sooper Classic Tikki Pack",
        "Sooper Half Roll",
        "Sooper Family Pack",
        "Peak Freans",
        "Rio biscuit",
        "Cake Up",
            ]
    
    # Common mishearings mapped to correct names
    COMMON_CORRECTIONS = {
        # Sooper variants
        "sooper ticky": "Sooper Classic Tikki Pack",
        "sooper tikki": "Sooper Classic Tikki Pack",
        "super ticky": "Sooper Classic Tikki Pack",
        "super tikki pack": "Sooper Classic Tikki Pack",
        "sooper classic": "Sooper Classic Tikki Pack",
        "super classic": "Sooper Classic Tikki Pack",
        "soper tikki": "Sooper Classic Tikki Pack",
        
        # Peak Freans variants
        "peak france": "Peak Freans",
        "peak friends": "Peak Freans",
        "peek freans": "Peak Freans",
        "peak freens": "Peak Freans",
        
        # Rio variants (biggest issue)
        "real biscuit": "Rio biscuit",
        "real packet": "Rio packet",
        "real pack": "Rio pack",
        "riyo": "Rio",
        "rioo": "Rio",
        
        # Cake Up variants
        "cake app": "Cake Up",
        "cakeup": "Cake Up",
                
    }

    PAKISTANI_LANGUAGES = {
        "ur": "ur",        # Urdu (best support)
        "pa": "pa",        # Punjabi (romanized works OK)
        "sd": "sd",        # Sindhi
        "ps": "ps",        # Pashto
        "bal": "ur",       # Balochi — fallback to Urdu voice, closest prosody
        "en": "en",        # English (mixed code-switching is common)
        "hi": "hi",        # Hindi (sometimes used interchangeably with Urdu)
    }
    
    def __init__(self):
        # --- FIXED AUTHENTICATION (Google AI Studio, API key path) ---
        api_key = (
            os.getenv("GEMINI_API_KEY")
            or os.getenv("GOOGLE_API_KEY")
            or os.getenv("GENAI_API_KEY")
        )
        if not api_key:
            logger.error("CRITICAL: GEMINI_API_KEY/GOOGLE_API_KEY missing for Transcription")

        http_opts = HttpOptions(
            baseUrl="https://generativelanguage.googleapis.com",
            apiVersion="v1",
        )
        self.client = genai.Client(api_key=api_key, vertexai=False, http_options=http_opts)
        self.gemini_client = self.client
        self.gemini_model = self.DEFAULT_MODEL

        # WhatsApp and feature flags
        wa_token = os.getenv("WHATSAPP_TOKEN") or os.getenv("WHATSAPP_ACCESS_TOKEN")
        if not wa_token:
            logger.warning("WHATSAPP_TOKEN missing; media downloads may fail")
        self.headers = {"Authorization": f"Bearer {wa_token}"} if wa_token else {}
        self.media_fetch_timeout = int(os.getenv("MEDIA_FETCH_TIMEOUT", "15"))

        # Optional transcription behaviors
        self.use_two_pass = os.getenv("TRANSCRIBE_USE_TWO_PASS", "false").lower() == "true"
        self.use_fuzzy_matching = os.getenv("TRANSCRIBE_USE_FUZZY", "true").lower() != "false"

    # ====================================================================================
    # PUBLIC API
    # ====================================================================================

    def _download_and_convert(self, url: str) -> str:
        """
        Downloads audio from URL and converts to 16kHz Mono WAV (ideal for STT).
        """
        # Download
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        
        # Save temp original
        with tempfile.NamedTemporaryFile(delete=False, suffix=".ogg") as tmp_orig:
            tmp_orig.write(r.content)
            tmp_orig_path = tmp_orig.name

        # Output path
        fd, tmp_wav = tempfile.mkstemp(suffix=".wav")
        os.close(fd)

        # FFmpeg conversion: 16k sample rate, 1 channel (mono), pcm_s16le codec
        cmd = [
            "ffmpeg", "-y",
            "-i", tmp_orig_path,
            "-ar", "16000",
            "-ac", "1",
            "-c:a", "pcm_s16le",
            tmp_wav
        ]
        
        try:
            subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            os.remove(tmp_orig_path)
            if os.path.exists(tmp_wav):
                os.remove(tmp_wav)
            raise RuntimeError(f"FFmpeg conversion failed: {e}")

        os.remove(tmp_orig_path)
        return tmp_wav
    
    def transcribe_whatsapp_vn(
        self,
        media_id: str,
        *,
        to_english: bool = False,
        script: str = "roman",          # default roman — works across all languages
        detect_lang: bool = True,       # NEW flag
    ) -> Tuple[str, str]:               # now returns (text, lang_code)
        """
        Returns:
            (transcribed_text, language_code)
            language_code: "ur", "pa", "sd", "ps", "bal", "en", "hi"
        """
        audio_path = None
        wav_path = None

        try:
            media_url = self.get_media_url(media_id)
            audio_path = self.download_audio(media_url)
            wav_path = self._to_wav_16k_mono(audio_path)

            # Detect language FIRST (same wav, single extra Gemini call)
            lang_code = "ur"
            if detect_lang:
                lang_code = self.detect_language(wav_path)

            # Auto-select script based on language
            # Urdu/Hindi → urdu script works; everything else → roman safer
            effective_script = script
            if detect_lang and lang_code not in ("ur", "hi") and script == "urdu":
                effective_script = "roman"
                logger.info("lang.script_override", lang=lang_code, forced="roman")

            text = self.transcribe_audio(
                wav_path,
                to_english=to_english,
                script=effective_script,
                language_hint=lang_code,   # pass hint into prompt (see Step 3)
            )

            return (text or "").strip(), lang_code

        except Exception as e:
            logger.error("wa.media.transcribe_failed", media_id=media_id, error=str(e))
            raise
        finally:
            for p in (wav_path, audio_path):
                if p and os.path.exists(p):
                    try:
                        os.unlink(p)
                    except Exception:
                        pass

    def detect_language(self, audio_path: str) -> str:
        """
        Detect the spoken language in an audio file using Gemini.

        Returns an ISO 639-1/3 code: "ur", "pa", "sd", "ps", "bal", "en", "hi"
        Defaults to "ur" if detection fails or is ambiguous.
        """
        with open(audio_path, "rb") as f:
            audio_data = f.read()

        prompt = """Listen to this voice note carefully.
    Identify the PRIMARY spoken language. Choose ONLY from this list:

    - ur  (Urdu)
    - pa  (Punjabi / Panjabi)
    - sd  (Sindhi)
    - ps  (Pashto / Pakhto)
    - bal (Balochi)
    - hi  (Hindi)
    - en  (English)

    Respond with ONLY the language code, nothing else. No explanation. No punctuation."""

        try:
            response = self.client.models.generate_content(
                model=self.DEFAULT_MODEL,
                contents=[
                    types.Content(
                        parts=[
                            types.Part.from_bytes(data=audio_data, mime_type="audio/wav"),
                            types.Part.from_text(text=prompt),
                        ]
                    )
                ],
                config=types.GenerateContentConfig(temperature=0.0),
            )
            detected = (response.text or "").strip().lower()
            # Validate against known codes
            if detected in self.PAKISTANI_LANGUAGES:
                logger.info("lang.detected", code=detected)
                return detected
            logger.warning("lang.detect_unknown", raw=detected)
            return "ur"
        except Exception as e:
            logger.warning(f"lang.detect_failed: {e}")
            return "ur"

    def transcribe_audio(
        self,
        file_path: str,
        *,
        to_english: bool = False,
        script: str = "urdu",
        language_hint: str = "ur", 
    ) -> str:
        """
        Core transcription with enhanced accuracy.
        
        - script="urdu": Urdu script output (with English brand names)
        - script="roman": Roman Urdu / Hinglish (Latin letters only)
        - to_english=True: English translation (ignores `script`)
        
        Improvements:
        - Product catalog in prompt
        - Two-pass transcription (optional)
        - Fuzzy matching post-processing (optional)
        
        Args:
            file_path: Path to audio file (WAV preferred)
            to_english: Translate to English
            script: Output script ("urdu" or "roman")
        
        Returns:
            Transcribed text
        """
        logger.info(
            "transcribe.start",
            path=file_path,
            to_english=to_english,
            script=script,
            two_pass=self.use_two_pass,
            fuzzy_match=self.use_fuzzy_matching,
        )

        if to_english:
            return self._translate_to_english(file_path)

        if script not in ("urdu", "roman"):
            script = "urdu"

        # Get the appropriate prompt with product catalog
        prompt = self._build_transcription_prompt(script)

        try:
            # First pass: transcription with product catalog awareness
            text = self._call_gemini(file_path, script, language_hint=language_hint)
            lang = language_hint

            # If we asked for Roman but got Urdu script, retry forcing Latin chars
            if script == "roman" and self._contains_urdu_script(text):
                logger.warning(
                    "transcribe.roman_urdu_retry_urdu_script_detected",
                    detected_lang=lang,
                    preview=text[:80],
                )
                text, lang = self._gemini_transcribe_raw(
                    file_path=file_path,
                    prompt=prompt,
                    language="en",  # this usually forces Latin characters
                )

            # Second pass: correction with product catalog (if enabled)
            if self.use_two_pass and text:
                text = self._correct_with_product_catalog(text, script)

            # Fuzzy matching for brand names (if enabled)
            if self.use_fuzzy_matching and text:
                text = self._apply_fuzzy_corrections(text)

            # Post-processing based on script
            if script == "roman":
                text = self._post_process_roman(text)
            
            # Common domain cleanups
            text = self._normalize_common_mishears(text)

            logger.info(
                "transcribe.done",
                text_len=len(text),
                detected_lang=lang,
                script=script,
            )
            return text

        except Exception as e:
            logger.error("transcribe.error", error=str(e), path=file_path)
            raise

    # ====================================================================================
    # WHATSAPP MEDIA DOWNLOAD
    # ====================================================================================

    def get_media_url(self, media_id: str) -> str:
        """Get media download URL from WhatsApp."""
        api_version = os.getenv("WHATSAPP_API_VER", "v23.0")
        url = f"https://graph.facebook.com/{api_version}/{media_id}"

        try:
            r = requests.get(
                url,
                headers=self.headers,
                timeout=self.media_fetch_timeout
            )
            r.raise_for_status()
            media_url = r.json().get("url")
            if not media_url:
                raise ValueError(f"No URL in WhatsApp media response: {r.text}")
            logger.info("wa.media.url_ok", media_id=media_id, url=media_url)
            return media_url
        except requests.RequestException as e:
            logger.error("wa.media.url_error", media_id=media_id, error=str(e))
            raise

    def download_audio(self, media_url: str) -> str:
        """Download audio from WhatsApp media URL."""
        try:
            r = requests.get(
                media_url,
                headers=self.headers,
                timeout=self.media_fetch_timeout
            )
            r.raise_for_status()
            audio_data = r.content

            # Validate content type
            ctype = (r.headers.get("Content-Type") or "").lower()
            size = len(audio_data)

            if "audio" not in ctype:
                logger.error(
                    "wa.media.not_audio",
                    content_type=ctype,
                    size=size,
                )
                raise RuntimeError(
                    f"WhatsApp media not audio: ctype={ctype} size={size}"
                )

            # Hard fail only if it's truly empty
            if size == 0:
                logger.error("wa.media.empty", size=size)
                raise RuntimeError("WhatsApp audio appears to be empty (0 bytes)")

            # Save to temp file
            _, save_path = tempfile.mkstemp(suffix=".ogg")
            with open(save_path, "wb") as f:
                f.write(audio_data)

            logger.info(
                "wa.media.download_ok",
                path=save_path,
                size=size,
                content_type=ctype,
            )
            return save_path

        except requests.RequestException as e:
            logger.error("wa.media.download_error", url=media_url, error=str(e))
            raise

    # ====================================================================================
    # AUDIO PREPROCESSING
    # ====================================================================================

    def _to_wav_16k_mono(self, in_path: str) -> str:
        """
        Re-encode to 16 kHz mono WAV (16-bit PCM) with enhanced noise reduction.
        
        Improvements:
        - Highpass filter to remove low-frequency noise (< 200 Hz)
        - Lowpass filter to remove high-frequency noise (> 3400 Hz)
        - FFT-based noise reduction
        - Dynamic range compression for consistent volume
        - Loudness normalization
        
        This makes WhatsApp OPUS notes much clearer for transcription.
        """
        out_fd, out_path = tempfile.mkstemp(suffix=".wav")
        os.close(out_fd)
        
        cmd = [
            "ffmpeg",
            "-y",
            "-i",
            in_path,
            "-ac",
            "1",  # mono
            "-ar",
            "16000",  # 16kHz (optimal for Gemini)
            "-c:a",
            "pcm_s16le",  # 16-bit PCM
            "-af",
            # Enhanced audio filter chain for speech clarity
            (
                "highpass=f=200,"      # Remove low-frequency rumble
                "lowpass=f=3400,"      # Remove high-frequency noise (phone speech is 300-3400 Hz)
                "afftdn=nf=-20,"       # FFT denoiser (noise floor -20dB)
                "loudnorm,"            # Normalize loudness
                "acompressor=threshold=-20dB:ratio=4:attack=5:release=50"  # Compress dynamics
            ),
            "-vn",
            out_path,
        ]
        
        try:
            subprocess.run(
                cmd,
                check=True,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=300,
            )
            logger.info(
                "audio.ffmpeg_enhanced_ok",
                in_path=in_path,
                out_path=out_path,
            )
            return out_path
        except subprocess.TimeoutExpired:
            logger.warning("audio.ffmpeg_timeout", error="FFmpeg took too long")
            try:
                os.remove(out_path)
            except Exception:
                pass
            return in_path
        except Exception as e:
            logger.warning("audio.ffmpeg_fail", error=str(e))
            try:
                os.remove(out_path)
            except Exception:
                pass
            return in_path

    # ====================================================================================
    # GEMINI TRANSCRIPTION
    # ====================================================================================

    def _build_transcription_prompt(self, script: str) -> str:
        """Build transcription prompt with product catalog."""
        product_list = "\n".join(f"- {p}" for p in self.PRODUCT_CATALOG)
        
        if script == "roman":
            return f"""Transcribe this audio accurately in Roman Urdu (using Latin letters only).

IMPORTANT PRODUCT NAMES (spell these EXACTLY as shown):
{product_list}

Rules:
- Use ONLY Latin letters (a-z, A-Z)
- Use digits for numbers (1, 2, 3...)
- Write product names EXACTLY as listed above
- For "Rio", NEVER write "real" - always write "Rio"
- Keep quantities as digits (e.g., "12 cartons")
- Maintain natural Hinglish/Roman Urdu flow
- No Urdu script characters"""
        else:
            return f"""Transcribe this audio accurately in Urdu script.

IMPORTANT PRODUCT NAMES (keep in English):
{product_list}

Rules:
- Use Urdu script for regular speech
- Keep product brand names in English (e.g., Rio, Peak Freans)
- Use digits for quantities
- For "Rio", NEVER write it as "real"
- Maintain natural conversational Urdu"""

    LANG_PROMPT_HINTS = {
        "pa": "The speaker is using Punjabi (Pakistani Punjabi / Shahmukhi). Transcribe accurately.",
        "sd": "The speaker is using Sindhi. Transcribe accurately.",
        "ps": "The speaker is using Pashto. Transcribe accurately.",
        "bal": "The speaker is using Balochi. Transcribe accurately.",
        "hi": "The speaker may mix Hindi and Urdu (Hindustani). Transcribe accurately.",
        "en": "The speaker is using English, possibly mixed with Urdu.",
        "ur": "",  # default — existing prompts already handle this
    }

    def _call_gemini(self, audio_path: str, script: str, language_hint: str = "ur") -> str:
        with open(audio_path, "rb") as f:
            audio_data = f.read()

        lang_note = self.LANG_PROMPT_HINTS.get(language_hint, "")

        if script == "urdu":
            sys_instruction = (
                f"You are an expert transcriber for Pakistani audio. {lang_note} "
                "Transcribe the audio into the appropriate script. "
                "For Urdu/Hindi use Urdu/Nastaliq script. "
                "For other languages use their native script if possible, otherwise Roman. "
                "Output ONLY the transcription text."
            )
        else:
            sys_instruction = (
                f"You are an expert transcriber for Pakistani audio. {lang_note} "
                "Transcribe the audio using Roman/Latin letters (Roman Urdu, Roman Punjabi, etc.). "
                "Keep FMCG brand names in English. Output ONLY the transcription text."
            )

        response = self.client.models.generate_content(
            model=self.DEFAULT_MODEL,
            contents=[
                types.Content(
                    parts=[
                        types.Part.from_bytes(data=audio_data, mime_type="audio/wav"),
                        types.Part.from_text(text=f"{sys_instruction}\n\nTranscribe this voice note."),
                    ]
                )
            ],
            config=types.GenerateContentConfig(
                temperature=0.2,
                # system_instruction removed — not supported in this SDK version
            ),
        )
        return response.text.strip() if response.text else ""

    def _gemini_transcribe_raw(
        self,
        *,
        file_path: str,
        prompt: str | None,
        language: str | None,
    ) -> Tuple[str, str]:
        with open(file_path, "rb") as f:
            audio_bytes = f.read()

        instruction = (prompt or "").strip()
        if language == "en":
            instruction += "\n\nOutput only in Roman script (English letters)."
        elif language == "ur":
            instruction += "\n\nOutput in Urdu script."
        instruction += "\n\nReturn only the transcription text; no extra commentary."

        # FIXED: Using correct model name
        resp = self.gemini_client.models.generate_content(
            model=self.gemini_model,
            contents=[
                instruction,
                Part.from_bytes(data=audio_bytes, mime_type="audio/wav"),
            ],
            config=types.GenerateContentConfig(
                temperature=0.1,
            ),
        )

        text_out = (getattr(resp, "text", None) or "").strip()
        return text_out, ""

    def _translate_to_english(self, file_path: str) -> str:
        """Translate speech to English (ignores script preference)."""
        text, _ = self._gemini_transcribe_raw(
            file_path=file_path,
            prompt="Translate this speech to English. Output only the English text, no commentary.",
            language="en",
        )
        text = (text or "").strip()
        logger.info("transcribe.translation_done", text_len=len(text))
        return text

    # ====================================================================================
    # POST-PROCESSING & CORRECTION
    # ====================================================================================

    def _correct_with_product_catalog(self, text: str, script: str) -> str:
        if not text:
            return text
        
        try:
            product_list = "\n".join(f"- {p}" for p in self.PRODUCT_CATALOG)
            correction_prompt = f"""Review and correct this transcription, focusing on product names.

Product catalog:
{product_list}

Original transcription:
{text}

Correct any misspelled product names. Output ONLY the corrected transcription."""

            # FIXED: Using correct model name
            resp = self.gemini_client.models.generate_content(
                model=self.gemini_model,
                contents=correction_prompt,
                config=types.GenerateContentConfig(
                    temperature=0.1,
                ),
            )
            
            corrected = (resp.text or "").strip()
            
            if corrected and len(corrected) > len(text) * 0.5:  # Safety check
                logger.info("transcribe.two_pass_correction_applied")
                return corrected
            else:
                logger.warning("transcribe.two_pass_suspicious_result")
                return text
                
        except Exception as e:
            logger.warning(f"transcribe.two_pass_failed: {e}")
            return text

    def _apply_fuzzy_corrections(self, text: str) -> str:
        """Apply fuzzy matching for common brand mishearings."""
        if not text:
            return text
        
        # Apply exact corrections first
        corrected = text
        for wrong, right in self.COMMON_CORRECTIONS.items():
            pattern = re.compile(rf'\b{re.escape(wrong)}\b', re.IGNORECASE)
            corrected = pattern.sub(right, corrected)
        
        # Fuzzy matching for partial matches
        words = corrected.split()
        for i, word in enumerate(words):
            # Check if word might be a misspelled product name
            clean_word = re.sub(r'[^\w\s]', '', word.lower())
            matches = get_close_matches(
                clean_word,
                [p.lower() for p in self.PRODUCT_CATALOG],
                n=1,
                cutoff=0.75
            )
            if matches:
                # Find the original casing in PRODUCT_CATALOG
                for product in self.PRODUCT_CATALOG:
                    if product.lower() == matches[0]:
                        words[i] = product
                        break
        
        return " ".join(words)

    def _contains_urdu_script(self, text: str) -> bool:
        """Detect if any Arabic/Urdu script characters are present."""
        if not text:
            return False
        for ch in text:
            code = ord(ch)
            if (
                0x0600 <= code <= 0x06FF  # Arabic
                or 0x0750 <= code <= 0x077F  # Arabic Supplement
                or 0xFB50 <= code <= 0xFDFF  # Arabic Presentation Forms
            ):
                return True
        return False

    def _post_process_roman(self, text: str) -> str:
        """
        Light Roman Urdu post-processing:
        - normalize spacing
        - fix common brand mis-hears
        """
        if not text:
            return text

        # Additional corrections specific to Roman Urdu
        corrections = {
            
            "rio": "Rio",  # Capitalize common brands
        }

        t = text
        for wrong, right in corrections.items():
            pattern = re.compile(rf'\b{re.escape(wrong)}\b', re.IGNORECASE)
            t = pattern.sub(right, t)

        # normalize whitespace
        t = " ".join(t.split())
        return t

    def _normalize_common_mishears(self, text: str) -> str:
        """
        Domain cleanups applied to all transcripts (Urdu/Roman).
        """
        if not text:
            return text

        t = text

        # Targeted Rio fixes (backup to fuzzy matching)
        rio_patterns = [
            (r"\breal\s+(pack|packs|packet|packets|biscuit|biscuits)\b", r"Rio \1"),
            (r"\brioo\b", "Rio"),
        ]
        for pat, repl in rio_patterns:
            t = re.sub(pat, repl, t, flags=re.IGNORECASE)

        # normalize whitespace
        t = " ".join(t.split())
        return t