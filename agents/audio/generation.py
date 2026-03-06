"""
TTS (Text-to-Speech) audio generation using ElevenLabs.

Consolidates:
- eleven_tool.py → ElevenLabs HTTP/streaming, chunking, ffmpeg conversion
- tts.py (TTSMixin) → High-level TTS interface, fallback chains, audio upload

Provides a clean TTSGenerator class that handles all voice note generation.
"""

import asyncio
import base64
import json
import os
import re
import shutil
import subprocess
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError as FutureTimeoutError
from typing import Optional, Dict, Tuple, List, Any

import requests  # type: ignore[import-untyped]
from dotenv import load_dotenv

from utils.logging import logger
from agents.audio.utils import (
    trim_trailing_silence,
    sniff_audio_mime,
    is_audio_too_small,
)

load_dotenv()


# ============================================================================
# CONFIGURATION & HELPERS
# ============================================================================

def _env(name: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(name, default)


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _eleven_defaults() -> Dict[str, Any]:
    """Get ElevenLabs configuration from environment."""
    return {
        "ELEVEN_API_KEY": _env("ELEVENLABS_API_KEY"),
        "ELEVEN_VOICE_ID": _env("ELEVENLABS_VOICE_ID", "RMnOVvJd4cSvgcHmUpJP"),
        "ELEVEN_MODEL_ID": _env("ELEVENLABS_MODEL_ID", "eleven_turbo_v2_5"),
        "ELEVEN_OUTPUT_FORMAT": _env("ELEVENLABS_OUTPUT_FORMAT", "mp3_22050_32"),
        "ELEVEN_DICTIONARY_ID": _env("ELEVENLABS_PRON_DICT_ID"),
        # Parallel & chunking
        "VN_PARALLEL": _int_env("VN_PARALLEL", 2),
        "VN_CHUNK_CHARS": _int_env("VN_CHUNK_CHARS", 300),
        # HTTP timeouts
        "VN_TTS_TIMEOUT": _int_env("VN_TTS_TIMEOUT", 1600),
        "VN_CHUNK_TIMEOUT": _int_env("VN_CHUNK_TIMEOUT", 1600),
        # FFmpeg timeouts
        "VN_FFMPEG_DECODE_TIMEOUT": _int_env("VN_FFMPEG_DECODE_TIMEOUT", 250),
        "VN_FFMPEG_CONCAT_TIMEOUT": _int_env("VN_FFMPEG_CONCAT_TIMEOUT", 750),
        "VN_FFMPEG_ENCODE_TIMEOUT": _int_env("VN_FFMPEG_ENCODE_TIMEOUT", 350),
    }


# Minimum timeouts so retries/fallbacks aren't doomed when global deadline is tight
TTS_HTTP_MIN_TIMEOUT = 8   # seconds; ElevenLabs needs at least this per request
TTS_FFMPEG_MIN_TIMEOUT = 5  # seconds; decode/encode/concat floor


def _time_left(deadline_ts: Optional[float], floor: float = 0.0) -> float:
    """Seconds remaining until deadline_ts (based on perf_counter())."""
    if not deadline_ts:
        return 1e9
    return max(floor, deadline_ts - time.perf_counter())


def _ffmpeg_bin() -> Optional[str]:
    """Find ffmpeg binary."""
    explicit = _env("FFMPEG_BIN")
    if explicit and os.path.isfile(explicit):
        return explicit
    explicit_win = _env("FFMPEG_BIN_WINDOWS")
    if explicit_win and os.path.isfile(explicit_win):
        return explicit_win
    return shutil.which("ffmpeg") or shutil.which("ffmpeg.exe")


# ============================================================================
# TEXT PREPARATION FOR TTS
# ============================================================================

# Markdown/cleanup patterns
MD_LINK = re.compile(r'\[([^\]]+)\]\(([^)]+)\)')
MD_BOLD = re.compile(r'(\*\*|__)(.+?)\1')
MD_ITAL = re.compile(r'(\*|_)(.+?)\1')
MD_CODE = re.compile(r'`([^`]+)`')
URL = re.compile(r'(https?://\S+|www\.\S+)')
EMOJI = re.compile(r'[\U00010000-\U0010ffff]')
SPC = re.compile(r'\s+')


def strip_markdown_for_audio(text: str) -> str:
    """Remove markdown, URLs, emojis for clean TTS."""
    t = text or ""
    t = MD_LINK.sub(r'\1', t)
    t = MD_CODE.sub(r'\1', t)
    t = MD_BOLD.sub(r'\2', t)
    t = MD_ITAL.sub(r'\2', t)
    t = URL.sub("", t)
    t = EMOJI.sub("", t)
    t = SPC.sub(" ", t).strip()
    return t


def normalize_units_currency(s: str) -> str:
    """Light text normalization BEFORE TTS (remove 'N x M' patterns)."""
    if not s:
        return s
    # Remove "N x M" math - VN layer decides how to phrase
    s = re.sub(r'\b(\d+)\s*[xX×]\s*(\d+)\b', r'\1 \2', s)
    s = re.sub(r'\s+', ' ', s).strip()
    return s


def make_full_audio_script(original: str) -> str:
    """
    Prepare text for TTS:
    - strip markdown / emojis / URLs
    - normalise basic patterns (e.g. "N x M")
    - ensure it ends with a sentence terminator
    """
    if not original:
        return ""
    text = strip_markdown_for_audio(original)
    text = normalize_units_currency(text)
    # Ensure clean sentence ending
    if not re.search(r'[\.!\?…۔]$', text):
        text += "."
    return text


_SENT_SPLIT = re.compile(r'(?<=[\.!\?۔…])\s+')


def split_text_into_chunks(text: str, target_chars: int) -> List[str]:
    """Split text into chunks for parallel TTS."""
    sents = [s.strip() for s in _SENT_SPLIT.split(text) if s.strip()]
    chunks, cur = [], ""
    for s in sents:
        if not cur:
            cur = s
        elif len(cur) + 1 + len(s) <= target_chars:
            cur += " " + s
        else:
            chunks.append(cur)
            cur = s
    if cur:
        chunks.append(cur)
    
    # Hard-wrap very long sentences
    out: List[str] = []
    for ch in chunks:
        if len(ch) <= target_chars + 40:
            out.append(ch)
            continue
        words = ch.split()
        seg: List[str] = []
        cnt = 0
        for w in words:
            wlen = len(w) + (1 if cnt > 0 else 0)
            if cnt + wlen > target_chars:
                out.append(" ".join(seg))
                seg = [w]
                cnt = len(w)
            else:
                seg.append(w)
                cnt += wlen
        if seg:
            out.append(" ".join(seg))
    return out


# ============================================================================
# AUDIO TAGS (EMOTION, ACCENT, BREATHS)
# ============================================================================

EMOTION_TAGS = {
    "excited": "[excited]",
    "friendly": "[friendly]",
    "empathetic": "[empathetic]",
    "calm": "[calm]",
    "conversational": "[conversational]",
}

ACCENT_TAGS = {
    # YTL Cement demo: keep accent neutral for English VNs.
    "pakistani": "",
    "indian": "",
    "neutral": "",
}


def maybe_add_breaths(text: str, level: str = "off") -> str:
    """Optional pacing tweaks (neutral by default)."""
    if not text or level == "off":
        return text
    # Light/medium: ensure small pause after sentence endings
    return re.sub(r'([\.!\?…؟۔])\s+', r'\1 ', text)


def apply_audio_tags(
    text: str,
    emotion: str = "friendly",
    accent: str = "neutral",
    add_breaths: str = "off",
    whisper: bool = False,
    emphasis: bool = False,
) -> str:
    """Add ElevenLabs audio tags to text."""
    prefix = []
    if accent and ACCENT_TAGS.get(accent, ""):
        prefix.append(ACCENT_TAGS[accent])
    if emotion and EMOTION_TAGS.get(emotion, ""):
        prefix.append(EMOTION_TAGS[emotion])
    if whisper:
        prefix.append("[whispers]")
    if emphasis:
        prefix.append("[speaks with emphasis]")

    tagged = (" ".join(prefix) + " " + text).strip()
    if add_breaths and add_breaths != "off":
        tagged = maybe_add_breaths(tagged, add_breaths)
    return tagged


# ============================================================================
# ELEVENLABS HTTP API
# ============================================================================

def sniff_audio(b: bytes) -> str:
    """Return 'audio/ogg' if OggS header, 'audio/mpeg' if MP3, else 'application/octet-stream'."""
    if not b or len(b) < 4:
        return "application/octet-stream"
    head4 = b[:4]
    if head4 == b"OggS":
        return "audio/ogg"
    if head4[:3] == b"ID3" or head4[:2] in (b"\xff\xfb", b"\xff\xf3", b"\xff\xf2"):
        return "audio/mpeg"
    return "application/octet-stream"


def eleven_tts_http_bytes(
    text: str,
    voice_id: str,
    model_id: str,
    output_format: str,
    timeout: int,
    pronunciation_dictionary_ids: Optional[List[str]] = None,
    stability: float = 0.5,
    similarity_boost: float = 0.90,
    style: float = 0.70,
    speaker_boost: bool = True,
    deadline_ts: Optional[float] = None,
    language_code: Optional[str] = None,
) -> Tuple[bool, bytes, str, str]:
    """Call ElevenLabs TTS API. Returns (ok, bytes, mime, error)."""
    cfg = _eleven_defaults()
    api_key = cfg["ELEVEN_API_KEY"]
    if not api_key:
        return False, b"", "", "Missing ELEVEN_API_KEY"

    url = f"https://api.elevenlabs.io/v1/text-to-speech/{voice_id}"
    accept = "audio/wav" if str(output_format or "").startswith("wav") else "audio/mpeg"
    headers = {"xi-api-key": str(api_key), "accept": accept, "Content-Type": "application/json"}

    payload: Dict[str, object] = {
        "text": text,
        "model_id": model_id,
        "voice_settings": {
            "stability": float(stability),
            "similarity_boost": float(similarity_boost),
            "style": float(style),
            "use_speaker_boost": bool(speaker_boost),
        },
        "output_format": output_format,
    }

    ELEVEN_LANG_MAP = {
        "ur": "ur", "pa": "pa", "hi": "hi", "en": "en",
        "sd": None, "ps": None, "bal": "ur",
    }

    mapped = ELEVEN_LANG_MAP.get(language_code or "", None)
    if mapped:
        payload["language_code"] = mapped

    if pronunciation_dictionary_ids:
        payload["pronunciation_dictionary_locators"] = [
            {"pronunciation_dictionary_id": pid, "version_id": "latest"}
            for pid in pronunciation_dictionary_ids
        ]

    # Clamp to remaining budget; floor ensures retries/fallbacks get a realistic timeout
    http_timeout = int(min(timeout, max(TTS_HTTP_MIN_TIMEOUT, _time_left(deadline_ts))))

    try:
        resp = requests.post(
            url, headers=headers, data=json.dumps(payload), timeout=(3, http_timeout), stream=False
        )
        if resp.status_code != 200:
            err_msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
            logger.warning(f"ElevenLabs API error: {err_msg}")
            return False, b"", "", err_msg

        b = resp.content
        if not b or len(b) < 500:
            return False, b"", "", f"Audio too small: {len(b)} bytes"

        mime = resp.headers.get("Content-Type", accept)
        return True, b, mime, ""
    except requests.Timeout:
        logger.warning(f"ElevenLabs timeout after {http_timeout}s for text: {text[:50]}...")
        return False, b"", "", f"Timeout after {http_timeout}s"
    except Exception as e:
        logger.exception("ElevenLabs TTS HTTP failed")
        return False, b"", "", str(e)


# ============================================================================
# FFMPEG HELPERS
# ============================================================================

def bytes_to_wav16k_mono(in_bytes: bytes, ffmpeg_path: str, in_ext_hint: str, *, timeout_sec: int = 600) -> bytes:
    """Convert audio bytes to 16kHz mono WAV."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=f".{in_ext_hint}") as src:
        src.write(in_bytes)
        src.flush()
        src_path = src.name
    dst_path = src_path + ".wav"
    try:
        cmd = [
            ffmpeg_path, "-y", "-hide_banner", "-loglevel", "error",
            "-i", src_path, "-ar", "16000", "-ac", "1", "-c:a", "pcm_s16le", dst_path
        ]
        actual_timeout = max(timeout_sec, 600)
        subprocess.run(cmd, check=True, timeout=actual_timeout)
   
        with open(dst_path, "rb") as f:
            return f.read()
    finally:
        for p in (src_path, dst_path):
            try:
                os.remove(p)
            except Exception:
                pass


def wav_to_ogg_opus(wav_blob: bytes, ffmpeg_path: str, *, timeout_sec: int = 600) -> bytes:
    """Encode WAV to OGG/Opus."""
    with tempfile.NamedTemporaryFile(delete=False, suffix=".wav") as src:
        src.write(wav_blob)
        src.flush()
        src_path = src.name
    out_path = src_path + ".ogg"
    try:
        cmd = [
            ffmpeg_path, "-y", "-hide_banner", "-loglevel", "error",
            "-i", src_path,
            "-ar", "16000", "-ac", "1",
            "-c:a", "libopus",
            "-application", "voip",
            "-compression_level", "4",
            "-b:a", "24k",
            "-frame_duration", "20",
            out_path
        ]
        actual_timeout = max(timeout_sec, 600)
        subprocess.run(cmd, check=True, timeout=actual_timeout)
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        for p in (src_path, out_path):
            try:
                os.remove(p)
            except Exception:
                pass


def concat_wavs_to_ogg_opus(wav_blobs: List[bytes], ffmpeg_path: str, *, timeout_sec: int = 600) -> bytes:
    """Concat WAVs then encode to OGG/Opus."""
    if len(wav_blobs) == 1:
        return wav_to_ogg_opus(wav_blobs[0], ffmpeg_path, timeout_sec=timeout_sec)

    tmpdir = tempfile.mkdtemp(prefix="vn_join_")
    list_file = os.path.join(tmpdir, "list.txt")
    paths = []
    try:
        for i, data in enumerate(wav_blobs):
            p = os.path.join(tmpdir, f"{i:03d}.wav")
            with open(p, "wb") as f:
                f.write(data)
            paths.append(p)
        with open(list_file, "w", encoding="utf-8") as f:
            for p in paths:
                f.write(f"file '{p}'\n")
        out_path = os.path.join(tmpdir, "out.ogg")
        cmd = [
            ffmpeg_path, "-y", "-hide_banner", "-loglevel", "error",
            "-f", "concat", "-safe", "0", "-i", list_file,
            "-ar", "16000", "-ac", "1",
            "-c:a", "libopus",
            "-application", "voip",
            "-compression_level", "4",
            "-b:a", "24k",
            "-frame_duration", "20",
            out_path
        ]
        actual_timeout = max(timeout_sec, 600)
        subprocess.run(cmd, check=True, timeout=actual_timeout)
        with open(out_path, "rb") as f:
            return f.read()
    finally:
        try:
            for p in paths:
                os.remove(p)
            if os.path.exists(list_file):
                os.remove(list_file)
            os.rmdir(tmpdir)
        except Exception:
            pass


# ============================================================================
# SINGLE-SHOT TTS (FALLBACK)
# ============================================================================

def synthesize_elevenlabs_single(
    text: str,
    voice_id: Optional[str] = None,
    model_id: Optional[str] = None,
    extra_aliases: Optional[Dict[str, str]] = None,
    pronunciation_dict_ids: Optional[List[str]] = None,
    emotion: str = "friendly",
    accent: str = "pakistani",
    language_code: Optional[str] = None,
    breaths: str = "off",
    whisper: bool = False,
    emphasis: bool = False,
    output_format: Optional[str] = None,
    deadline_ts: Optional[float] = None,
) -> Dict:
    """Single-shot ElevenLabs TTS (no chunking)."""
    cfg = _eleven_defaults()
    if not text or not text.strip():
        return {"success": False, "error": "Empty text"}

    script = make_full_audio_script(text)
    if extra_aliases:
        for k, alias in extra_aliases.items():
            script = re.sub(rf'\b{re.escape(k)}\b', alias, script, flags=re.I)
    
    vn_text_tagged = apply_audio_tags(
        script, emotion=emotion, accent=accent, add_breaths=breaths,
        whisper=whisper, emphasis=emphasis
    )

    ok, audio_bytes, mime, err = eleven_tts_http_bytes(
        vn_text_tagged,
        voice_id=voice_id or cfg["ELEVEN_VOICE_ID"],
        model_id=model_id or cfg["ELEVEN_MODEL_ID"],
        output_format=(output_format or cfg["ELEVEN_OUTPUT_FORMAT"]),
        timeout=cfg["VN_TTS_TIMEOUT"],
        language_code=language_code,
        pronunciation_dictionary_ids=pronunciation_dict_ids or (
            [cfg["ELEVEN_DICTIONARY_ID"]] if cfg["ELEVEN_DICTIONARY_ID"] else None
        ),
        deadline_ts=deadline_ts,
    )
    if not ok:
        return {"success": False, "error": err or "TTS HTTP failed"}

    ffmpeg = _ffmpeg_bin()
    if ffmpeg:
        decode_to = int(cfg["VN_FFMPEG_DECODE_TIMEOUT"])
        enc_budget = int(cfg["VN_FFMPEG_ENCODE_TIMEOUT"])
        dec_budget = min(decode_to, max(TTS_FFMPEG_MIN_TIMEOUT, int(_time_left(deadline_ts))))
        enc_budget = min(enc_budget, max(TTS_FFMPEG_MIN_TIMEOUT, int(_time_left(deadline_ts))))
        
        if (mime or "").startswith("audio/wav") or str(output_format or "").startswith("wav"):
            wav = audio_bytes
        else:
            ext_hint = "mp3" if "mpeg" in (mime or "") else "bin"
            wav = bytes_to_wav16k_mono(audio_bytes, ffmpeg, ext_hint, timeout_sec=dec_budget)
        
        ogg = wav_to_ogg_opus(wav, ffmpeg, timeout_sec=enc_budget)
        sniff = sniff_audio(ogg)
        b64 = base64.b64encode(ogg).decode("utf-8")
        return {
            "success": True,
            "audio_data": b64,
            "audio_format": "ogg" if sniff == "audio/ogg" else "bin",
            "mime": sniff if sniff == "audio/ogg" else "application/octet-stream",
            "method": "elevenlabs_http",
            "bytes_len": len(ogg),
        }

    # No ffmpeg → return raw
    sniff = sniff_audio(audio_bytes)
    b64 = base64.b64encode(audio_bytes).decode("utf-8")
    return {
        "success": True,
        "audio_data": b64,
        "audio_format": "mp3" if sniff == "audio/mpeg" else "bin",
        "mime": sniff,
        "method": "elevenlabs_http",
        "bytes_len": len(audio_bytes),
    }


# ============================================================================
# PARALLEL CHUNKED TTS
# ============================================================================

def synthesize_elevenlabs_parallel(
    text: str,
    voice_id: Optional[str] = None,
    model_id: Optional[str] = None,
    extra_aliases: Optional[Dict[str, str]] = None,
    pronunciation_dict_ids: Optional[List[str]] = None,
    emotion: str = "friendly",
    accent: str = "neutral",
    breaths: str = "off",
    whisper: bool = False,
    emphasis: bool = False,
    chunk_chars: Optional[int] = None,
    parallel: Optional[int] = None,
    per_chunk_timeout: Optional[int] = None,
    language_code: Optional[str] = None, 
    output_format: Optional[str] = None,
    deadline_ts: Optional[float] = None,
) -> Dict:
    """Fast parallel chunked TTS."""
    cfg = _eleven_defaults()
    if not text or not text.strip():
        return {"success": False, "error": "Empty text"}

    ffmpeg = _ffmpeg_bin()
    if not ffmpeg:
        logger.warning("FFmpeg not found; falling back to single-shot")
        return synthesize_elevenlabs_single(
            text, voice_id=voice_id, model_id=model_id,
            emotion=emotion, accent=accent, breaths=breaths,
            whisper=whisper, emphasis=emphasis,
            output_format=output_format, deadline_ts=deadline_ts,
        )

    # Prepare script
    script = make_full_audio_script(text)
    if extra_aliases:
        for k, alias in extra_aliases.items():
            script = re.sub(rf'\b{re.escape(k)}\b', alias, script, flags=re.I)

    # Split
    target = int(chunk_chars or cfg["VN_CHUNK_CHARS"])
    chunks = split_text_into_chunks(script, target_chars=target)
    if not chunks:
        return {"success": False, "error": "Nothing to synthesize"}

    # Tag first chunk
    tagged_first = apply_audio_tags(
        chunks[0], emotion=emotion, accent=accent, add_breaths=breaths,
        whisper=whisper, emphasis=emphasis
    )
    chunks_tagged = [tagged_first] + chunks[1:]

    # Parallel synth
    voice = voice_id or cfg["ELEVEN_VOICE_ID"]
    model = model_id or cfg["ELEVEN_MODEL_ID"]
    out_fmt = output_format or "wav_16000"
    http_timeout = int(per_chunk_timeout or cfg["VN_TTS_TIMEOUT"])
    future_timeout = int(cfg["VN_CHUNK_TIMEOUT"])
    decode_to = int(cfg["VN_FFMPEG_DECODE_TIMEOUT"])
    concat_to = int(cfg["VN_FFMPEG_CONCAT_TIMEOUT"])
    pron = pronunciation_dict_ids or ([cfg["ELEVEN_DICTIONARY_ID"]] if cfg["ELEVEN_DICTIONARY_ID"] else None)
    max_workers = int(parallel or cfg["VN_PARALLEL"])

    def _synthesize_chunk(idx_and_text):
        idx, txt = idx_and_text
        for attempt in range(2):
            try:
                ok, b, mime, err = eleven_tts_http_bytes(
                    txt, voice, model, out_fmt, http_timeout,
                    pronunciation_dictionary_ids=pron,
                    deadline_ts=deadline_ts,
                    language_code=language_code,   
                )
                if not ok:
                    if attempt == 0:
                        logger.warning(f"Chunk {idx} attempt {attempt+1} failed: {err}, retrying...")
                        time.sleep(0.4)
                        continue
                    raise RuntimeError(f"Chunk {idx} failed after 2 attempts: {err}")

                if (mime or "").startswith("audio/wav") or str(out_fmt).startswith("wav"):
                    wav = b
                else:
                    ext_hint = "mp3" if "mpeg" in (mime or "") else "bin"
                    wav = bytes_to_wav16k_mono(
                        b, ffmpeg, ext_hint,
                        timeout_sec=min(int(decode_to), max(TTS_FFMPEG_MIN_TIMEOUT, int(_time_left(deadline_ts))))
                    )
                return idx, wav
            except Exception as e:
                if attempt == 0:
                    logger.warning(f"Chunk {idx} exception on attempt {attempt+1}: {e}, retrying...")
                    time.sleep(0.4)
                    continue
                raise RuntimeError(f"Chunk {idx} failed: {e}")
        raise RuntimeError(f"Chunk {idx} exhausted retries")

    wavs: List[Tuple[int, bytes]] = []
    failed = []
    started = time.perf_counter()

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(_synthesize_chunk, (i, ch)): i for i, ch in enumerate(chunks_tagged)}
        for future in as_completed(futures, timeout=future_timeout * max(1, len(chunks_tagged))):
            idx_hint = futures[future]
            try:
                idx, wav = future.result(timeout=future_timeout)
                wavs.append((idx, wav))
                logger.info(f"Chunk {idx}/{len(chunks_tagged)-1} completed")
            except FutureTimeoutError:
                logger.error(f"Chunk {idx_hint} TIMEOUT after {future_timeout}s")
                failed.append(idx_hint)
            except Exception as e:
                logger.error(f"Chunk {idx_hint} FAILED: {e}")
                failed.append(idx_hint)

    elapsed = time.perf_counter() - started

    
    if len(failed) > 0:
        logger.error(f"Chunk failure detected ({len(failed)}/{len(chunks_tagged)} failed). Falling back to single-shot to prevent text truncation.")
        return synthesize_elevenlabs_single(
            text, voice_id=voice_id, model_id=model_id,
            emotion=emotion, accent=accent, breaths=breaths,
            output_format=out_fmt, deadline_ts=deadline_ts,
        )
    # ---------------------

    if not wavs:
        return {"success": False, "error": "All chunks failed"}

    wavs.sort(key=lambda x: x[0])
    wav_blobs = [w for _, w in wavs]

    try:
        ogg = concat_wavs_to_ogg_opus(
            wav_blobs, ffmpeg,
            timeout_sec=min(int(concat_to), max(TTS_FFMPEG_MIN_TIMEOUT, int(_time_left(deadline_ts))))
        )
    except subprocess.TimeoutExpired:
        logger.error(f"FFmpeg concat timed out")
        return synthesize_elevenlabs_single(
            text, voice_id=voice_id, model_id=model_id,
            emotion=emotion, accent=accent, breaths=breaths,
            output_format="wav_16000", deadline_ts=deadline_ts,
        )
    except Exception as e:
        logger.error(f"FFmpeg concat failed: {e}")
        return {"success": False, "error": f"Concat failed: {e}"}

    sniff = sniff_audio(ogg)
    b64 = base64.b64encode(ogg).decode("utf-8")
    logger.info(f"Parallel TTS completed: {len(wavs)}/{len(chunks_tagged)} chunks in {elapsed:.2f}s")
    return {
        "success": True,
        "audio_data": b64,
        "audio_format": "ogg" if sniff == "audio/ogg" else "bin",
        "mime": sniff if sniff == "audio/ogg" else "application/octet-stream",
        "method": "elevenlabs_parallel",
        "bytes_len": len(ogg),
        "chunks": len(wavs),
        "chunks_failed": len(failed),
        "elapsed_sec": round(elapsed, 2),
    }


# ============================================================================
# HIGH-LEVEL TTS GENERATOR CLASS
# ============================================================================

class TTSGenerator:
    """
    High-level TTS generator with fallback chains.
    
    Replaces:
    - eleven_tool.py → synthesize_elevenlabs_vn, synthesize_elevenlabs_full_vn_fast
    - tts.py (TTSMixin) → _tts_get_audio_bytes, _eleven_*, etc.
    
    Usage:
        generator = TTSGenerator()
        preferred_bytes, meta, mp3_bytes = await generator.generate_audio(text)
    """
    
    def __init__(self):
        self.eleven_api_key = os.getenv("ELEVENLABS_API_KEY", "")
        self.eleven_voice_id = os.getenv("ELEVENLABS_VOICE_ID", "")
        self.eleven_model_id = os.getenv("ELEVENLABS_MODEL_ID", "eleven_turbo_v2_5")
        # Total budget for TTS + FFmpeg; use 25–30s so single-shot + fallbacks can succeed.
        self.tts_global_deadline_sec = float(os.getenv("TTS_GLOBAL_DEADLINE_SEC", "30"))
        self.enable_parallel = os.getenv("VN_PARALLEL_ENABLED", "true").lower() == "true"
        
        # Session for HTTP requests
        self.session = requests.Session()
        try:
            from requests.adapters import HTTPAdapter  # type: ignore[import-untyped]
            from urllib3.util.retry import Retry
            adapter = HTTPAdapter(
                pool_connections=10,
                pool_maxsize=10,
                max_retries=Retry(total=1, backoff_factor=0.2)
            )
            self.session.mount("https://", adapter)
        except Exception:
            pass
    
    async def generate_audio(
        self,
        text: str,
        *,
        voice_id: Optional[str] = None,
        model_id: Optional[str] = None,
        emotion: str = "friendly",
        accent: str = "neutral",
        breaths: str = "off",
        **kwargs
    ) -> Tuple[Optional[bytes], dict, Optional[bytes]]:
        """
        Generate TTS audio with fallback chain.
        
        Returns:
            (preferred_bytes, meta, mp3_bytes)
            - preferred_bytes: OGG Opus (if available)
            - meta: Metadata dict
            - mp3_bytes: MP3 fallback (if available)
        """
        if not text or not text.strip():
            return None, {}, None
        
        start_ts = time.perf_counter()
        deadline_ts = start_ts + self.tts_global_deadline_sec
        time_left = deadline_ts - time.perf_counter()
        if time_left <= 0:
            logger.warning(
                "tts.deadline_already_expired",
                time_left_sec=round(time_left, 2),
                deadline_sec=self.tts_global_deadline_sec,
            )
        elif time_left < 5:
            logger.info(
                "tts.low_deadline_budget",
                time_left_sec=round(time_left, 2),
                deadline_sec=self.tts_global_deadline_sec,
            )

        # Single-shot TTS (parallel chunked path removed to avoid cascading timeouts and redundant fallbacks)
        try:
            result = await asyncio.to_thread(
                synthesize_elevenlabs_single,
                text,
                voice_id=voice_id or self.eleven_voice_id,
                model_id=model_id or self.eleven_model_id,
                emotion=emotion,
                accent=accent,
                breaths=breaths,
                deadline_ts=deadline_ts,
                **kwargs
            )
            
            if result.get("success") and result.get("audio_data"):
                audio_bytes = base64.b64decode(result["audio_data"])
                meta = {
                    "mime": result.get("mime", "audio/ogg"),
                    "path": "single",
                    "is_voice": True,
                    "is_mp3": False,
                }
                return audio_bytes, meta, None
        except Exception as e:
            logger.error(f"Single-shot TTS failed: {e}")
        
        # MP3 fallback
        try:
            mp3_bytes = await self._http_mp3_fallback(text, start_ts)
            if mp3_bytes:
                meta = {"mime": "audio/mpeg", "path": "http_mp3", "is_mp3": True, "is_voice": False}
                return None, {}, mp3_bytes
        except Exception as e:
            logger.warning(f"MP3 fallback failed: {e}")
        
        return None, {}, None
    
    async def _http_mp3_fallback(self, text: str, start_ts: float) -> Optional[bytes]:
        """HTTP MP3 fallback."""
        try:
            def _http_mp3():
                url = f"https://api.elevenlabs.io/v1/text-to-speech/{self.eleven_voice_id}"
                payload = {
                    "text": text,
                    "model_id": self.eleven_model_id,
                    "voice_settings": {
                        "stability": float(os.getenv("ELEVEN_VOICE_STABILITY", "0.5")),
                        "similarity_boost": float(os.getenv("ELEVEN_VOICE_SIMILARITY", "0.75")),
                        "style": float(os.getenv("ELEVEN_VOICE_STYLE", "0.0")),
                        "use_speaker_boost": True,
                    },
                    "output_format": "mp3_22050_32",
                }
                headers = {
                    "xi-api-key": self.eleven_api_key,
                    "Accept": "audio/mpeg",
                    "Content-Type": "application/json",
                }
                remaining = max(5, int(self.tts_global_deadline_sec - (time.perf_counter() - start_ts) - 2))
                r = self.session.post(url, headers=headers, data=json.dumps(payload), timeout=remaining)
                r.raise_for_status()
                return r.content
            
            mp3_bytes = await asyncio.to_thread(_http_mp3)
            if mp3_bytes:
                return mp3_bytes
        except Exception as e:
            logger.warning(f"HTTP MP3 fallback failed: {e}")
        return None