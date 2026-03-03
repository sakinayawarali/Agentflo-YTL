"""
Audio and text utilities for voice note processing.

Consolidates:
- audio_utils.py → Audio processing utilities
- text_utils.py → Text conversion utilities (Urdu numbers, currency)
"""

import os
import re
import shutil
import subprocess
import tempfile
from typing import Optional

from utils.logging import logger


# ============================================================================
# AUDIO UTILITIES
# ============================================================================

def estimate_min_audio_bytes(
    text_len: int,
    *,
    kbps: int = 24,
    chars_per_sec: float = 18.0,
    slack: float = 0.50
) -> int:
    """
    Estimate minimum audio file size based on text length.
    
    Args:
        text_len: Length of text in characters
        kbps: Bitrate in kilobits per second
        chars_per_sec: Average speech rate (chars/second)
        slack: Slack tolerance (0.5 = 50% below estimate is acceptable)
    
    Returns:
        Minimum expected bytes
    """
    if text_len <= 0:
        return 0
    duration = max(0.8, text_len / max(10.0, chars_per_sec))
    bytes_at_bitrate = duration * (kbps * 1000 / 8.0)
    return int(max(2400.0, bytes_at_bitrate * (1.0 - slack)))


def is_audio_too_small(
    text_len: int,
    audio_bytes: Optional[bytes],
    *,
    kbps: int = 24
) -> bool:
    """
    Check if audio file is suspiciously small for the given text length.
    
    Args:
        text_len: Length of text in characters
        audio_bytes: Audio file bytes
        kbps: Bitrate to use for estimate
    
    Returns:
        True if audio is too small to be valid
    """
    if not audio_bytes:
        return True
    floor = estimate_min_audio_bytes(
        text_len,
        kbps=kbps,
        chars_per_sec=float(os.getenv("SPEECH_RATE_CHARS_PER_SEC", "13")),
        slack=float(os.getenv("AUDIO_SIZE_TOLERANCE", "0.35")),
    )
    return len(audio_bytes) < floor


def sniff_bytes_for_ogg(b: bytes) -> bool:
    """Check if bytes contain OGG Opus markers."""
    if not b:
        return False
    head = b[:512]
    if b"OggS" in head:
        return True
    if b"OpusHead" in head:
        return True
    return False


def sniff_audio_mime(b: bytes) -> str:
    """
    Detect audio MIME type from byte content.
    
    Returns:
        MIME type string ("audio/ogg", "audio/mpeg", or "application/octet-stream")
    """
    if not b or len(b) < 4:
        return "application/octet-stream"
    
    # Check for OggS anywhere in first 512 bytes
    if sniff_bytes_for_ogg(b):
        return "audio/ogg"
    
    # MP3 header common markers
    if b[:3] == b"ID3" or b[:2] in (b"\xff\xfb", b"\xff\xf3", b"\xff\xf2"):
        return "audio/mpeg"
    
    return "application/octet-stream"


def trim_trailing_silence(
    audio_bytes: bytes,
    mime: Optional[str] = None,
    *,
    max_silence_sec: float = 1.0,
    threshold_db: float = -40.0,
) -> bytes:
    """
    Trim trailing silence from audio to avoid long empty tails in voice notes.
    
    Modes (via env VN_TRIM_MODE):
      - "none": no trimming, return original bytes
      - "safe": gentle trim with high-quality re-encode (default)
    
    For ElevenLabs Opus we use high-quality Opus encode (48 kHz / 48kbit / application=audio),
    so voice quality stays close to the original.
    
    Args:
        audio_bytes: Audio file bytes
        mime: MIME type hint
        max_silence_sec: Max silence duration to remove (seconds)
        threshold_db: Silence threshold in dB
    
    Returns:
        Trimmed audio bytes (or original if trimming fails/disabled)
    """
    if not audio_bytes:
        return audio_bytes

    mode = os.getenv("VN_TRIM_MODE", "safe").lower()
    if mode == "none":
        return audio_bytes

    ffmpeg = shutil.which("ffmpeg") or shutil.which("ffmpeg.exe")
    if not ffmpeg:
        return audio_bytes

    # Decide extension based on mime
    ext = ".ogg"
    if mime:
        m = mime.lower()
        if "mpeg" in m or "mp3" in m:
            ext = ".mp3"
        elif "wav" in m:
            ext = ".wav"
        elif "ogg" in m or "opus" in m:
            ext = ".ogg"

    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=ext) as src:
            src.write(audio_bytes)
            src.flush()
            src_path = src.name

        out_path = src_path + ".trimmed" + ext

        # silenceremove: remove silence only at end
        filter_arg = (
            f"silenceremove=start_periods=0:"
            f"stop_periods=1:"
            f"stop_duration={max_silence_sec}:"
            f"stop_threshold={threshold_db}dB"
        )

        # SAFE re-encode settings, tuned for voice
        if ext == ".ogg":
            codec_args = [
                "-c:a", "libopus",
                "-ar", "48000",
                "-ac", "1",
                "-b:a", "48k",
                "-application", "audio",
            ]
        elif ext == ".mp3":
            codec_args = [
                "-c:a", "libmp3lame",
                "-b:a", "48k",
                "-ac", "1",
            ]
        elif ext == ".wav":
            codec_args = ["-c:a", "pcm_s16le"]
        else:
            codec_args = [
                "-c:a", "libopus",
                "-ar", "48000",
                "-ac", "1",
                "-b:a", "48k",
                "-application", "audio",
            ]

        cmd = [
            ffmpeg,
            "-y",
            "-hide_banner",
            "-loglevel", "error",
            "-i", src_path,
            "-af", filter_arg,
            *codec_args,
            out_path,
        ]

        timeout_sec = float(os.getenv("VN_TRIM_TIMEOUT_SEC", "1000"))
        subprocess.run(cmd, check=True, timeout=timeout_sec)

        with open(out_path, "rb") as f:
            trimmed = f.read()

        # Only accept if it actually became shorter and still non-trivial
        if trimmed and len(trimmed) < len(audio_bytes):
            return trimmed
        return audio_bytes
        
    except Exception as e:
        logger.warning(f"trim_trailing_silence.failed: {e}")
        return audio_bytes
        
    finally:
        for p in (locals().get("src_path"), locals().get("out_path")):
            if p:
                try:
                    os.unlink(p)
                except Exception:
                    pass


# ============================================================================
# TEXT UTILITIES (URDU / ROMAN URDU)
# ============================================================================

def clean_store_name_for_vn(text: str) -> str:
    """
    Remove store names in parentheses for voice notes.
    Example: "Salam Rohail bhai (Apple Pharmacy)" → "Salam Rohail bhai"
    
    Args:
        text: Input text
    
    Returns:
        Text with store names removed
    """
    if not text:
        return text
    # Remove parentheses with store names but keep other parenthetical content
    text = re.sub(
        r'\s*\([^)]*(?:Store|Pharmacy|Shop|Traders|Mart)\)',
        '',
        text,
        flags=re.IGNORECASE
    )
    return text


def digit_to_urdu(d: str) -> str:
    """Convert single digit to Urdu word (Roman Urdu)."""
    digits = {
        '0': 'zero', '1': 'ek', '2': 'do', '3': 'teen', '4': 'char',
        '5': 'panch', '6': 'chhe', '7': 'saat', '8': 'aath', '9': 'nau'
    }
    return digits.get(d, d)


def int_to_urdu_words(num: int) -> str:
    """
    Convert integer to Roman Urdu number words.
    
    - Explicit mapping for 0–99 (to avoid 'tees teen' style mistakes)
    - Supports 100+ using lakh / hazaar / sau composition
    
    Args:
        num: Integer to convert
    
    Returns:
        Roman Urdu words (e.g., 33 → "taintees", 2145 → "do hazaar ek sau pentaalees")
    """
    # Spellings tuned for ElevenLabs English phonetic engine:
    # - Hyphens force syllable breaks so compound sounds aren't slurred
    # - Vowels written to match how ElevenLabs reads English letters
    #   e.g. "aa" → long-a, "ay" → long-a at end, "uh" → schwa
    NUM_0_99 = {
        0:  "zero",          1:  "ek",            2:  "do",            3:  "teen",          4:  "char",
        5:  "paanch",        6:  "che",            7:  "saat",          8:  "aath",           9:  "nau",
        10: "das",           11: "gyaa-rah",       12: "baa-rah",       13: "tay-rah",        14: "chau-dah",
        15: "pan-drah",      16: "so-lah",         17: "sat-rah",       18: "ath-rah",        19: "un-nees",
        20: "bees",          21: "ik-kees",        22: "ba-ees",        23: "tay-ees",        24: "chau-bees",
        25: "pa-chees",      26: "chab-bees",      27: "satta-ees",     28: "attha-ees",      29: "un-tees",
        30: "tees",          31: "ik-tees",        32: "bat-tees",      33: "tain-tees",      34: "chaun-tees",
        35: "pain-tees",     36: "chhat-tees",     37: "sain-tees",     38: "ar-tees",        39: "un-taa-lees",
        40: "cha-lees",      41: "ik-taa-lees",    42: "byaa-lees",     43: "tain-taa-lees",  44: "chaw-a-lees",
        45: "pin-taa-lees",  46: "chhiyaa-lees",   47: "sain-taa-lees", 48: "ar-taa-lees",    49: "un-chaas",
        50: "pa-chaas",      51: "ik-awan",        52: "baa-wan",       53: "tir-pan",        54: "chaa-wan",
        55: "pach-pan",      56: "chhap-pan",      57: "satta-wan",     58: "atha-wan",       59: "un-saath",
        60: "saath",         61: "ik-saath",       62: "ba-saath",      63: "tir-saath",      64: "chau-saath",
        65: "pain-saath",    66: "chhiyaa-saath",  67: "sad-saath",     68: "atha-saath",     69: "un-hat-tar",
        70: "sat-tar",       71: "ik-hat-tar",     72: "ba-hat-tar",    73: "ti-hat-tar",     74: "chau-hat-tar",
        75: "pach-hat-tar",  76: "chi-hat-tar",    77: "sat-tat-tar",   78: "atha-hat-tar",   79: "u-naasi",
        80: "assi",          81: "ik-yaasi",       82: "bi-yaasi",      83: "ti-raasi",       84: "chu-raasi",
        85: "pa-chaasi",     86: "chi-yaasi",      87: "sat-taasi",     88: "ath-aasi",       89: "nav-vay",
        90: "nab-bay",       91: "ik-yan-way",     92: "baan-way",      93: "tir-yan-way",    94: "chour-an-way",
        95: "pach-yan-way",  96: "chhiy-an-way",   97: "sattay-an-way", 98: "athay-an-way",   99: "nin-yan-way",
    }

    # Handle zero + negative
    if num == 0:
        return NUM_0_99[0]
    if num < 0:
        return "minus " + int_to_urdu_words(-num)

    # Direct lookup for 0–99
    if num <= 99:
        return NUM_0_99.get(num, str(num))

    # -----------------------------------------------------------------------
    # COLLOQUIAL "X so Y" FORMAT for 1000–9900
    # Pakistani speech says "pandrah so pachaas" (15 hundred 50) not
    # "ek hazaar paanch sau pachaas". We replicate that here so ElevenLabs
    # hears natural price-reading Urdu rather than formal/textbook forms.
    # Only applies when the number divides cleanly into hundreds (no extra
    # thousands left over), i.e. 1000–9900 range.
    # -----------------------------------------------------------------------
    if 1000 <= num <= 9999:
        hundreds_total = num // 100   # e.g. 1550 → 15
        remainder      = num % 100    # e.g. 1550 → 50
        # Only use "X so" form when hundreds_total fits a clean lookup (1–99)
        if hundreds_total <= 99:
            h_word = NUM_0_99[hundreds_total]
            if remainder == 0:
                return f"{h_word} so"
            else:
                return f"{h_word} so {NUM_0_99[remainder]}"

    parts = []

    # Lakhs
    if num >= 100000:
        lakhs = num // 100000
        parts.append(int_to_urdu_words(lakhs))
        parts.append("lakh")
        num %= 100000

    # Thousands
    if num >= 1000:
        thousands = num // 1000
        parts.append(int_to_urdu_words(thousands))
        parts.append("hazaar")
        num %= 1000

    # Hundreds
    if num >= 100:
        hundreds = num // 100
        parts.append(int_to_urdu_words(hundreds))
        parts.append("sau")
        num %= 100

    # Remaining 0–99
    if num > 0:
        parts.append(int_to_urdu_words(num))

    return " ".join(parts).strip()


def number_to_urdu_words(num_str: str) -> str:
    try:
        if '.' in num_str:
            rounded = int(round(float(num_str)))  # round, never say decimals
            return int_to_urdu_words(rounded)
        return int_to_urdu_words(int(num_str))
    except Exception:
        return num_str