"""
Audio processing module for voice notes.

Consolidated from:
- audio_helper.py
- eleven_tool.py
- tts.py (mixin)
- audio_utils.py
- text_utils.py

Clean interface:
- VoiceNoteTranscriber: Transcribe WhatsApp voice notes
- TTSGenerator: Generate TTS audio using ElevenLabs
- VoiceNoteProcessor: Process text for voice notes
- GreetingVNCache: Manage greeting voice note cache
- Audio/text utilities
"""

# Main classes
from agents.audio.transcription import VoiceNoteTranscriber
from agents.audio.generation import TTSGenerator

# Utilities
from agents.audio.utils import (
    # Audio utilities
    estimate_min_audio_bytes,
    is_audio_too_small,
    sniff_bytes_for_ogg,
    sniff_audio_mime,
    trim_trailing_silence,
    # Text utilities
    clean_store_name_for_vn,
    digit_to_urdu,
    int_to_urdu_words,
    number_to_urdu_words,
)

from agents.audio.processing import VoiceNoteProcessor

__all__ = [
    # Main classes
    "VoiceNoteTranscriber",
    "TTSGenerator",
    # Utilities
    "estimate_min_audio_bytes",
    "is_audio_too_small",
    "sniff_bytes_for_ogg",
    "sniff_audio_mime",
    "trim_trailing_silence",
    "clean_store_name_for_vn",
    "digit_to_urdu",
    "int_to_urdu_words",
    "number_to_urdu_words",
]