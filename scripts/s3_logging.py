import json
import uuid
import hashlib
from datetime import datetime, timezone
from utils.logging import logger
import os

# -----------------------------
# Configuration / Constants
# -----------------------------
PRICING = {
    "gemini": {
        "currency": "USD",
        "input_unit": "1K_tokens",
        "output_unit": "1K_tokens",
        "input_price_per_unit": 0.30,
        "output_price_per_unit": 2.50,
    },
    "elevenlabs_tts": {
        "currency": "USD",
        "unit": "minutes",
        "price_per_unit": 0.24,
    },
    "groq_stt": {
        "currency": "USD",
        "unit": "hour",
        "price_per_unit": 0.111,
    }
}

# -----------------------------
# Helper functions
# -----------------------------

def now_rfc3339():
    """Return current time in RFC3339 format."""
    return datetime.now(timezone.utc).isoformat()

def sha256_idempotency_key(*parts):
    """Generate SHA256 hash from key parts."""
    key_input = "|".join(parts)
    return hashlib.sha256(key_input.encode("utf-8")).hexdigest()

def calculate_cost_gemini(input_tokens, output_tokens):
    """Compute token-level costs for Gemini."""
    input_cost = (input_tokens / 1000) * PRICING["gemini"]["input_price_per_unit"]
    output_cost = (output_tokens / 1000) * PRICING["gemini"]["output_price_per_unit"]
    total = input_cost + output_cost
    return {
        "input_cost_usd": round(input_cost, 6),
        "output_cost_usd": round(output_cost, 6),
        "total_cost_usd": round(total, 6)
    }

def calculate_cost_elevenlabs_tts(minutes):
    """Compute cost for ElevenLabs TTS."""
    total = minutes * PRICING["elevenlabs_tts"]["price_per_unit"]
    return {
        "units_billed": minutes,
        "total_cost_usd": round(total, 6)
    }

def calculate_cost_groq_stt(hour):
    """Compute cost for ElevenLabs STT."""
    total = hour * PRICING["groq_stt"]["price_per_unit"]
    return {
        "units_billed": round(hour, 6),
        "total_cost_usd": round(total, 6)
    }

# -----------------------------
# Example Runtime Data
# -----------------------------
tenant_id = "tnt_123"
message_id = "msg_abc"
event_dt = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Example usage numbers
input_tokens = 4000
output_tokens = 250
tts_duration = 4.5
stt_duration = 0.0  # seconds

# -----------------------------
# Event Construction
# -----------------------------
event = {
    "schema_version": "agentflo.billing.v1",
    "event_type": "message_usage",
    "event_id": str(uuid.uuid4()),
    "idempotency_key": sha256_idempotency_key(tenant_id, message_id, event_dt),
    "tenant": {"id": tenant_id, "name": "Acme"},
    "agent": {"id": "agt_42", "name": "Sales Assistant"},
    "conversation": {"id": "conv_abc"},
    "message": {
        "id": message_id,
        "role": "assistant",
        "direction": "outbound",
        "channel": "web",
        "created_at": now_rfc3339(),
        "latency_ms": 720
    },
    "usage": {
        "gemini": {
            "enabled": True,
            "model": "gemini-1.5-pro",
            "request_id": "req_8744",
            "latency_ms": 720,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": input_tokens + output_tokens,
            "pricing": PRICING["gemini"],
            "cost": calculate_cost_gemini(input_tokens, output_tokens)
        },
        "elevenlabs_tts": {
            "enabled": True,
            "model": "eleven_multilingual_v2",
            "voice_id": "emma",
            "request_id": "req_tts_12",
            "latency_ms": 250,
            "input_characters": tts_duration,
            "audio_duration_seconds": 8.0,
            "pricing": PRICING["elevenlabs_tts"],
            "cost": calculate_cost_elevenlabs_tts(tts_duration)
        },
        "groq_stt": {
            "enabled": False,
            "model": "",
            "request_id": "",
            "latency_ms": 0,
            "audio_duration_seconds": stt_duration,
            "pricing": PRICING["groq_stt"],
            "cost": calculate_cost_groq_stt(stt_duration)
        }
    },
}

# -----------------------------
# Cost Summary
# -----------------------------
gemini_cost = event["usage"]["gemini"]["cost"]["total_cost_usd"]
tts_cost = event["usage"]["elevenlabs_tts"]["cost"]["total_cost_usd"]
stt_cost = event["usage"]["groq_stt"]["cost"]["total_cost_usd"]

event["cost_summary"] = {
    "currency": "USD",
    "gemini_cost_usd": gemini_cost,
    "elevenlabs_tts_cost_usd": tts_cost,
    "elevenlabs_stt_cost_usd": stt_cost,
    "total_cost_usd": round(gemini_cost + tts_cost + stt_cost, 6)
}

# -----------------------------
# Additional Metadata
# -----------------------------
event["materials"] = {"text_in_bytes": 1024, "audio_in_bytes": 0, "audio_out_bytes": 25600}
event["privacy"] = {"pii_redaction": True, "store_payloads": False}
event["vendor_payload_refs"] = {"gemini_request_s3": None, "gemini_response_s3": None, "tts_audio_s3": None}
event["ingest"] = {"producer": "agentflo-billing-writer@x.y.z", "ingested_at": now_rfc3339(), "source_ip": "192.168.0.2"}
event["dt"] = event_dt
event["tenant_id"] = tenant_id

# -----------------------------
# Serialize / Emit
# -----------------------------
json_output = json.dumps(event, indent=2)
logger.info(json_output)