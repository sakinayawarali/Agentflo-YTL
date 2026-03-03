import uuid
import urllib
import hashlib
from datetime import datetime , timezone
import os
import json
import threading
import requests
from utils.logging import logger
import boto3
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

load_dotenv()

# Environment variables for tenant and agent identification
TENANT_ID = os.getenv("TENANT_ID", "tenant_ebm")
TENANT_NAME = os.getenv("TENANT_NAME", "EBM")
AGENT_ID = os.getenv("AGENT_ID", "agt_42")
AGENT_NAME = os.getenv("AGENT_NAME", "Ayesha")
BILLING_LOG_ENDPOINT = os.getenv("BILLING_LOG_ENDPOINT", "")
# Firehose configuration (optional; used if provided)
# Support both FIREHOSE_STREAM_NAME and DELIVERY_STREAM_NAME (from the test script)
FIREHOSE_STREAM_NAME = os.getenv("FIREHOSE_STREAM_NAME")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
AWS_ACCESS_KEY_ID = os.getenv("AGENTFLO_ACCESS_KEY")
AWS_SECRET_ACCESS_KEY = os.getenv("AGENTFLO_SECRET_KEY")

def now_rfc3339() -> str:
    return datetime.now(timezone.utc).isoformat()

def sha256_key(*parts: str) -> str:
    return hashlib.sha256("|".join(parts).encode()).hexdigest()

def post_json(url: str, data: dict):
    req = urllib.request.Request(
        url=url,
        data=json.dumps(data).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=5) as resp:
        return resp.read()

def generate_billing_event_v2(
    *,
    tenant_id: str,
    conversation_id: str,
    msg_type: str,
    message_id: str,
    role: str,
    channel: str,
    conversation_text: str,
    gemini_usage: dict | None = None,
    eleven_tts_usage: dict | None = None,
    s3_key: str | None = None,
):
    """Generate unified billing JSON (v2) with line_items for Gemini, ElevenLabs TTS, and WhatsApp templates."""
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    event_id = str(uuid.uuid4())

    line_items = []

    # === Gemini line items ===
    if gemini_usage and gemini_usage.get("enabled"):
        input_tokens = gemini_usage.get("input_tokens", 0)
        output_tokens = gemini_usage.get("output_tokens", 0)
        input_price = round(0.30 / 1000000 , 10)
        output_price = round(2.50 / 1000000 , 10)
        model_name = gemini_usage.get("model", "gemini-2.5-flash")
        req_id = gemini_usage.get("request_id", "")
        latency = gemini_usage.get("latency_ms", 0)

        line_items.append({
            "use_case": "Gemini input",
            "vendor": "google",
            "service": "gemini",
            "resource": model_name,
            "unit": "1M_tokens",
            "qty": input_tokens,
            "unit_price_usd": input_price,
            "cost_usd": round((input_tokens * input_price) , 10),
            "meta": {
                "kind": "input",
                "request_id": req_id,
                "latency_ms": latency,
                "input_tokens": input_tokens,
            },
        })

        line_items.append({
            "use_case": "Gemini output",
            "vendor": "google",
            "service": "gemini",
            "resource": model_name,
            "unit": "1M_tokens",
            "qty": output_tokens ,
            "unit_price_usd": output_price,
            "cost_usd": round((output_tokens * output_price), 10),
            "meta": {
                "kind": "output",
                "request_id": req_id,
                "latency_ms": latency,
                "output_tokens": output_tokens,
            },
        })

    # === ElevenLabs TTS line item ===
    if eleven_tts_usage and eleven_tts_usage.get("enabled"):
        model_name = eleven_tts_usage.get("model", "")
        req_id = eleven_tts_usage.get("request_id", "")
        latency = eleven_tts_usage.get("latency_ms", 0)
        voice_id = eleven_tts_usage.get("voice_id", "Ayesha")

        # Support character-based pricing (preferred) and keep legacy duration-based as fallback
        if "input_characters" in eleven_tts_usage:
            chars = int(eleven_tts_usage.get("input_characters", 0) or 0)
            unit_price = float(eleven_tts_usage.get("pricing", {}).get("price_per_characters_usd", 0.0001))
            qty_chars = chars 
            cost = qty_chars * unit_price

            line_items.append({
                "use_case": "ElevenLabs TTS",
                "vendor": "elevenlabs",
                "service": "tts",
                "resource": model_name,
                "unit": "11M_characters",
                "qty": round(qty_chars, 3),
                "unit_price_usd": unit_price,
                "cost_usd": round(cost, 6),
                "meta": {
                    "voice_id": voice_id,
                    "request_id": req_id,
                    "latency_ms": latency,
                    "input_characters": chars,
                },
            })
    
        
    total_cost = sum(li["cost_usd"] for li in line_items)

    event = {
        "v": "1",
        "event_id": event_id,
        "idempotency_key": hashlib.sha256(f"{tenant_id}:{message_id}".encode()).hexdigest(),
        "tenant_id": tenant_id,
        "conversation_id": conversation_id,
        "msg_type": msg_type,
        "message_id": message_id,
        "role": role,
        "channel": channel,
        "conversation_text": conversation_text,
        "ts": ts,
        "line_items": line_items,
        "total_cost_usd": round(total_cost, 8),
        "currency": "USD",
    }
    if msg_type in {"audio", "image"} and s3_key:
        event["s3_key"] = s3_key

    return event


def send_billing_event_fire_and_forget(event: dict):
    """Send the unified billing event asynchronously."""
    logger.info(json.dumps(event,indent=2))
    
    def _worker():
        try:
            # Prefer Firehose if configured; otherwise use HTTP endpoint; otherwise log only
            if FIREHOSE_STREAM_NAME:
                try:
                    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
                        logger.warning("billing.firehose_missing_credentials")
                        return
                    firehose = boto3.client(
                        "firehose",
                        region_name=AWS_REGION,
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                    )
                except Exception as exc:
                    logger.warning("billing.firehose_client_error", error=str(exc))
                    return

                # Build JSON Lines single record, as in send_firehose_events.py
                line = json.dumps(event, separators=(",", ":")) + "\n"
                data_bytes = line.encode("utf-8")
                try:
                    resp = firehose.put_record(
                        DeliveryStreamName=FIREHOSE_STREAM_NAME,
                        Record={"Data": data_bytes},
                    )
                    logger.info("billing.firehose_put_record_ok", status="ok", response=resp)
                except (BotoCoreError, ClientError) as exc:
                    logger.warning("billing.firehose_put_record_failed", error=str(exc))
                return

            if BILLING_LOG_ENDPOINT:
                resp = requests.post(BILLING_LOG_ENDPOINT, json=event, timeout=5)
                if resp.status_code in (200, 201, 202):
                    logger.info("billing.event_sent", status=resp.status_code, total_cost_usd=event.get("total_cost_usd"))
                else:
                    logger.warning("billing.post_failed", status=resp.status_code, response_preview=(resp.text or "")[:300])
                return

            logger.warning("billing.no_destination_configured")
        except Exception as e:
            logger.warning("billing.send_exception", error=str(e))

    threading.Thread(target=_worker, daemon=True).start()
