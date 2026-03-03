import requests
import boto3
import os
import random
import string
import time
import uuid
import re
from datetime import datetime
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv
from utils.logging import logger

load_dotenv()

# Configuration
META_API_VERSION = "v23.0"
WHATSAPP_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME", "agentflo-analytics")
AWS_ACCESS_KEY_ID=os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY=os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION=os.getenv("AWS_REGION")

def _sanitize_wa_number(num: str) -> str:
    if not num:
        return ""
    n = str(num)
    n = n.replace("whatsapp:", "").strip()
    return n.lstrip("+")

def _safe_msg_id(msg_id: str) -> str:
    if not msg_id:
        return "msg"
    return re.sub(r"[^A-Za-z0-9._-]", "_", str(msg_id))

def _safe_path_segment(val: str) -> str:
    if not val:
        return ""
    return re.sub(r"[^A-Za-z0-9._-]", "_", str(val))

def upload_bytes_to_s3(
    payload: bytes,
    *,
    content_type: str,
    user_phone: str = "",
    conversation_id: str = "",
    msg_id: str = "",
    key_prefix: str = "twilio-media",
    expires_sec: int = 3600,
    timestamp: int = 0,
    base_dir: str | None = None,
):
    """
    Upload raw bytes to S3 and return (s3_key, presigned_url).
    Useful for Twilio media uploads where we already have audio bytes.
    """
    if not payload:
        logger.error("s3.upload.skip", reason="missing_payload")
        return None, None
    if not S3_BUCKET_NAME:
        logger.error("s3.upload.skip", reason="missing_bucket")
        return None, None

    ct = (content_type or "").lower()
    is_pdf = "pdf" in ct
    if is_pdf:
        ext = ".pdf"
    elif "png" in ct:
        ext = ".png"
    elif "jpeg" in ct or "jpg" in ct:
        ext = ".jpg"
    elif "heic" in ct:
        ext = ".heic"
    elif "gif" in ct:
        ext = ".gif"
    elif "mpeg" in ct or "mp3" in ct:
        ext = ".mp3"
    elif "wav" in ct:
        ext = ".wav"
    else:
        ext = ".ogg"

    user_phone = _sanitize_wa_number(user_phone)
    safe_msg_id = _safe_msg_id(msg_id)
    ts = int(timestamp or time.time())

    safe_conversation_id = _safe_path_segment(conversation_id)
    if user_phone and safe_conversation_id and safe_msg_id:
        current_date = datetime.now().strftime('%Y-%m-%d')
        base = base_dir or ("twilio_media" if is_pdf else "voice_notes")
        s3_key = f"{base}/{user_phone}/{current_date}/{safe_conversation_id}/{ts}_{safe_msg_id}{ext}"
    else:
        base = base_dir or ("twilio_media" if is_pdf else key_prefix)
        s3_key = f"{base}/{uuid.uuid4().hex}{ext}"

    try:
        s3 = boto3.client("s3")
        s3.put_object(
            Bucket=S3_BUCKET_NAME,
            Key=s3_key,
            Body=payload,
            ContentType=content_type or "application/octet-stream",
        )
        url = s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": S3_BUCKET_NAME, "Key": s3_key},
            ExpiresIn=max(60, int(expires_sec or 3600)),
        )
        logger.info("s3.uploaded.bytes", bucket=S3_BUCKET_NAME, key=s3_key)
        return s3_key, url
    except Exception as e:
        logger.error("s3.upload.error", error=str(e))
        return None, None



def store_voice_note_to_s3(user_phone , media_id , msg_id , conversation_id):
    """
    Downloads voice media from WhatsApp and streams it directly to S3 
    using the specific directory structure requested.
    """
    
    # 1. Get the temporary URL from WhatsApp
    url_endpoint = f"https://graph.facebook.com/{META_API_VERSION}/{media_id}"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}
    
    try:
        # Get Media URL
        response = requests.get(url_endpoint, headers=headers)
        response.raise_for_status()
        media_info = response.json()
        download_url = media_info.get("url")
        mime_type = media_info.get("mime_type", "audio/ogg")
        
        # Determine extension
        extension = ".ogg" if "ogg" in mime_type else ".mp3"
        
        # ---------------------------------------------------------
        # PATH CONSTRUCTION LOGIC
        # ---------------------------------------------------------
        
        # 1. Date: YYYY-MM-DD (e.g., 2025-12-12)
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        
        # 2. Timestamp & MsgId (Dummy)
        timestamp = int(time.time())
        
        
        # Final S3 Key Structure:
        # voice_notes/user_id/2025-12-12/conversation_id/Timestamp_MsgId.ogg
        s3_key = f"voice_notes/{user_phone}/{current_date}/{conversation_id}/{timestamp}_{msg_id}{extension}"
        
        # ---------------------------------------------------------

        # 2. Download the binary content (Stream)
        media_response = requests.get(download_url, headers=headers, stream=True)
        media_response.raise_for_status()
        
        # 3. Upload to S3 (Stream)
        s3 = boto3.client('s3')
        
        logger.info(f"Uploading to path: {s3_key} ...")
        
        s3.upload_fileobj(
            media_response.raw, 
            S3_BUCKET_NAME, 
            s3_key,
            ExtraArgs={'ContentType': mime_type}
        )
        
        logger.info(f" Successfully uploaded to s3://{S3_BUCKET_NAME}/{s3_key}")
        return s3_key

    except Exception as e:
        logger.info(f" Error processing media {media_id}: {str(e)}")
        return None


def store_image_to_s3(user_phone: str, media_id: str, msg_id: str, conversation_id: str):
    """
    Downloads an image from WhatsApp and streams it directly to S3 using the
    same path structure as voice notes but under the `images` prefix.
    """

    url_endpoint = f"https://graph.facebook.com/{META_API_VERSION}/{media_id}"
    headers = {"Authorization": f"Bearer {WHATSAPP_TOKEN}"}

    try:
        response = requests.get(url_endpoint, headers=headers)
        response.raise_for_status()
        media_info = response.json()
        download_url = media_info.get("url")
        mime_type = media_info.get("mime_type", "image/jpeg")

        # Choose an image-friendly extension (default to jpg)
        mime_lower = (mime_type or "").lower()
        if "png" in mime_lower:
            extension = ".png"
        elif "jpeg" in mime_lower or "jpg" in mime_lower:
            extension = ".jpg"
        elif "heic" in mime_lower:
            extension = ".heic"
        elif "gif" in mime_lower:
            extension = ".gif"
        else:
            extension = ".jpg"

        current_date = datetime.now().strftime('%Y-%m-%d')
        timestamp = int(time.time())

        s3_key = f"images/{user_phone}/{current_date}/{conversation_id}/{timestamp}_{msg_id}{extension}"

        media_response = requests.get(download_url, headers=headers, stream=True)
        media_response.raise_for_status()

        s3 = boto3.client('s3')
        logger.info(
            "s3.image.upload.start",
            bucket=S3_BUCKET_NAME,
            key=s3_key,
            media_id=media_id,
            conversation_id=conversation_id,
            user_phone=user_phone,
            mime_type=mime_type,
        )

        s3.upload_fileobj(
            media_response.raw,
            S3_BUCKET_NAME,
            s3_key,
            ExtraArgs={'ContentType': mime_type}
        )

        logger.info(
            "s3.image.upload.success",
            bucket=S3_BUCKET_NAME,
            key=s3_key,
            media_id=media_id,
            conversation_id=conversation_id,
            user_phone=user_phone,
            mime_type=mime_type,
        )
        return s3_key

    except Exception as e:
        logger.warning(
            "s3.image.upload.error",
            bucket=S3_BUCKET_NAME,
            key=locals().get("s3_key"),
            media_id=media_id,
            conversation_id=conversation_id,
            user_phone=user_phone,
            error=str(e),
        )
        return None

# Execute the function for testing
if __name__ == "__main__":
    store_voice_note_to_s3()
