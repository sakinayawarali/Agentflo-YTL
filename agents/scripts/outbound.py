import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

# =========================
# Configuration
# =========================

WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
WHATSAPP_API_URL = f"https://graph.facebook.com/v23.0/{WHATSAPP_PHONE_NUMBER_ID}/messages"
ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")

TEMPLATE_NAME = "en_acquisition_1"

# ✅ Shared template variables (defined ONCE)
DEFAULT_TEMPLATE_VARS = {
    "business": "peek freans",
    "discount_percentage": "10",
    "discount_code": "first10",
    "header_image_link": "https://agentflo-datasets.s3.us-east-1.amazonaws.com/data/products/ebm/images/c947ff34-7c86-474e-b301-6a22d481c877.jpg"
}

# =========================
# Send Single Message
# =========================

def send_whatsapp_template(
    phone_number,
    name,
    business,
    discount_percentage,
    discount_code,
    header_image_link
):
    """
    Send a WhatsApp template message to a single recipient.
    """

    payload = json.dumps({
        "messaging_product": "whatsapp",
        "to": phone_number,
        "type": "template",
        "template": {
            "name": TEMPLATE_NAME,
            "language": {"code": "en"},
            "components": [
                {
                    "type": "header",
                    "parameters": [
                        {
                            "type": "image",
                            "image": {
                                "link": header_image_link
                            }
                        }
                    ]
                },
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "parameter_name": "name", "text": name},
                        {"type": "text", "parameter_name": "business", "text": business},
                        {"type": "text", "parameter_name": "discount_percentage", "text": str(discount_percentage)},
                        {"type": "text", "parameter_name": "discount_code", "text": discount_code}
                    ]
                }
            ]
        }
    })

    headers = {
        "Authorization": f"Bearer {ACCESS_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        response = requests.post(
            WHATSAPP_API_URL,
            headers=headers,
            data=payload,
            timeout=10
        )

        return {
            "phone": phone_number,
            "name": name,
            "status": response.status_code,
            "response": response.json()
        }

    except Exception as e:
        return {
            "phone": phone_number,
            "name": name,
            "status": "error",
            "error": str(e)
        }

# =========================
# Bulk Sender
# =========================

def send_bulk_messages(recipients, delay=1, defaults=None):
    """
    Send WhatsApp template messages to multiple recipients.
    """

    defaults = defaults or DEFAULT_TEMPLATE_VARS
    results = []

    for i, recipient in enumerate(recipients, 1):
        phone = recipient.get("phone")
        name = recipient.get("name")

        if not phone or not name:
            print(f"⚠️  Skipping invalid recipient: {recipient}")
            continue

        print(f"📤 Sending message {i}/{len(recipients)} to {name} ({phone})...")

        result = send_whatsapp_template(
            phone_number=phone,
            name=name,
            business=defaults["business"],
            discount_percentage=defaults["discount_percentage"],
            discount_code=defaults["discount_code"],
            header_image_link=defaults["header_image_link"]
        )

        results.append(result)

        if result["status"] == 200:
            print(f"✅ Success: Message sent to {name}")
        else:
            print(f"❌ Failed: {name}")
            print(result.get("response", result.get("error")))

        if i < len(recipients):
            time.sleep(delay)

    return results

# =========================
# Run
# =========================

if __name__ == "__main__":

    recipients = [
        {"phone": "923312167555", "name": "Sakina"},
        {"phone": "923161620950", "name": "Aniq"},
        {"phone": "923168242299", "name": "Musab"},
        {"phone": "923161278341", "name": "Humayun"},
    ]

    print(f"\n🚀 Starting WhatsApp bulk send to {len(recipients)} recipients...\n")

    results = send_bulk_messages(recipients, delay=1)

    print("\n" + "=" * 50)
    print("SUMMARY")
    print("=" * 50)

    successful = sum(1 for r in results if r["status"] == 200)
    failed = len(results) - successful

    print(f"Total: {len(results)} | Success: {successful} | Failed: {failed}")

    if failed > 0:
        print("\nFailed messages:")
        for r in results:
            if r["status"] != 200:
                print(f"  - {r['name']} ({r['phone']}): {r.get('response', r.get('error'))}")
