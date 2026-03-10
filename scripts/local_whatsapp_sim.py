from __future__ import annotations

import argparse
import os
import pathlib
import sys
import time
from typing import List


QUESTIONS: List[str] = [
    # Product & Specifications
    "What concrete grades do you offer?",
    "What grade do I need for house foundation?",
    "What's the difference between Grade 25 and Grade 30?",
    "Can I get Grade 40 concrete?",
    "Do you have self-compacting concrete?",
    "Can you add waterproofing admixture?",
    "Do you offer fibre reinforced concrete?",
    "What slump do you provide for pump delivery?",
    "Can I get coloured concrete?",
    "Do you supply lightweight concrete?",
    "What's the maximum aggregate size?",
    "Can I get a mix design certificate?",
    "Is your concrete SIRIM certified?",
    # Pricing & Payment
    "What's the price per m³ for Grade 25?",
    "How much for Grade 30 per cubic metre?",
    "Any discount for large orders?",
    "What's the pump truck charge?",
    "Is there a weekend delivery surcharge?",
    "What's the minimum order quantity?",
    "Do you charge for waiting time?",
    "What payment methods do you accept?",
    "Do I need to pay deposit?",
    "When do I pay – before or after delivery?",
    "Is GST included in the price?",
    "What's the cancellation fee?",
    "Do you charge extra for small loads under 5m³?",
    "How much for standby time if pour is delayed?",
    # Ordering Process
    "How do I place an order on WhatsApp?",
    "Can I order without an account?",
    "Do I need a PO number to order?",
    "How far in advance must I book?",
    "What's the cutoff time for same-day delivery?",
    "Can I change my order after confirmation?",
    "Can I cancel my order?",
    "How do I get an official quotation?",
    "Do you require site photos before ordering?",
    "Can I order for tomorrow morning?",
    "How many trucks can I book at once?",
    # Delivery & Logistics
    "What size is your mixer truck?",
    "Will your truck fit in a narrow lane?",
    "How wide does the road need to be for your truck?",
    "What's the chute length for unloading?",
    "Can the chute reach 10 metres?",
    "Do I need a pump truck for first floor?",
    "How many pumps needed for high-rise?",
    "Can two trucks arrive at the same time?",
    "Does the driver help with unloading?",
    "How long does unloading take per m³?",
    "Can the truck reverse into my site?",
    "Do you deliver to Petaling Jaya?",
    "What areas do you cover?",
    "Is there a delivery charge?",
    "Can I get delivery on Sunday?",
    "When will my truck arrive?",
    "Where is my concrete truck now?",
    "Can you check my order ETA?",
    "How long from batching to delivery?",
    "How long before concrete starts setting?",
    "Can I delay the truck if site not ready?",
    "What if truck arrives before we're ready?",
    "Can you call me 10 minutes before arrival?",
    "What's the latest time you deliver?",
    "Can you deliver at 6 AM?",
    "What if it rains during delivery?",
    "How long can concrete stay in the truck?",
    "What's your policy if pour is delayed?",
    # Quality & Compliance
    "Can I get a delivery docket?",
    "Do you provide test cubes?",
    "Can I take samples on site?",
    "What if concrete arrives too dry/wet?",
    "What if wrong grade is delivered?",
    "What if short load is delivered?",
    "Do you have quality guarantee?",
    "What's your policy for rejected concrete?",
    "Can I visit your batching plant?",
    "Are your materials sourced locally?",
    # Site Preparation & Requirements
    "What site preparation do I need?",
    "How to prepare subgrade for slab?",
    "Do I need to wet the surface before pouring?",
    "How many workers needed for 10m³ pour?",
    "What equipment do I need on site?",
    "Can you advise on curing method?",
    "How long before I can walk on new slab?",
    "When to remove formwork?",
    "Do you provide curing compound?",
    # Emergency & Issues
    "My truck is late – where is it?",
    "Site not ready – can you hold the truck?",
    "Concrete setting too fast – what to do?",
    "Wrong grade delivered – what now?",
    "Short load – only got 6m³ for 8m³ order",
    "Truck stuck at site entrance",
    "Driver left without unloading",
    "Need urgent delivery today – possible?",
    "Can I get extra 2m³ on same truck?",
    "Concrete too stiff – can you add water?",
    "Rain started during pour – will it affect quality?",
]


def _print_block(title: str, body: str) -> None:
    bar = "-" * 88
    print(f"\n{bar}\n{title}\n{bar}")
    print(body.rstrip())


def main() -> int:
    parser = argparse.ArgumentParser(description="Local WhatsApp → Agent simulator (prints replies to console).")
    parser.add_argument("--user-id", default="923312167555", help="Simulated WhatsApp user id (digits).")
    parser.add_argument("--name", default="Sakina", help="Customer name to preload (avoids name prompt).")
    parser.add_argument("--sleep-ms", type=int, default=0, help="Optional delay between messages.")
    parser.add_argument("--limit", type=int, default=0, help="Only run first N messages (0 = all).")
    parser.add_argument(
        "--endpoint",
        default="",
        help="If set (e.g., http://localhost:8080/webhook), send WhatsApp Cloud-API shaped payloads to a running server instead of importing ADKHelper directly.",
    )
    args = parser.parse_args()

    # Make sure we behave like YTL demo.
    os.environ.setdefault("TENANT_ID", "ytl")
    os.environ.setdefault("PROMPT_LANGUAGE", "EN")

    msgs = QUESTIONS[: args.limit] if args.limit and args.limit > 0 else QUESTIONS

    if args.endpoint.strip():
        import requests

        endpoint = args.endpoint.strip().rstrip("/")

        def _payload_text(i: int, text: str) -> dict:
            return {
                "object": "whatsapp_business_account",
                "entry": [
                    {
                        "id": "local-sim",
                        "changes": [
                            {
                                "field": "messages",
                                "value": {
                                    "messaging_product": "whatsapp",
                                    "metadata": {"display_phone_number": "000", "phone_number_id": "000"},
                                    "contacts": [{"profile": {"name": args.name}, "wa_id": args.user_id}],
                                    "messages": [
                                        {
                                            "from": args.user_id,
                                            "id": f"wamid.local.{i}",
                                            "timestamp": str(int(time.time())),
                                            "type": "text",
                                            "text": {"body": text},
                                        }
                                    ],
                                },
                            }
                        ],
                    }
                ],
            }

        for idx, q in enumerate(msgs, 1):
            _print_block(f"USER [{idx}/{len(msgs)}]", q)
            try:
                resp = requests.post(f"{endpoint}", json=_payload_text(idx, q), timeout=60)
                _print_block("SERVER", f"HTTP {resp.status_code}\n{(resp.text or '').strip()}")
            except Exception as e:
                _print_block("ERROR", f"{type(e).__name__}: {e}")
            if args.sleep_ms and args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)
    else:
        # In-process mode (requires local credentials for Firestore/Vertex if enabled).
        # Ensure Agentflo-YTL is on sys.path so `import agents...` works when running as a script.
        repo_root = pathlib.Path(__file__).resolve().parents[1]
        if str(repo_root) not in sys.path:
            sys.path.insert(0, str(repo_root))

        from agents.helpers.adk_helper import ADKHelper

        helper = ADKHelper()

        # Monkeypatch outbound WhatsApp sends to print instead of sending.
        helper._send_text_once = lambda to, body, reply_to_message_id=None: (_print_block("AGENT", str(body)), True)[1]

        # Preload name to avoid the "What's your name?" branch.
        try:
            helper.create_user_document(args.user_id)
            helper._persist_customer_metadata(args.user_id, {"customer_name": args.name})
            helper.session_helper.set_onboarding_status(args.user_id, None)
        except Exception:
            pass

        for idx, q in enumerate(msgs, 1):
            _print_block(f"USER [{idx}/{len(msgs)}]", q)
            try:
                helper.handle_message(q, args.user_id, is_voice_input=False, inbound_key=f"local-{idx}", reply_to_message_id=None)
            except Exception as e:
                _print_block("ERROR", f"{type(e).__name__}: {e}")
            if args.sleep_ms and args.sleep_ms > 0:
                time.sleep(args.sleep_ms / 1000.0)

    print("\nDone.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

