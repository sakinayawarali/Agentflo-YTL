# agents/tests/twilio_typing_indicator_test.py

import sys
from pathlib import Path

# --- ensure project root on sys.path ---
ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from flask import Flask

from agents.helpers import adk_helper
from agents.helpers.adk_helper import ADKHelper
from agents.helpers.route_handlers import RouteHandler


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload


def test_send_typing_indicator_calls_twilio(monkeypatch):
    helper = ADKHelper.__new__(ADKHelper)
    helper.twilio_account_sid = "AC123"
    helper.twilio_auth_token = "token"

    captured = {}

    def fake_post(url, data=None, auth=None, timeout=None):
        captured["url"] = url
        captured["data"] = data
        captured["auth"] = auth
        captured["timeout"] = timeout
        return FakeResponse(status_code=200, payload={"success": True}, text="ok")

    monkeypatch.setattr(adk_helper.requests, "post", fake_post)

    ok = helper.send_typing_indicator("SM123")

    assert ok is True
    assert captured["url"] == adk_helper.TWILIO_TYPING_URL
    assert captured["data"] == {"messageId": "SM123", "channel": "whatsapp"}
    assert captured["auth"] == ("AC123", "token")


def test_send_typing_indicator_retries_on_mdr_400(monkeypatch):
    helper = ADKHelper.__new__(ADKHelper)
    helper.twilio_account_sid = "AC123"
    helper.twilio_auth_token = "token"

    calls = {"count": 0}

    def fake_post(url, data=None, auth=None, timeout=None):
        calls["count"] += 1
        if calls["count"] == 1:
            return FakeResponse(
                status_code=400,
                payload={"message": "invalid or incomplete inbound data in MDR"},
                text="invalid or incomplete inbound data in MDR",
            )
        return FakeResponse(status_code=200, payload={"success": True}, text="ok")

    monkeypatch.setattr(adk_helper.requests, "post", fake_post)
    monkeypatch.setattr(adk_helper.time, "sleep", lambda _s: None)

    ok = helper.send_typing_indicator("SM123")

    assert ok is True
    assert calls["count"] == 2


class DummyInboundStore:
    def claim_message(self, user_id, inbound_key):
        return True

    def mark_stale(self, user_id, inbound_key):
        return None


class DummyMessageBuffer:
    def append_message(self, user_id, inbound_key, text, reply_to, now, window_sec):
        return now + window_sec, 1, None


class DummyAdkHelper:
    def __init__(self):
        self.typing_calls = []

    def send_typing_indicator(self, message_sid):
        self.typing_calls.append(message_sid)
        return True

    def is_goodbye_message(self, text):
        return False

    def _is_trivial_greeting(self, text):
        return False


def _make_twilio_handler():
    handler = RouteHandler.__new__(RouteHandler)
    handler.verify_token = "token"
    handler.transport = "twilio"
    handler.is_twilio = True
    handler.twilio_account_sid = "AC123"
    handler.twilio_auth_token = "token"
    handler.twilio_from_number = "whatsapp:+15551234567"
    handler.twilio_profile_fallback = "Unknown"
    handler.adk_helper = DummyAdkHelper()
    handler.inbound_store = DummyInboundStore()
    handler.message_buffer = DummyMessageBuffer()
    handler._emitted_billing_ids = set()
    handler._enqueue_buffer_drain_task = lambda user_id, flush_at, generation: None
    return handler


def test_handle_webhook_post_calls_typing_indicator_for_whatsapp():
    app = Flask(__name__)
    handler = _make_twilio_handler()
    form = {
        "From": "whatsapp:+1234567890",
        "To": "whatsapp:+15551234567",
        "MessageSid": "SM123",
        "Body": "hello",
    }

    with app.test_request_context("/webhook", method="POST", data=form):
        handler.handle_webhook_post()

    assert handler.adk_helper.typing_calls == ["SM123"]


def test_handle_webhook_post_skips_typing_indicator_for_sms():
    app = Flask(__name__)
    handler = _make_twilio_handler()
    form = {
        "From": "+1234567890",
        "To": "+15551234567",
        "MessageSid": "SM999",
        "Body": "hello",
    }

    with app.test_request_context("/webhook", method="POST", data=form):
        handler.handle_webhook_post()

    assert handler.adk_helper.typing_calls == []


def test_handle_webhook_post_skips_typing_indicator_without_message_sid():
    app = Flask(__name__)
    handler = _make_twilio_handler()
    form = {
        "From": "whatsapp:+1234567890",
        "To": "whatsapp:+15551234567",
        "Body": "hello",
    }

    with app.test_request_context("/webhook", method="POST", data=form):
        handler.handle_webhook_post()

    assert handler.adk_helper.typing_calls == []
