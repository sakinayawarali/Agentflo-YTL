import os
import signal
import sys
import asyncio
import time
import datetime
from types import FrameType
from dotenv import load_dotenv
from flask import Flask, request, make_response, jsonify
from agents.helpers import route_handlers
from agents.helpers.route_handlers import RouteHandler
from agents.helpers.adk_helper import ADKHelper
from utils.logging import logger


app = Flask(__name__)
load_dotenv(override=True)


VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "sakina123")
TASKS_SHARED_SECRET = os.getenv("TASKS_SHARED_SECRET")
BILLING_LOG_ENDPOINT = os.getenv("BILLING_LOG_ENDPOINT")
TENANT_ID = os.getenv("TENANT_ID")
TENANT_NAME = os.getenv("TENANT_NAME")
AGENT_ID = os.getenv("AGENT_ID")
AGENT_NAME = os.getenv("AGENT_NAME", "Ayesha")

# Global helper instance for tasks
_drain_helper = None

def _get_drain_helper():
    """Get or create the ADK helper singleton."""
    global _drain_helper
    if _drain_helper is None:
        _drain_helper = ADKHelper()
    return _drain_helper


@app.route("/")
def hello() -> str:
    return "Hello, World!"


@app.route("/query", methods=["POST"])
def handle_query():
    data = request.get_json() or {}
    query = data.get("query", "")
    return {"message": f"Your query was: {query}"}


@app.route("/webhook", methods=["GET", "POST"], strict_slashes=False)
def handle_webhook():
    route_handler = RouteHandler(VERIFY_TOKEN)
    
    if request.method == "GET":
        if route_handler.is_twilio:
            # Twilio webhook POST handling
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                resp = loop.run_until_complete(route_handler.handle_webhook_post())
            finally:
                loop.close()
        else:
            # Regular GET verification
            resp = route_handler.handle_webhook_get()
            
    elif request.method == "POST":
        # Log the raw payload
        payload = request.get_json(silent=True) or {}
        logger.info("webhook.payload", payload=payload)
        
        # Run async handler in event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            resp = loop.run_until_complete(route_handler.handle_webhook_post())
        finally:
            loop.close()
    else:
        resp = make_response("Method not allowed", 405)
        
    return resp if resp is not None else make_response("", 200)

# import threading  # Add this to your imports at the top

# @app.route("/webhook", methods=["GET", "POST"], strict_slashes=False)
# def webhook():
#     route_handler = RouteHandler(VERIFY_TOKEN)
    
#     if request.method == "GET":
#         # GET verification stays the same (it's fast)
#         return route_handler.handle_webhook_get()
            
#     elif request.method == "POST":
#         # 1. Log the payload
#         payload = request.get_json(silent=True) or {}
#         logger.info("webhook.payload", payload=payload)
        
#         # 2. Start the AI processing in a SEPARATE thread
#         def run_async_handler(handler):
#             loop = asyncio.new_event_loop()
#             asyncio.set_event_loop(loop)
#             try:
#                 loop.run_until_complete(handler.handle_webhook_post())
#             finally:
#                 loop.close()

#         thread = threading.Thread(target=run_async_handler, args=(route_handler,))
#         thread.start()
        
#         # 3. IMMEDIATELY return 200 OK to Meta
#         # This tells Meta "Got it!" and turns that tick blue/double-gray.
#         return make_response("EVENT_RECEIVED", 200)

#     return make_response("Method not allowed", 405)

@app.route("/twilio/status", methods=["GET", "POST"], strict_slashes=False)
def twilio_status():
    route_handler = RouteHandler(VERIFY_TOKEN)
    return route_handler.handle_twilio_status_post()


@app.route("/tasks/tts-send", methods=["POST"])
def tasks_tts_send():
    # Auth check
    if TASKS_SHARED_SECRET:
        if request.headers.get("X-Tasks-Token", "") != TASKS_SHARED_SECRET:
            return make_response("unauthorized", 401)

    data = request.get_json(silent=True) or {}
    user_id = data.get("user_id")
    text = data.get("text", "")
    job_id = data.get("job_id", "")

    if not user_id or not text:
        return make_response("bad request", 400)

    helper = _get_drain_helper()

    if job_id and helper.vn_job_already_sent(job_id, user_id):
        return make_response(jsonify({"status": "already-sent"}), 200)

    # Run async properly
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(helper._gen_and_send_vn(user_id, text))
        loop.close()
    except Exception as e:
        logger.error("tts_send.error", user_id=user_id, error=str(e), exc_info=True)
        return make_response(jsonify({"status": "error"}), 500)

    if job_id:
        helper.mark_vn_job_sent(job_id, user_id)

    return make_response(jsonify({"status": "ok"}), 200)

# @app.route('/tasks/drain-buffer', methods=['POST'])
# def drain_buffer():
#     handler = RouteHandler(VERIFY_TOKEN) 
#     return handler.handle_drain_buffer()

def shutdown_handler(signal_int: int, frame: FrameType | None) -> object:
    logger.info("Caught Signal Terminated" if signal_int in (signal.SIGTERM, ) else f"Caught Signal {signal_int}")
    from utils.logging import flush
    flush()
    sys.exit(0)


if __name__ == "__main__":
    signal.signal(signal.SIGINT, shutdown_handler)   # local
    app.run(host="localhost", port=8080, debug=True, use_reloader=False)
    logger.info("Application started")
else:
    signal.signal(signal.SIGTERM, shutdown_handler)  # Cloud Run