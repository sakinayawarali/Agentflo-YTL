# agents/helpers/flow_endpoints.py
"""
WhatsApp Flow Data Exchange Endpoint
Handles all communication between WhatsApp Flows and your backend
"""
import json
import hmac
import hashlib
import base64
import os
from typing import Any, Dict, List, Optional

from flask import request, jsonify, make_response
from dotenv import load_dotenv

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from utils.logging import logger
from agents.tools.order_draft_tools import (
    get_cart,
    OrderDraft,
    _ensure_total_amount_field,
)
from agents.tools.catalog_search import find_first_retailer_id
from agents.tools.cart_tools import agentflo_cart_tool

load_dotenv()

# -------------------------------------------------------------------
# Flow security (HMAC signature – existing, for X-Hub-Signature-256)
# -------------------------------------------------------------------
FLOW_PRIVATE_KEY = os.getenv("WA_FLOW_PRIVATE_KEY", "")
FLOW_PUBLIC_KEY_ID = os.getenv("WA_FLOW_PUBLIC_KEY_ID", "")

# -------------------------------------------------------------------
# RSA private key for WhatsApp Flows encryption
# PEM string is stored in WA_FLOW_RSA_PRIVATE_KEY_PEM (via Secret Manager)
# Passphrase is stored in WA_FLOW_RSA_PRIVATE_KEY_PASSPHRASE
# -------------------------------------------------------------------
FLOW_RSA_PRIVATE_KEY_PEM = os.getenv("WA_FLOW_RSA_PRIVATE_KEY_PEM", "")
FLOW_RSA_PRIVATE_KEY_PASSPHRASE = os.getenv("WA_FLOW_RSA_PRIVATE_KEY_PASSPHRASE", "")

_rsa_private_key_obj = None  # cached


def get_flow_rsa_private_key():
    """
    Load and cache the RSA private key used for WhatsApp Flows encryption
    (decrypting encrypted_aes_key).
    """
    global _rsa_private_key_obj

    if _rsa_private_key_obj is not None:
        return _rsa_private_key_obj

    pem_str = FLOW_RSA_PRIVATE_KEY_PEM or ""
    if not pem_str:
        logger.error("flow.rsa_key_env_missing")
        raise RuntimeError("WA_FLOW_RSA_PRIVATE_KEY_PEM is not set in environment")

    # DEBUG: don't log full key, just a bit of metadata
    logger.info(
        "flow.rsa_key_env_info",
        length=len(pem_str),
        starts_with=pem_str[:40].replace("\n", "\\n"),
    )

    # If it came as a single line with literal '\n', fix it
    if "\\n" in pem_str and "-----BEGIN" in pem_str:
        pem_str = pem_str.replace("\\n", "\n")

    # If it accidentally has quotes around it, strip them
    if pem_str.startswith('"') and pem_str.endswith('"'):
        pem_str = pem_str[1:-1]

    # At this point we expect BEGIN PRIVATE KEY, not PUBLIC
    if "BEGIN PUBLIC KEY" in pem_str:
        logger.error("flow.rsa_key_is_public_key")
        raise RuntimeError("WA_FLOW_RSA_PRIVATE_KEY_PEM contains a PUBLIC key, not a PRIVATE key")

    key_bytes = pem_str.encode("utf-8")

    password_bytes = (
        FLOW_RSA_PRIVATE_KEY_PASSPHRASE.encode("utf-8")
        if FLOW_RSA_PRIVATE_KEY_PASSPHRASE
        else None
    )

    logger.info(
        "flow.rsa_passphrase_info",
        has_passphrase=bool(password_bytes),
        passphrase_len=len(password_bytes) if password_bytes else 0,
    )

    try:
        _rsa_private_key_obj = serialization.load_pem_private_key(
            key_bytes,
            password=password_bytes,
            backend=default_backend(),
        )
    except Exception as e:
        logger.error("flow.rsa_key_load_failed", error=str(e))
        # Re-raise so the caller still sees "Decryption failed: ..."
        raise

    return _rsa_private_key_obj

# -------------------------------------------------------------------
# Crypto helpers for WhatsApp Flows
# -------------------------------------------------------------------
def _decrypt_flow_request(encrypted_body: Dict[str, Any]) -> tuple[Dict[str, Any], bytes, bytes]:
    """
    Decrypt WhatsApp Flow payload.

    encrypted_body contains:
    - encrypted_flow_data: base64(AES-GCM(ciphertext || tag))
    - encrypted_aes_key:   base64(RSA-OAEP(AES_key))
    - initial_vector:      base64(iv)

    Returns:
    - decrypted JSON (dict)
    - aes_key (bytes)
    - iv (bytes)
    """
    try:
        encrypted_flow_data_b64 = encrypted_body["encrypted_flow_data"]
        encrypted_aes_key_b64 = encrypted_body["encrypted_aes_key"]
        initial_vector_b64 = encrypted_body["initial_vector"]
    except KeyError as e:
        raise ValueError(f"Missing encryption field: {e}")

    # Decode from base64
    encrypted_flow_data = base64.b64decode(encrypted_flow_data_b64)
    encrypted_aes_key = base64.b64decode(encrypted_aes_key_b64)
    iv = base64.b64decode(initial_vector_b64)

    # Decrypt AES key using RSA OAEP (SHA256)
    private_key = get_flow_rsa_private_key()
    aes_key = private_key.decrypt(
        encrypted_aes_key,
        padding.OAEP(
            mgf=padding.MGF1(algorithm=hashes.SHA256()),
            algorithm=hashes.SHA256(),
            label=None,
        ),
    )

    # Split ciphertext and tag (last 16 bytes)
    ciphertext = encrypted_flow_data[:-16]
    tag = encrypted_flow_data[-16:]

    decryptor = Cipher(
        algorithms.AES(aes_key),
        modes.GCM(iv, tag),
    ).decryptor()

    plaintext_bytes = decryptor.update(ciphertext) + decryptor.finalize()
    decrypted_json = json.loads(plaintext_bytes.decode("utf-8"))

    return decrypted_json, aes_key, iv


def _flip_iv(iv: bytes) -> bytes:
    """Flip IV bytes by XOR with 0xFF (WhatsApp spec for response IV)."""
    return bytes(b ^ 0xFF for b in iv)


def _encrypt_flow_response(response_obj: Dict[str, Any], aes_key: bytes, iv: bytes) -> str:
    """
    Encrypt the response JSON using AES-GCM with flipped IV,
    then return base64(ciphertext || tag) string.
    """
    flipped_iv = _flip_iv(iv)
    plaintext = json.dumps(response_obj, separators=(",", ":")).encode("utf-8")

    encryptor = Cipher(
        algorithms.AES(aes_key),
        modes.GCM(flipped_iv),
    ).encryptor()

    ciphertext = encryptor.update(plaintext) + encryptor.finalize()
    tag = encryptor.tag
    blob = ciphertext + tag

    return base64.b64encode(blob).decode("utf-8")


# -------------------------------------------------------------------
# Business logic handler
# -------------------------------------------------------------------
class FlowDataExchangeHandler:
    """Handles WhatsApp Flow data exchange requests"""

    def __init__(self):
        self.private_key = FLOW_PRIVATE_KEY
        self.public_key_id = FLOW_PUBLIC_KEY_ID

    def _cart_summary(self, cart: Dict[str, Any]) -> tuple[str, int]:
        """Return (total_amount_str, item_count) for a cart payload."""
        items = cart.get("items") or cart.get("skus") or []
        totals = cart.get("totals") if isinstance(cart, dict) else {}

        total_val: Optional[Any] = None
        if isinstance(totals, dict):
            for key in ("grand_total", "total", "total_amount", "subtotal"):
                if totals.get(key) is not None:
                    total_val = totals.get(key)
                    break

        if total_val is None:
            running = 0.0
            for item in items:
                if not isinstance(item, dict):
                    continue
                try:
                    qty = float(item.get("qty") or 0)
                    price = float(item.get("price") or item.get("base_price") or 0.0)
                    running += qty * price
                except Exception:
                    continue
            total_val = running

        try:
            total_display = f"{float(total_val or 0):.2f}"
        except Exception:
            total_display = "0.00"
        item_count = len([it for it in items if isinstance(it, dict)])
        return total_display, item_count

    def verify_signature(self, payload: str, signature: str) -> bool:
        """Verify request signature from WhatsApp"""
        if not self.private_key:
            logger.warning("flow.verify.no_private_key")
            return True  # Allow in dev mode

        try:
            # WhatsApp sends signature as base64(hmac_sha256(private_key, payload))
            expected_sig = base64.b64encode(
                hmac.new(
                    self.private_key.encode(),
                    payload.encode(),
                    hashlib.sha256,
                ).digest()
            ).decode()

            return hmac.compare_digest(signature, expected_sig)
        except Exception as e:
            logger.error("flow.verify.error", error=str(e))
            return False

    def handle_health_check(self) -> Dict[str, Any]:
        """Handle Flow health check (ping)"""
        # For encrypted ping, we'll wrap this into {"version":"3.0", ...}
        return {
            "data": {
                "status": "active",
            }
        }

    def handle_init(self, flow_token: str) -> Dict[str, Any]:
        """
        Handle Flow initialization - load order draft and prepare screen data

        Args:
            flow_token: User ID passed when Flow was triggered
        """
        user_id = flow_token  # We pass user_id as flow_token

        try:
            # Get current order draft
            draft_data = _ensure_total_amount_field(get_cart(user_id) or {})

            if not draft_data:
                logger.warning("flow.init.empty_draft", user_id=user_id)
                return {
                    "data": {
                        "error": "No items in cart",
                        "cart_items": [],
                        "total_amount": "0.00",
                    },
                    "screen": "ERROR_SCREEN",
                }

            # Validate draft
            draft = OrderDraft.model_validate(draft_data)

            if not draft.items:
                return {
                    "data": {
                        "error": "Cart is empty",
                        "cart_items": [],
                        "total_amount": "0.00",
                    },
                    "screen": "ERROR_SCREEN",
                }

            # Build product list for Flow
            cart_items = []
            for item in draft.items:
                # Try to get retailer_id if missing
                retailer_id = getattr(item, "product_retailer_id", None)
                if not retailer_id:
                    try:
                        query = item.name or item.sku_code
                        retailer_id = find_first_retailer_id(query)
                    except Exception as e:
                        logger.warning(
                            "flow.init.catalog_lookup_failed",
                            user_id=user_id,
                            sku=item.sku_code,
                            error=str(e),
                        )

                cart_items.append(
                    {
                        "id": item.sku_code,
                        "product_retailer_id": retailer_id or "",
                        "name": item.name,
                        "quantity": item.qty,
                        "price": float(item.price),
                        "line_total": float(item.qty * item.price),
                    }
                )

            total_amount = float(draft.total_amount or 0)

            logger.info(
                "flow.init.success",
                user_id=user_id,
                items_count=len(cart_items),
                total=total_amount,
            )

            return {
                "data": {
                    "cart_items": cart_items,
                    "total_amount": f"{total_amount:.2f}",
                    "currency": "Rs",
                },
                "screen": "PRODUCT_LIST",
            }

        except Exception as e:
            logger.error("flow.init.error", user_id=user_id, error=str(e))
            return {
                "data": {
                    "error": f"Failed to load cart: {str(e)}",
                    "cart_items": [],
                    "total_amount": "0.00",
                },
                "screen": "ERROR_SCREEN",
            }

    def handle_data_exchange(self, flow_token: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle Flow data exchange - update quantities, remove items, etc.

        Args:
            flow_token: User ID
            payload: Data from Flow (updated quantities, actions)
        """
        user_id = flow_token
        action = payload.get("action", "update")

        try:
            if action == "update_quantity":
                return self._handle_quantity_update(user_id, payload)

            elif action == "remove_item":
                return self._handle_item_removal(user_id, payload)

            elif action == "confirm_order":
                return self._handle_order_confirmation(user_id, payload)

            else:
                logger.warning("flow.data_exchange.unknown_action", action=action)
                return {
                    "data": {
                        "error": f"Unknown action: {action}",
                    }
                }

        except Exception as e:
            logger.error("flow.data_exchange.error", user_id=user_id, error=str(e))
            return {
                "data": {
                    "error": f"Update failed: {str(e)}",
                }
            }

    def _handle_quantity_update(self, user_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Update item quantity in Firestore"""
        sku_code = payload.get("sku_code")
        new_qty = payload.get("quantity", 0)

        if not sku_code:
            return {"data": {"error": "Missing sku_code"}}

        try:
            qty_int = int(new_qty)
        except Exception:
            qty_int = 0
        qty_int = max(qty_int, 0)

        try:
            draft_data = get_cart(user_id) or {}
        except Exception as e:
            logger.warning("flow.quantity_update.read_failed", user_id=user_id, error=str(e))
            draft_data = {}

        store_id = draft_data.get("store_id") or user_id

        try:
            resp = agentflo_cart_tool(
                {
                    "user_id": user_id,
                    "store_id": store_id,
                    "operations": [{"op": "SET_QTY", "sku_code": sku_code, "qty": qty_int}],
                }
            ) or {}
        except Exception as e:
            logger.error("flow.quantity_update.tool_error", user_id=user_id, error=str(e))
            return {"data": {"error": "Update failed due to a system issue."}}

        if not isinstance(resp, dict) or not resp.get("ok"):
            err_msg = "Cart update failed."
            if isinstance(resp, dict):
                errors = resp.get("errors") or []
                if errors and isinstance(errors[0], dict):
                    err_msg = errors[0].get("message") or err_msg
            return {"data": {"error": err_msg}}

        cart = resp.get("cart") or {}
        total_str, item_count = self._cart_summary(cart)

        logger.info(
            "flow.quantity_update",
            user_id=user_id,
            sku=sku_code,
            new_qty=new_qty,
            new_total=total_str,
        )

        return {
            "data": {
                "success": True,
                "total_amount": total_str,
                "items_count": item_count,
            }
        }

    def _handle_item_removal(self, user_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Remove item from cart"""
        sku_code = payload.get("sku_code")

        if not sku_code:
            return {"data": {"error": "Missing sku_code"}}

        try:
            draft_data = get_cart(user_id) or {}
        except Exception as e:
            logger.warning("flow.item_remove.read_failed", user_id=user_id, error=str(e))
            draft_data = {}

        store_id = draft_data.get("store_id") or user_id

        try:
            resp = agentflo_cart_tool(
                {
                    "user_id": user_id,
                    "store_id": store_id,
                    "operations": [{"op": "REMOVE_ITEM", "sku_code": sku_code}],
                }
            ) or {}
        except Exception as e:
            logger.error("flow.item_remove.tool_error", user_id=user_id, error=str(e))
            return {"data": {"error": "System error while removing item."}}

        if not isinstance(resp, dict) or not resp.get("ok"):
            err_msg = "Failed to remove the item from cart."
            if isinstance(resp, dict):
                errors = resp.get("errors") or []
                if errors and isinstance(errors[0], dict):
                    err_msg = errors[0].get("message") or err_msg
            return {"data": {"error": err_msg}}

        cart = resp.get("cart") or {}
        total_str, item_count = self._cart_summary(cart)

        logger.info(
            "flow.item_removed",
            user_id=user_id,
            sku=sku_code,
            remaining_items=item_count,
        )

        return {
            "data": {
                "success": True,
                "total_amount": total_str,
                "items_count": item_count,
            }
        }

    def _handle_order_confirmation(self, user_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Handle final order confirmation from Flow"""
        confirmed = payload.get("confirmed", False)

        logger.info(
            "flow.order_confirmation",
            user_id=user_id,
            confirmed=confirmed,
        )

        if confirmed:
            # Return success - placeOrderTool will be called after Flow closes
            return {
                "data": {
                    "success": True,
                    "message": "Order confirmed! Processing...",
                    "next_action": "place_order",
                },
                "screen": "SUCCESS_SCREEN",
            }
        else:
            return {
                "data": {
                    "success": False,
                    "message": "Order cancelled",
                    "next_action": "edit_cart",
                },
                "screen": "PRODUCT_LIST",
            }


# -------------------------------------------------------------------
# Flask route handler
# -------------------------------------------------------------------
def handle_flow_request():
    """
    Main Flask route for /flow/order-review

    - Verifies HMAC signature (X-Hub-Signature-256)
    - Supports BOTH:
        * Encrypted payloads (encrypted_flow_data / encrypted_aes_key / initial_vector)
        * Plain payloads with {action, flow_token, data} (useful for manual tests)
    - For encrypted requests, returns Base64(AES-GCM(ciphertext||tag))
      as required by WhatsApp Flows.
    """
    handler = FlowDataExchangeHandler()

    # 1. Verify signature (optional but recommended)
    signature = request.headers.get("X-Hub-Signature-256", "")
    raw_body = request.get_data(as_text=True)

    if not handler.verify_signature(raw_body, signature):
        logger.error("flow.signature_invalid")
        return make_response(jsonify({"error": "Invalid signature"}), 401)

    # 2. Parse JSON
    try:
        outer_data = request.get_json() or {}
    except Exception as e:
        logger.error("flow.parse_error", error=str(e))
        return make_response(jsonify({"error": "Invalid JSON"}), 400)

    # ----------------------------------------------------------------
    # Path A: Encrypted payload (what WhatsApp uses in production)
    # ----------------------------------------------------------------
    if all(k in outer_data for k in ("encrypted_flow_data", "encrypted_aes_key", "initial_vector")):
        try:
            decrypted_body, aes_key, iv = _decrypt_flow_request(outer_data)
        except Exception as e:
            logger.error("flow.decrypt_error", error=str(e))
            # JSON error response (will not satisfy Meta health check but logs the issue)
            return make_response(
                jsonify({"error": f"Decryption failed: {str(e)}"}),
                400,
            )

        request_type = decrypted_body.get("action")
        flow_token = decrypted_body.get("flow_token", "")
        payload = decrypted_body.get("data", {}) or {}

        logger.info(
            "flow.request.encrypted",
            type=request_type,
            token=flow_token[:10] if flow_token else "",
        )

        try:
            # Route to correct handler
            if request_type == "ping":
                # WhatsApp spec for ping:
                # decrypted: { "version": "3.0", "action": "ping" }
                # response:  { "version": "3.0", "data": { "status": "active" } }
                inner_response = handler.handle_health_check()

            elif request_type == "INIT":
                inner_response = handler.handle_init(flow_token)

            elif request_type == "data_exchange":
                inner_response = handler.handle_data_exchange(flow_token, payload)

            else:
                logger.warning("flow.unknown_action", action=request_type)
                inner_response = {
                    "data": {
                        "error": f"Unknown action: {request_type}",
                    }
                }

            # Wrap with version for WhatsApp Flow
            full_response = {
                "version": "3.0",
                **inner_response,
            }

            encrypted_b64 = _encrypt_flow_response(full_response, aes_key, iv)

            resp = make_response(encrypted_b64, 200)
            # Meta examples often use application/json, but body itself is base64 string
            resp.headers["Content-Type"] = "application/json"
            return resp

        except Exception as e:
            logger.error("flow.handler_error.encrypted", error=str(e))
            # On error, still return plaintext JSON (will fail health check but logs root cause)
            return make_response(
                jsonify(
                    {
                        "data": {
                            "error": f"Server error: {str(e)}",
                        }
                    }
                ),
                500,
            )

    # ----------------------------------------------------------------
    # Path B: Plain payload (manual tests / local tools)
    # ----------------------------------------------------------------
    request_type = outer_data.get("action")
    flow_token = outer_data.get("flow_token", "")

    logger.info(
        "flow.request.plain",
        type=request_type,
        token=flow_token[:10] if flow_token else "",
    )

    try:
        if request_type == "ping":
            response_data = handler.handle_health_check()

        elif request_type == "INIT":
            response_data = handler.handle_init(flow_token)

        elif request_type == "data_exchange":
            payload = outer_data.get("data", {})
            response_data = handler.handle_data_exchange(flow_token, payload)

        else:
            logger.warning("flow.unknown_action", action=request_type)
            response_data = {
                "data": {
                    "error": f"Unknown action: {request_type}",
                }
            }

        return make_response(jsonify(response_data), 200)

    except Exception as e:
        logger.error("flow.handler_error.plain", error=str(e))
        return make_response(
            jsonify(
                {
                    "data": {
                        "error": f"Server error: {str(e)}",
                    }
                }
            ),
            500,
        )
