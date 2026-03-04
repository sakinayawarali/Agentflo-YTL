import re
import requests
import json
import os
import time
from typing import Optional, Dict, Any, List, Tuple
from utils.logging import logger, debug_enabled

# --- API URL Constants --- 

# Base URL for the Lambda function
API_BASE_URL = "https://qe63yda6ybsmbi52gtow465qua0lpbhh.lambda-url.us-east-1.on.aws"

# Specific API endpoints
CUSTOMER_PHONE_API_URL = f"{API_BASE_URL}/customers/phone"
CUSTOMER_NAME_API_URL = f"{API_BASE_URL}/customers/name"
PRODUCT_SKU_API_URL = f"{API_BASE_URL}/products/sku"
PRODUCT_SEMANTIC_SEARCH_API_URL = f"{API_BASE_URL}/products/search"

# --- Authentication ---
# Fetch the JWT token from an environment variable
API_JWT_TOKEN = os.environ.get("API_JWT_TOKEN")
AGENT_ID=os.environ.get("AGENT_ID")

# --- Invoice Verification ---
INVOICE_VERIFY_URL = os.environ.get("INVOICE_VERIFY_URL")
if not INVOICE_VERIFY_URL:
    _invoice_base = os.environ.get("INVOICE_VERIFY_BASE_URL") or API_BASE_URL
    if _invoice_base:
        INVOICE_VERIFY_URL = f"{_invoice_base.rstrip('/')}/api/InvoiceVerification"

# Fields to strip from product/pricing payloads before returning to the agent
EXCLUDED_PRICING_KEYS = {"total_sell_price_virtual_pack", "retailer_profit_margin"}

# --- YTL demo: use dummy data when no EBM API auth (no user authentication) ---
USE_LOCAL_DATA = os.environ.get("USE_LOCAL_DATA", "").strip().lower() in ("1", "true", "yes")


def _data_dir() -> str:
    """Path to data folder (project root / data)."""
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "data")


def _load_local_json(filename: str) -> Any:
    path = os.path.join(_data_dir(), filename)
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def _normalize_phone_digits(phone: str) -> str:
    return re.sub(r"\D", "", str(phone or "")) if phone else ""


def _customer_payload_from_dummy(c: dict, phone: str) -> dict:
    """Shape expected by adk_helper _bootstrap_user_from_api and _fetch_customer_metadata."""
    store = (c.get("store_code") or c.get("store_key") or "DEMO").strip()
    name = (c.get("contact_name") or c.get("customer_name") or "Customer").strip()
    uid = (c.get("user_id") or c.get("phone") or phone).strip()
    return {
        "success": True,
        "user_id": uid,
        "id": uid,
        "customer_id": uid,
        "store_code": store,
        "store_key": store,
        "contact_name": name,
        "customer_name": name,
        "additional_info": {"storecode": store},
        "data": {
            "store_key": store,
            "contact_name": name,
            "customer_name": name,
            "additional_info": {"storecode": store},
        },
    }


def _strip_pricing_fields(obj: Any) -> Any:
    """
    Remove pricing fields that should not be exposed.
    Works recursively across dicts/lists to cover all API payload shapes.
    """
    if isinstance(obj, dict):
        return {
            key: _strip_pricing_fields(value)
            for key, value in obj.items()
            if key not in EXCLUDED_PRICING_KEYS
        }
    if isinstance(obj, list):
        return [_strip_pricing_fields(item) for item in obj]
    return obj


# --- Tool Response Helpers ---

def _tool_source(system_name: str) -> Dict[str, Any]:
    return {"system": system_name, "timestamp": int(time.time())}


def _tool_success(data: Any, system_name: str) -> Dict[str, Any]:
    return {
        "success": True,
        "data": data,
        "error": None,
        "source": _tool_source(system_name),
    }


def _tool_error(code: str, message: str, retryable: bool, system_name: str) -> Dict[str, Any]:
    return {
        "success": False,
        "data": None,
        "error": {"code": code, "message": message, "retryable": retryable},
        "source": _tool_source(system_name),
    }


def normalize_tool_response(raw: Any, *, system_name: str = "legacy") -> Dict[str, Any]:
    """
    Normalize legacy tool outputs into the standard ToolResponse schema.
    """
    if isinstance(raw, dict) and "success" in raw and "data" in raw:
        return raw

    if isinstance(raw, str):
        if raw.strip().lower().startswith("error:"):
            return _tool_error("LEGACY_ERROR", raw.strip(), False, system_name)
        try:
            parsed = json.loads(raw)
            return _tool_success(parsed, system_name)
        except Exception:
            return _tool_success(raw, system_name)

    return _tool_success(raw, system_name)


def unwrap_tool_response(raw: Any, *, system_name: str = "legacy") -> Tuple[bool, Any, Optional[dict]]:
    normalized = normalize_tool_response(raw, system_name=system_name)
    return (
        bool(normalized.get("success")),
        normalized.get("data"),
        normalized.get("error"),
    )


def _log_tool_call(system_name: str, **fields: Any) -> None:
    if debug_enabled():
        logger.info("tool.call", tool=system_name, **fields)


def _log_tool_result(system_name: str, success: bool, error: Optional[dict] = None) -> None:
    if debug_enabled():
        logger.info("tool.result", tool=system_name, success=success, error=error)


# --- Helper Function for Error Handling ---

def _handle_api_error(
    error: requests.exceptions.RequestException,
    response: Optional[requests.Response] = None
) -> Tuple[str, str, bool]:
    """Return (code, message, retryable) for API failures."""
    if isinstance(error, requests.exceptions.HTTPError) and response is not None:
        status = response.status_code
        if status == 404:
            return "NOT_FOUND", "The requested resource was not found (404).", False
        if status == 401:
            return "AUTH_FAILED", "Authentication failed (401). Check API_JWT_TOKEN.", False
        if 500 <= status < 600:
            return "SERVER_ERROR", f"API server failed (Status {status}).", True
        return "HTTP_ERROR", f"API request failed (Status {status}).", False
    return "NETWORK_ERROR", f"Network error: {error}", True


def _parse_json_response(response: requests.Response) -> Tuple[Optional[Any], Optional[str]]:
    try:
        return response.json(), None
    except ValueError:
        raw_text = None
        try:
            raw_text = response.text
        except Exception:
            raw_text = None
        if raw_text:
            try:
                return json.loads(raw_text), None
            except Exception:
                return None, raw_text
        return None, None


# --- Tool Definitions ---


def search_customer_by_phone(phone_number: str) -> Dict[str, Any]:
    """
    Searches for a customer by their phone number and returns their data.
    When USE_LOCAL_DATA is set or API_JWT_TOKEN is not set, uses dummy data from data/customers.json
    (no EBM API, no user authentication). Unknown numbers get a generic customer so they can talk to the agent.
    """
    system_name = "search_customer_by_phone"
    _log_tool_call(
        system_name,
        phone_tail=str(phone_number)[-4:] if phone_number else "",
        phone_len=len(str(phone_number or "")),
    )

    # YTL demo: use dummy data only (no EBM APIs, no auth)
    if USE_LOCAL_DATA or not API_JWT_TOKEN:
        data = _load_local_json("customers.json")
        norm = _normalize_phone_digits(phone_number)
        if data and isinstance(data.get("customers"), list):
            for c in data["customers"]:
                p = _normalize_phone_digits(c.get("phone") or c.get("user_id") or "")
                if p and norm and (p == norm or p.endswith(norm[-9:]) or norm.endswith(p[-9:])):
                    payload = _customer_payload_from_dummy(c, norm or phone_number)
                    result = _tool_success(payload, system_name)
                    _log_tool_result(system_name, True)
                    return result
        # Unknown number: still allow (no auth) with generic customer
        payload = _customer_payload_from_dummy(
            {"store_code": "DEMO", "contact_name": "Customer", "user_id": norm or phone_number},
            norm or phone_number,
        )
        result = _tool_success(payload, system_name)
        _log_tool_result(system_name, True)
        return result

    headers = {"Authorization": f"Bearer {API_JWT_TOKEN}"}
    try:
        response = requests.get(
            CUSTOMER_PHONE_API_URL,
            headers=headers,
            params={"phone": phone_number},
        )
        response.raise_for_status()
        payload, raw_text = _parse_json_response(response)
        if payload is None:
            result = _tool_error(
                "INVALID_JSON",
                f"Invalid JSON response. Sample: {raw_text[:200] if raw_text else 'empty'}",
                True,
                system_name,
            )
            _log_tool_result(system_name, False, result.get("error"))
            return result
        result = _tool_success(payload, system_name)
        _log_tool_result(system_name, True)
        return result
    except requests.exceptions.RequestException as e:
        code, msg, retryable = _handle_api_error(e, getattr(e, "response", None))
        result = _tool_error(code, msg, retryable, system_name)
        _log_tool_result(system_name, False, result.get("error"))
        return result


def update_customer_name(phone: str, contact_name: str) -> Dict[str, Any]:
    """
    Update customer contact name in backend by phone number.

    Args:
        phone (str): Phone in international format (e.g. +923331234567).
        contact_name (str): New contact name.

    Returns:
        dict: ToolResponse schema.
    """
    system_name = "update_customer_name"
    _log_tool_call(
        system_name,
        phone_tail=str(phone)[-4:] if phone else "",
        has_contact_name=bool(contact_name),
    )

    if not phone or not str(phone).strip():
        result = _tool_error("INVALID_ARGS", "phone is required.", False, system_name)
        _log_tool_result(system_name, False, result.get("error"))
        return result

    if not contact_name or not str(contact_name).strip():
        result = _tool_error("INVALID_ARGS", "contact_name is required.", False, system_name)
        _log_tool_result(system_name, False, result.get("error"))
        return result

    # YTL demo: no EBM API; pretend update succeeded
    if USE_LOCAL_DATA or not API_JWT_TOKEN:
        result = _tool_success({"status": "updated", "demo": True}, system_name)
        _log_tool_result(system_name, True)
        return result

    headers = {
        "Authorization": f"Bearer {API_JWT_TOKEN}",
        "Content-Type": "application/json",
    }
    payload = {
        "phone": str(phone).strip(),
        "contact_name": str(contact_name).strip(),
    }

    try:
        response = requests.put(
            CUSTOMER_NAME_API_URL,
            headers=headers,
            json=payload,
            timeout=20,
        )
        response.raise_for_status()
        data, raw_text = _parse_json_response(response)

        # Accept successful non-JSON responses as success too.
        if data is None:
            if raw_text:
                result = _tool_success({"raw_response": raw_text}, system_name)
            else:
                result = _tool_success({"status": "updated"}, system_name)
        else:
            result = _tool_success(data, system_name)

        _log_tool_result(system_name, True)
        return result

    except requests.exceptions.RequestException as e:
        code, msg, retryable = _handle_api_error(e, getattr(e, "response", None))
        result = _tool_error(code, msg, retryable, system_name)
        _log_tool_result(system_name, False, result.get("error"))
        return result


def verify_invoice(
    tenant_id: str,
    mobile_number: Optional[str],
    invoice_type: str,
    invoice_number: Optional[str] = None,
    store_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """
    Validate a single invoice against the backend Invoice Verification API.

    Args:
        tenant_id (str): Tenant identifier (e.g., "ebm").
        mobile_number (Optional[str]): WhatsApp/mobile number (e.g., +92XXXXXXXXXX).
        invoice_type (str): "salesflo" or "premier".
        invoice_number (Optional[str]): Required for salesflo invoices.
        store_codes (Optional[List[str]]): Store/customer codes (first element is primary).

    Returns:
        dict: ToolResponse schema.
    """
    system_name = "verify_invoice"
    _log_tool_call(
        system_name,
        invoice_type=invoice_type,
        has_invoice_number=bool(invoice_number),
        store_count=len(store_codes or []),
    )

    # YTL demo: no EBM invoice API; return dummy success
    if USE_LOCAL_DATA or not API_JWT_TOKEN or not INVOICE_VERIFY_URL:
        result = _tool_success({"success": True, "verified": True, "message": "Demo: verified (dummy)"}, system_name)
        _log_tool_result(system_name, True)
        return result

    invoice_type_norm = (invoice_type or "").strip().lower()
    if invoice_type_norm not in {"salesflo", "premier"}:
        result = _tool_error(
            "INVALID_ARGS",
            f"Unsupported invoice_type '{invoice_type}'. Expected 'salesflo' or 'premier'.",
            False,
            system_name,
        )
        _log_tool_result(system_name, False, result.get("error"))
        return result
    if invoice_type_norm == "salesflo" and not invoice_number:
        result = _tool_error(
            "INVALID_ARGS",
            "invoice_number is required for salesflo invoices.",
            False,
            system_name,
        )
        _log_tool_result(system_name, False, result.get("error"))
        return result

    payload = {
        "tenant_id": tenant_id,
        "mobile_number": mobile_number,
        "invoice_type": invoice_type_norm,
        "store_codes": store_codes or [],
        "agent_id": AGENT_ID,
    }
    if invoice_type_norm == "salesflo":
        payload["invoice_number"] = invoice_number

    headers = {
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest",
        "Authorization": f"Bearer {API_JWT_TOKEN}",
    }

    try:
        logger.info(
            "verify_invoice.http_request",
            url=INVOICE_VERIFY_URL,
            payload=payload,
            header_keys=list(headers.keys()),
            timeout_sec=30,
        )
        response = requests.post(
            INVOICE_VERIFY_URL,
            headers=headers,
            json=payload,
            timeout=30,
        )
        logger.info(
            "verify_invoice.http_response",
            status_code=response.status_code,
            reason=response.reason,
            text_preview=(response.text[:1200] if response is not None and response.text else ""),
            headers=dict(response.headers or {}),
        )
        response.raise_for_status()
        payload, raw_text = _parse_json_response(response)
        if payload is None:
            result = _tool_error(
                "INVALID_JSON",
                f"Invalid JSON response. Sample: {raw_text[:200] if raw_text else 'empty'}",
                True,
                system_name,
            )
            _log_tool_result(system_name, False, result.get("error"))
            return result
        result = _tool_success(payload, system_name)
        _log_tool_result(system_name, True)
        return result
    except requests.exceptions.RequestException as e:
        try:
            resp_text = ""
            status = None
            if hasattr(e, "response") and getattr(e, "response", None) is not None:
                status = e.response.status_code
                resp_text = (e.response.text or "")[:1200]
            logger.warning(
                "verify_invoice.http_error",
                error=str(e),
                status_code=status,
                response_preview=resp_text,
            )
        except Exception:
            pass
        code, msg, retryable = _handle_api_error(e, getattr(e, "response", None))
        result = _tool_error(code, msg, retryable, system_name)
        _log_tool_result(system_name, False, result.get("error"))
        return result


def search_products_by_sku(
    sku_codes: List[str],
    store_code: Optional[str] = None,
) -> Dict[str, Any]:
    """
    LLM tool: fetch product details by exact SKU code(s). Use this when you
    already know the SKU(s) from a cart, template, or prior lookup and need the
    authoritative product record (name, pack size, availability, pricing fields
    sanitized). Supports batching multiple SKUs in one call.

    Batch behavior:
    - For multi-item intents (e.g., "add these 5 items", "yes add all"),
      pass the full SKU list in one call instead of one-SKU-per-call.
    - Split into multiple calls only when API/tool limits require it.

    Arguments the model should send:
    - sku_codes (required List[str]): One or more SKU codes, e.g., ["SKU00059"]
      or ["SKU00059", "SKU00310"]. Keep the list concise (1-20 items).
    - store_code (Optional[str]): Customer/store code to scope pricing/availability.

    Example calls:
    - search_products_by_sku(["SKU00059"])
    - search_products_by_sku(sku_codes=["SKU00059", "SKU00310"])

    Response shape (ToolResponse):
    - success: bool
    - data: product payload from the API with sensitive pricing fields removed
            (often a list of product objects keyed by the SKUs provided)
    - error: {code, message, retryable} when success is False
    - source: {system, timestamp}
    """
    system_name = "search_products_by_sku"

    # Try to infer store_code from the current callback context when not provided.
    if not store_code:
        try:
            from agents.guardrails.adk_guardrails import get_callback_context

            ctx = get_callback_context()
            state = getattr(ctx, "state", {}) or {}
            inferred = (
                state.get("store_code")
                or state.get("storecode")
                or state.get("store_id")
            )
            store_code = str(inferred).strip() if inferred else None
        except Exception:
            store_code = None

    _log_tool_call(
        system_name,
        sku_count=len(sku_codes or []),
        sku_preview=(sku_codes or [])[:5],
        store_code=store_code,
    )

    # YTL demo: use dummy products from data/products.json (no EBM API)
    if USE_LOCAL_DATA or not API_JWT_TOKEN:
        data = _load_local_json("products.json")
        if data and isinstance(data.get("products"), list):
            codes = [str(s).strip().upper() for s in (sku_codes or []) if s]
            found = [
                p for p in data["products"]
                if (str(p.get("sku_code") or p.get("sku") or "").strip().upper() in codes
            ]
            payload = {"products": found}
            sanitized = _strip_pricing_fields(payload)
            result = _tool_success(sanitized, system_name)
            _log_tool_result(system_name, True)
            return result
        result = _tool_success({"products": []}, system_name)
        _log_tool_result(system_name, True)
        return result

    headers = {"Authorization": f"Bearer {API_JWT_TOKEN}"}
    payload = {"skus": sku_codes}
    if store_code:
        payload["store_code"] = store_code
    try:
        response = requests.post(PRODUCT_SKU_API_URL, json=payload, headers=headers)
        response.raise_for_status()
        payload, raw_text = _parse_json_response(response)
        if payload is None:
            result = _tool_error(
                "INVALID_JSON",
                f"Invalid JSON response. Sample: {raw_text[:200] if raw_text else 'empty'}",
                True,
                system_name,
            )
            _log_tool_result(system_name, False, result.get("error"))
            return result
        sanitized = _strip_pricing_fields(payload)
        result = _tool_success(sanitized, system_name)
        _log_tool_result(system_name, True)
        return result
    except requests.exceptions.RequestException as e:
        code, msg, retryable = _handle_api_error(e, getattr(e, "response", None))
        result = _tool_error(code, msg, retryable, system_name)
        _log_tool_result(system_name, False, result.get("error"))
        return result


def semantic_product_search(
    query: str,
    store_code: Optional[str] = None,
    # company_code: str,  <-- Removed company_code
    min_score: float = 0.6,
    limit: int = 5
) -> str:
    """
    Searches for products using a natural language semantic query.

    Args:
        query (str): The natural language search query 
                    (e.g., "sooper ticky pack").
        store_code (Optional[str]): Customer/store code for scoping results.
        min_score (float, optional): The minimum similarity score to return. 
                                    Defaults to 0.71.
        limit (int, optional): The maximum number of products to return. 
                            Defaults to 5.

    Returns:
        dict: ToolResponse schema with product data on success.
    """
    system_name = "semantic_product_search"
    # Try to infer store_code from the current callback context when not provided.
    if not store_code:
        try:
            # Lazy import to avoid circulars when tools are imported outside ADK.
            from agents.guardrails.adk_guardrails import get_callback_context

            ctx = get_callback_context()
            state = getattr(ctx, "state", {}) or {}
            inferred = (
                state.get("store_code")
                or state.get("storecode")
                or state.get("store_id")
            )
            store_code = str(inferred).strip() if inferred else None
        except Exception:
            store_code = None

    _log_tool_call(
        system_name,
        query_preview=(query or "")[:120],
        min_score=min_score,
        limit=limit,
        store_code=store_code,
    )

    # YTL demo: use dummy products from data/products.json (no EBM API)
    if USE_LOCAL_DATA or not API_JWT_TOKEN:
        data = _load_local_json("products.json")
        if data and isinstance(data.get("products"), list):
            q = (query or "").lower()
            found = []
            for p in data["products"]:
                name = (p.get("name") or p.get("product_name") or p.get("sku_code") or "").lower()
                if q and (q in name or any(w in name for w in q.split())):
                    found.append(p)
                if len(found) >= limit:
                    break
            if not found and data["products"]:
                found = list(data["products"][:limit])
            payload = {"products": found[:limit]}
            sanitized = _strip_pricing_fields(payload)
            result = _tool_success(sanitized, system_name)
            _log_tool_result(system_name, True)
            return result
        result = _tool_success({"products": []}, system_name)
        _log_tool_result(system_name, True)
        return result

    headers = {"Authorization": f"Bearer {API_JWT_TOKEN}"}
    payload = {"query": query, "min_score": min_score, "limit": limit}
    if store_code:
        payload["store_code"] = store_code
    try:
        response = requests.post(PRODUCT_SEMANTIC_SEARCH_API_URL, json=payload, headers=headers)
        response.raise_for_status()
        payload, raw_text = _parse_json_response(response)
        if payload is None:
            result = _tool_error(
                "INVALID_JSON",
                f"Invalid JSON response. Sample: {raw_text[:200] if raw_text else 'empty'}",
                True,
                system_name,
            )
            _log_tool_result(system_name, False, result.get("error"))
            return result
        sanitized = _strip_pricing_fields(payload)
        result = _tool_success(sanitized, system_name)
        _log_tool_result(system_name, True)
        return result
    except requests.exceptions.RequestException as e:
        code, msg, retryable = _handle_api_error(e, getattr(e, "response", None))
        result = _tool_error(code, msg, retryable, system_name)
        _log_tool_result(system_name, False, result.get("error"))
        return result
