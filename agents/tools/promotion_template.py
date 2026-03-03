# promotion_template.py
# Tool + template helpers for fetching & formatting promotion data.
#
# Inspired by engro_templates_module / templates.py style:
# - Roman Urdu, Ayesha persona
# - WhatsApp-ready text blocks
# - Small, focused helpers

import requests
from typing import List, Optional, Dict, Any
import os
from dotenv import load_dotenv


PROMO_API_URL = "https://qe63yda6ybsmbi52gtow465qua0lpbhh.lambda-url.us-east-1.on.aws/salesflo-promotions"
API_JWT_TOKEN = os.environ.get("API_JWT_TOKEN") # Bearer token for promotions lambda


def fetch_promotions(
    store_code: str,
    sku_codes: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Call the promotion API and return the parsed JSON.

    New API (POST lambda):

    - URL: PROMO_API_URL
    - Header: Authorization: Bearer <token from env API_JWT_TOKEN >
    - Body JSON:
        {"store_code": "N00000003291"}                      # without SKU filter
        {"store_code": "N00000003291", "skus": [..]}      # with SKU filter


    Returns a dict:
    {
        "status": "success" | "error",
        "data": [...] or None,
        "message": "..." (optional)
    }

    On hard failures (network, JSON decode), we normalize into:
    { "status": "error", "message": "..." }
    """
    # Build auth header from env
    token = API_JWT_TOKEN
    if not token:
        return {
            "status": "error",
            "message": (
                "API_JWT_TOKEN environment variable is not set. "
                "Please configure the Bearer token for the promotions lambda."
            ),
        }

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }

    # Build request body
    body: Dict[str, Any] = {
        "storecode": store_code,
    }
    # Strictly follow the lambda contract:
    # - With SKUs: {"store_code": "...", "skus": ["SKU00870", "SKU00904"]}
    # - Without SKUs provided: {"store_code": "..."}
    # shape is exactly what the caller requested.
    if sku_codes is not None:
        body["skus"] = sku_codes

    try:
        resp = requests.post(PROMO_API_URL, headers=headers, json=body)
        resp.raise_for_status()
    except requests.RequestException as e:
        return {
            "status": "error",
            "message": f"Failed to contact promotion service: {e}",
        }

    try:
        payload = resp.json()
    except ValueError:
        return {
            "status": "error",
            "message": "Promotion API did not return valid JSON.",
        }

    # Normalize minimal shape (we expect a {status, data, message} style envelope)
    status = payload.get("status") or payload.get("Status")
    if not status:
        return {
            "status": "error",
            "message": "Unexpected promotion API response format.",
        }

    if str(status).lower() != "success":
        return {
            "status": "error",
            "message": payload.get("message")
            or payload.get("Message")
            or "Failed to fetch promotions for the provided store.",
        }

    return {
        "status": "success",
        "data": payload.get("data") or payload.get("Data") or [],
    }


def _group_promotions_by_sku(promotions: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    """
    Group promotions by SKUCode.

    Output:
    {
      "SKU00909": {
         "sku_code": "SKU00909",
         "sku_name": "CHOCO BITE DC MP Rs.40",   # from SKUDescription
         "promotions": [ {promotion_obj}, ... ]
      },
      ...
    }

    If a promotion has multiple SKUs, it will be added under each one.
    """
    grouped: Dict[str, Dict[str, Any]] = {}

    for promo in promotions:
        sku_list = promo.get("SKUs") or []
        if not sku_list:
            # If somehow promotion has no SKUs, skip grouping
            continue

        for sku in sku_list:
            sku_code = str(sku.get("SKUCode", "")).strip()
            sku_desc = str(sku.get("SKUDescription") or "").strip()
            group_key = sku_code or sku_desc
            if not group_key:
                continue

            bucket = grouped.setdefault(
                group_key,
                {
                    "sku_code": sku_code or None,
                    "sku_name": sku_desc or None,
                    "promotions": [],
                },
            )
            if not bucket.get("sku_name") and sku_desc:
                bucket["sku_name"] = sku_desc
            bucket["promotions"].append(promo)

    return grouped


def _format_promotions_for_whatsapp(
    promotions: List[Dict[str, Any]],
    store_code: str,
    sku_codes: Optional[List[str]] = None,
) -> str:
    """
    Turn raw promotion list into a WhatsApp-ready Roman Urdu message.
    """
    if not promotions:
        if sku_codes:
            return (
                "No active promotions are available for the selected SKUs right now.\n"
                "I can check deals across the rest of the Peek Freans range if you like."
            )
        else:
            return (
                f"No active promotions are currently visible for store *{store_code}*.\n"
                "We can try again shortly or I can suggest the top generic deals."
            )

    grouped = _group_promotions_by_sku(promotions)
    if not grouped:
        # safety: promotions exist but no SKUs parsed
        return (
            "Promotions were found, but SKU mapping is unclear right now.\n"
            "Give me a moment to refresh the data and share details."
        )

    lines: List[str] = []

    if sku_codes:
        lines.append(
            "Here are the promotions on your selected SKUs:"
        )
    else:
        lines.append(
            f"Here are the promotions currently active for your store:"
        )

    lines.append("")

    # Iterate deterministically: sort by sku_name
    for sku_bucket in sorted(grouped.values(), key=lambda b: b.get("sku_name") or ""):
        sku_name = sku_bucket.get("sku_name") or "Promotion item"
        lines.append(f"*{sku_name}*")

        promos = sku_bucket.get("promotions") or []

        for promo in promos:
            desc = promo.get("PromotionDescription") or ""
            if desc:
                lines.append(f"• offer: {desc}")
            else:
                # Fallback construction if description missing
                min_val = promo.get("MinimumValue")
                max_val = promo.get("MaximumValue")
                base_type = promo.get("BaseTypeDescription") or ""
                disc_value = promo.get("DiscountValue")
                bonus = promo.get("BonusValue")

                snippet_parts = []
                if min_val:
                    snippet_parts.append(f"min {min_val} {base_type}".strip())
                if max_val:
                    snippet_parts.append(f"max {max_val} {base_type}".strip())
                if disc_value:
                    snippet_parts.append(f"{disc_value}% discount")
                if bonus and bonus not in ("0", "0.0", 0):
                    snippet_parts.append(f"bonus {bonus}")

                snippet = ", ".join(snippet_parts) if snippet_parts else "promotion available"
                lines.append(f"• offer: {snippet}")

        lines.append("")  # blank line between SKUs

    lines.append(
        "Tell me which item and quantity you would like to stock."
    )

    return "\n".join(lines).strip()


def promotions_tool(
    store_code: str,
    sku_codes: Optional[List[str]] = None,
) -> str:
    """High-level "tool" entrypoint:

    - Calls the promotion API with given store + optional SKUs via POST
    - Handles error / empty cases
    - Returns a WhatsApp-ready Roman Urdu string
    - round off the percentages to 1 decimal place.
    """
    api_result = fetch_promotions( store_code, sku_codes)


    if api_result.get("status") != "success":
        msg = api_result.get("message") or "Failed to fetch promotions for the provided store."
        return (
            "There was an issue fetching promotions from the system.\n"
            f"Detail: {msg}\n"
            "Please try again shortly."
        )

    promotions = api_result.get("data") or []
    # debug=_format_promotions_for_whatsapp(
    #     promotions=promotions,
    #     store_code=store_code,
    #     sku_codes=sku_codes,
    # )
    # print(debug)
    return _format_promotions_for_whatsapp(
        promotions=promotions,
        store_code=store_code,
        sku_codes=sku_codes,
    )

# if __name__== "__main__":
#     load_dotenv()
#     API_JWT_TOKEN = os.getenv("API_JWT_TOKEN") # Bearer token for promotions lambda
#     promotions_tool(store_code="N00000000210")
