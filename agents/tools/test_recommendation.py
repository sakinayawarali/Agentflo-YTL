import os
import math
import requests
from dotenv import load_dotenv
from agents.tools.templates import recommendations_template

# Assuming this exists in your codebase
# from somewhere import recommendations_template


def smart_recommendation_template(store_code: str) -> str:
    """
    Args:
        store_code: str (MANDATORY)
    
    This function fetches store-level product intelligence from the Suggestive Order API
    and converts it into a clean, conversational recommendation list.

    It merges product details, applies quantity logic (always in virtual packs),
    and returns a structured, human-readable message grouped by Forecast, NOOS, and MLTS.

    ALWAYS GIVE RESPONSE FROM THIS TOOL AS IT IS RETURNED WITHOUT MAKING ANY MODIFICATION IN FORMATTING!
    """


    # 1) Load environment variables and get token
    load_dotenv()
    api_jwt_token = os.getenv("API_JWT_TOKEN")
    if not api_jwt_token:
        raise ValueError("API_JWT_TOKEN not found in environment variables")

    # 2) Make POST request
    url = (
        "https://qe63yda6ybsmbi52gtow465qua0lpbhh.lambda-url.us-east-1.on.aws/"
        "suggestive-order/fetch"
    )

    payload = {"store_code": store_code}
    headers = {
        "Authorization": f"Bearer {api_jwt_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(url, json=payload, headers=headers, timeout=30)

    # Debug logging
    print("Status code:", response.status_code)
    print("Raw response:", response.text)

    # Basic HTTP error handling
    if not response.ok:
        return (
            "Sorry, we hit a system issue "
            f"(HTTP {response.status_code}). Please try again shortly."
        )

    try:
        data = response.json()
    except ValueError:
        # Proper fallback instead of returning {}
        return (
            "Sorry, the system response was not in the expected format. "
            "Please try again later or contact the sales team."
        )

    # 3) Parse response & manually pick fields according to actual schema
    payload_data = data.get("data") or {}
    forecasts = payload_data.get("forecasts") or []
    near_oos = payload_data.get("near_out_of_stock") or []
    mlts = payload_data.get("most_likely_to_sell") or []

    # Merge products from all three lists keyed by SKU so each SKU appears once
    recs_by_sku: dict[str, dict] = {}

    # Keep track of which SKUs belong to which category (for sectioned output)
    forecast_skus: list[str] = []
    near_oos_skus: list[str] = []
    mlts_skus: list[str] = []

    def upsert_from_block(block: dict, source: str) -> None:
        product = block.get("product") or {}
        pricing = block.get("pricing") or product.get("pricing") or {}

        sku_code = product.get("skucode")
        if not sku_code:
            return

        rec = recs_by_sku.get(sku_code, {})

        # Track SKU membership per source for sectioned display
        if source == "forecast":
            if sku_code not in forecast_skus:
                forecast_skus.append(sku_code)
        elif source == "near_oos":
            if sku_code not in near_oos_skus:
                near_oos_skus.append(sku_code)
        elif source == "mlts":
            if sku_code not in mlts_skus:
                mlts_skus.append(sku_code)

        # Core product info
        rec["sku_code"] = sku_code

        official_name = product.get("official_name")
        if official_name:
            rec["official_name"] = official_name

        pivp = product.get("pieces_in_virtual_pack")
        if isinstance(pivp, (int, float)) and pivp > 0:
            rec["pieces_in_virtual_pack"] = pivp

        buy_vp = pricing.get("total_buy_price_virtual_pack")
        if isinstance(buy_vp, (int, float)):
            rec["total_buy_price_virtual_pack"] = buy_vp

        sell_vp = pricing.get("total_sell_price_virtual_pack")
        if isinstance(sell_vp, (int, float)):
            rec["total_sell_price_virtual_pack"] = sell_vp

        margin = pricing.get("retailer_profit_margin")
        if isinstance(margin, (int, float)):
            rec["retailer_profit_margin"] = margin

        # Store raw quantity signals in PIECES, we'll convert to VPs later
        if source == "forecast":
            fq = block.get("forecast_remaining")
            if isinstance(fq, (int, float)):
                rec["forecast_remaining_pieces"] = fq
            rec["forecast_adj"] = block.get("forecast_adj", rec.get("forecast_adj"))
            rec["month_sold_qty"] = block.get("month_sold_qty", rec.get("month_sold_qty"))
        elif source == "near_oos":
            noos_qty = block.get("near_out_of_stock")
            if isinstance(noos_qty, (int, float)):
                rec["near_oos_pieces"] = noos_qty
            rec["near_out_of_stock_week_date"] = block.get(
                "week_date", rec.get("near_out_of_stock_week_date")
            )
        elif source == "mlts":
            rec["mlts_rank"] = block.get("rank", rec.get("mlts_rank"))

        # Fallback for name
        if not rec.get("official_name"):
            rec["official_name"] = f"SKU {sku_code}"

        recs_by_sku[sku_code] = rec

    # Fill from all 3 lists
    for fc in forecasts:
        upsert_from_block(fc, "forecast")

    for noos in near_oos:
        upsert_from_block(noos, "near_oos")

    for itm in mlts:
        upsert_from_block(itm, "mlts")

    # 🔹 NEW: De-duplicate forecast section:
    # Remove any SKU from forecast_skus that also appears in NOOS or MLTS.
    conflict_skus = set(near_oos_skus) | set(mlts_skus)
    forecast_skus = [s for s in forecast_skus if s not in conflict_skus]

    recs = list(recs_by_sku.values())

    # If there are no products at all, fall back to recommendations_template
    if not recs:
        fallback_args = {
            "store_code": store_code,
            "reason": "no_ml_recommendations",
        }
        return recommendations_template(fallback_args)

    # 4) First pass: compute VP quantities from forecast/NOOS to build average
    vp_samples_for_avg: list[float] = []

    def base_vp_from_signals(rec: dict) -> float | None:
        """Return VP qty from forecast/NOOS signals (no MLTS fallback here)."""
        pivp = rec.get("pieces_in_virtual_pack")
        if not isinstance(pivp, (int, float)) or pivp <= 0:
            return None

        forecast_pieces = rec.get("forecast_remaining_pieces")
        near_oos_pieces = rec.get("near_oos_pieces")

        # Priority 1: forecast
        if isinstance(forecast_pieces, (int, float)) and forecast_pieces > 0:
            return forecast_pieces / pivp

        # Priority 2: near_oos
        if isinstance(near_oos_pieces, (int, float)) and near_oos_pieces > 0:
            return near_oos_pieces / pivp

        return None

    for rec in recs:
        vp = base_vp_from_signals(rec)
        if isinstance(vp, (int, float)) and vp > 0:
            vp_samples_for_avg.append(vp)

    avg_vp = sum(vp_samples_for_avg) / len(vp_samples_for_avg) if vp_samples_for_avg else None

    # 5) Second pass: compute final VP quantities per SKU (with MLTS fallback)
    recommended_vp_int: dict[str, int] = {}
    total_value = 0.0

    def final_vp_for_rec(rec: dict) -> int | None:
        """Return final VP quantity for this record, using priority + avg fallback."""
        pivp = rec.get("pieces_in_virtual_pack")
        if not isinstance(pivp, (int, float)) or pivp <= 0:
            return None

        vp = base_vp_from_signals(rec)

        # If no forecast/NOOS suggestion, fallback to avg for MLTS-only SKUs
        if vp is None and isinstance(avg_vp, (int, float)) and avg_vp > 0:
            vp = avg_vp

        if not isinstance(vp, (int, float)) or vp <= 0:
            return None

        # Always ceil to at least 1 VP if positive
        vp_int = math.ceil(vp)
        return vp_int if vp_int > 0 else None

    for rec in recs:
        sku = rec.get("sku_code")
        if not sku:
            continue

        vp_int = final_vp_for_rec(rec)
        if vp_int is None:
            continue

        recommended_vp_int[sku] = vp_int

        buy_price_vp = rec.get("total_buy_price_virtual_pack")
        if isinstance(buy_price_vp, (int, float)):
            total_value += buy_price_vp * vp_int

    # 6) Render into conversational template with sections

    output_lines: list[str] = []
    output_lines.append(
        "Here are some great Peek Freans products recommended for you:"
    )
    output_lines.append("")  # blank line

    def format_product_list_item(index: int, name: str, buy_price_vp, qty_vp) -> list[str]:
        """
        Standardized product listing in this format:

        *1)* _RIO Strawberry Vanilla Packet_  
           Rs 77.30/box × *2* = *Rs 154.60*
        """
        lines: list[str] = []

        # First line: numbered, italic product name, with two spaces at the end for Markdown line-break
        lines.append(f"*{index})* _{name}_  ")

        # Second line: price/box × qty = total
        if isinstance(buy_price_vp, (int, float)) and isinstance(qty_vp, int) and qty_vp > 0:
            price_str = f"{buy_price_vp:.2f}"
            total_value = buy_price_vp * qty_vp
            total_str = f"{total_value:.2f}"
            lines.append(f"   Rs {price_str}/box × *{qty_vp}* = *Rs {total_str}*")
        elif isinstance(buy_price_vp, (int, float)):
            price_str = f"{buy_price_vp:.2f}"
            lines.append(f"   Rs {price_str}/box × *N/A* = *N/A*")
        elif isinstance(qty_vp, int) and qty_vp > 0:
            lines.append(f"   Rs N/A/box × *{qty_vp}* = *N/A*")
        else:
            lines.append("   Rs N/A/box × *N/A* = *N/A*")

        # Blank line after each item
        lines.append("")

        return lines

    product_counter = 0

    def render_section(
        heading_line: str,
        sku_list: list[str],
    ) -> None:
        """Render a single section with heading + items."""
        nonlocal product_counter
        if not sku_list:
            return

        output_lines.append(heading_line)
        output_lines.append("")  # blank line

        for sku in sku_list:
            item = recs_by_sku.get(sku)
            if not item:
                continue

            name = item.get("official_name", f"SKU {sku}")
            buy_price_vp = item.get("total_buy_price_virtual_pack")
            vp_int = recommended_vp_int.get(sku)

            if vp_int is None:
                continue

            # Increment global product counter and render with the new format
            product_counter += 1
            item_lines = format_product_list_item(product_counter, name, buy_price_vp, vp_int)
            output_lines.extend(item_lines)

    # Forecast-based recommendations (after removing any SKUs that also appear in NOOS/MLTS)
    render_section(
        "Yeh woh items hain jo forecast ke mutabiq aap ko stock rakhne chahiye:",
        forecast_skus,
    )

    # Near-out-of-stock recommendations
    render_section(
        "Yeh woh items hain jo aap ke out-of-stock hone walay hain, inko jaldi refill karein:",
        near_oos_skus,
    )

    # Most-likely-to-sell recommendations
    render_section(
        "These items sell very well in your area:",
        mlts_skus,
    )

    # Summary
    if total_value > 0:
        output_lines.append(f"Approx total basket value: {total_value:.2f} Rs")
        output_lines.append("")

    output_lines.append("Tell me which of these products you would like to order.")

    return "\n".join(output_lines) 


if __name__ == "__main__":
    message = smart_recommendation_template("STORE_CODE_PLACEHOLDER")
    print("\n--- Final message ---\n")
    print(message)
