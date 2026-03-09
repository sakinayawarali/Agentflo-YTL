import os
import random
from typing import Any, List, Optional
from agents.tools.templates import (
    format_sku_price_block,
    _compute_profit_fields,
    _coerce_float,
    _smart_greeting_line,
    extract_first_name,
    MULTI_MESSAGE_DELIMITER,
)
from utils.logging import logger, debug_enabled

_DEFAULT_COMPANY_NAME = "YTL Cement"


def _resolve_company_name(args: Optional[dict] = None) -> str:
    if isinstance(args, dict):
        for key in ("company_name", "business_name", "brand_name"):
            val = args.get(key)
            if isinstance(val, str) and val.strip():
                return val.strip()
    for key in ("BUSINESS_NAME", "COMPANY_NAME", "BRAND_NAME"):
        val = os.getenv(key)
        if isinstance(val, str) and val.strip():
            return val.strip()
    return _DEFAULT_COMPANY_NAME


# 1) Greeting / General Queries
def greeting_template(
    user_message: Optional[str] = None,
    customer_name: Optional[str] = None,
) -> str:
    """
    YTL Cement demo greeting.

    Personalised, English-only welcome that uses the customer's
    name when available and reinforces the YTL Cement brand.
    """
    company_name = _resolve_company_name() or "YTL Cement"
    name = extract_first_name(customer_name) or ""

    # Normalise spacing for name placeholder
    display_name = name.strip()

    if display_name:
        options = [
            # 1
            (
                f"Hey {display_name}, welcome! 🏗️ I'm here to make finding the right YTL Cement product easy. "
                "You can ask me to:\n"
                "🎯 Recommend a product for your project\n"
                "📋 Explain product types & technical specs\n"
                "⚖️ Compare cement grades & mixes\n"
                "So — what's the project?"
            ),
            # 2
            (
                f"Hi {display_name}! 👋 Great to have you here. I'm your YTL Cement product assistant — here's what I can do:\n"
                "🧱 Match you with the right cement or concrete product\n"
                "🔬 Break down technical specs in simple terms\n"
                "📊 Compare grades & mixes side by side\n"
                "What are you working on?"
            ),
            # 3
            (
                f"Welcome, {display_name}! 🏛️ At YTL Cement, we're all about Building Better — and I'm here to help. Ask me to:\n"
                "🏠 Recommend a product suited to your build\n"
                "📄 Walk you through product specs & features\n"
                "🔍 Find the best mix for your requirements\n"
                "Where do we start?"
            ),
            # 4
            (
                f"Hey {display_name}! 💪 Let's get your project off to a strong start. I can help you:\n"
                "🗺️ Navigate YTL Cement's full product range\n"
                "⚙️ Understand technical specs & applications\n"
                "⚖️ Compare options to find the perfect fit\n"
                "Tell me about your project!"
            ),
            # 5
            (
                f"Hello {display_name}, welcome to YTL Cement! 🌟 I'm your dedicated product guide. Here's what I can help with:\n"
                "🧩 Matching the right product to your project needs\n"
                "📐 Explaining specs, grades & technical details\n"
                "🏆 Comparing our best mixes & cement types\n"
                "So — what are we building?"
            ),
        ]
    else:
        # Fallback when we don't know the customer's name yet.
        options = [
            "Hi there! 👋 Welcome to YTL Cement — Building Better since 1955.\n"
            "I can help you with:\n"
            "🔹 Exploring our cement & concrete products\n"
            "🔹 Finding the right material for your project\n"
            "🔹 Getting product specs & recommendations\n"
            "What are you building today?",
            "Hello! Welcome to YTL Cement. I'm your product assistant — I can answer questions about our full range "
            "of cement, ready-mix concrete, drymix, and aggregates, and help match you to the right product for your "
            "project. Where would you like to start?",
            "Hi! 👷 I'm YTL Cement's product guide. Ask me about product specs, compare materials, or get a "
            "recommendation for your build — I'm here to help you Build Better. What do you need?",
            "Hey, welcome! 🏗️ I'm here to make finding the right YTL Cement product easy. You can ask me to:\n"
            "✅ Recommend a product for your project\n"
            "✅ Explain product types & technical specs\n"
            "✅ Compare cement grades & mixes\n"
            "So — what's the project?",
            "Welcome. At YTL Cement, we've been Building Better for over 70 years. As your dedicated product assistant, "
            "I'm here to help you navigate our full range — from Portland cement to specialty mixes — and find exactly "
            "what your project needs. How can I assist you today?",
        ]

    # Keep a bit of variation so repeated greetings feel natural.
    greeting = random.choice(options)
    return greeting.strip()


# 2) Manual Order Flow
def manual_order_template(args: dict) -> str:
    """
    Manual ordering helper:
    - Confirms SKU
    - Shows close matches if multiple
    - Mentions totals / promos using Rs.
    - Always in English
    """

    output = []
    company_name = _resolve_company_name(args)

    # Confirm main SKU
    sku = args.get("sku") or {}
    if sku:
        name = sku.get("name", "item")
        price = sku.get("price")
        qty = sku.get("qty") or 1

        output.append("Sure, I found this item:")
        sku_lines, _ = format_sku_price_block(name, qty, price, price)
        output.extend(sku_lines)

        output.append("Should I confirm this one?")

    # Multiple matches
    matches = args.get("multiple_matches") or []
    if matches:
        if not sku:
            output.append("Here are some close options in the system:")
        else:
            output.append("These options are coming up along with it:")

        for idx, m in enumerate(matches, 1):
            m_name = m.get("name", "item")
            m_price = m.get("price")

            line_lines, _ = format_sku_price_block(m_name, 1, m_price, m_price, index=idx)
            output.extend(line_lines)

        output.append("Let me know which one to finalize for you?")

    # Quantity ask for confirmed sku
    if sku:
        name = sku.get("name", "item")
        price = sku.get("price")
        if price is not None:
            output.append(f"Alright, the rate for *{name}* is *Rs {price}*.")
        else:
            output.append(f"Alright, *{name}* is selected.")

        output.append("How many cartons / boxes should I add?")

    # Promo already applied
    promo_applied = args.get("promo_applied")
    if promo_applied:
        old_total = promo_applied.get("old_total")
        new_total = promo_applied.get("new_total")
        savings = promo_applied.get("savings")
        output.append("")
        output.append("There's a good offer on this:")
        if old_total is not None and new_total is not None:
            output.append(f"Earlier it was ~Rs {old_total}~, now it will be *Rs {new_total}*.")
        if savings is not None:
            output.append(f"That means you save *Rs {savings}*.")
        output.append("It's a good deal... should I lock it at this rate?")

    # Promo needs more quantity
    promo_needs_more = args.get("promo_needs_more")
    if promo_needs_more:
        min_qty = promo_needs_more.get("min_qty")
        old_bulk_total = promo_needs_more.get("old_bulk_total")
        new_bulk_total = promo_needs_more.get("new_bulk_total")
        save_bulk = promo_needs_more.get("save_bulk")

        output.append("")
        if min_qty:
            output.append(f"If you take {min_qty} quantity, the deal will be even better:")
        if old_bulk_total is not None and new_bulk_total is not None:
            output.append(f"Total will go from ~Rs {old_bulk_total}~ to *Rs {new_bulk_total}*.")
        if save_bulk is not None:
            output.append(f"That's a straight saving of *Rs {save_bulk}*.")
        output.append("Should I increase the quantity for you?")

    # Simple total info (no promo)
    no_promo = args.get("no_promo")
    if no_promo:
        total = no_promo.get("total")
        if total is not None:
            output.append(f"Alright, current total is *Rs {total}*.")
        output.append(f"If you want, I can suggest a few more {company_name} items for better margin?")

    # CTA override if provided
    if "cta_line" in args:
        output.append(args["cta_line"])

    return "\n".join([line for line in output if line]).strip()


# 5) Order Draft (Show / Edit)
# Change signature to accept named arguments instead of a generic 'args' dict
def order_draft_template(
    cart: Optional[dict] = None,
    draft: Optional[dict] = None,
    *,
    ok: Optional[bool] = None,
    errors: Optional[List[Any]] = None,
    warnings: Optional[List[Any]] = None,
) -> str:
    """
    Formats the current order draft like natural WhatsApp Ayesha style.
    Updated to support new order draft structure with extended details.

    :param cart: The cart object returned from agentflo_cart_tool GET_CART.("cart": <cart_dict>)
    :param draft: Alternative parameter name for the order draft. ("cart": <cart_dict>)
    :param ok: Optional passthrough from agentflo_cart_tool (ignored, accepted for compatibility).
    :param errors: Optional passthrough from agentflo_cart_tool (ignored, accepted for compatibility).
    :param warnings: Optional passthrough from agentflo_cart_tool (ignored, accepted for compatibility).
    """

    # ---------------- CHANGED LOGIC START ----------------
    # Unwrap if caller passed the full agentflo_cart_tool response
    cart_payload = cart
    if isinstance(cart_payload, dict) and "cart" in cart_payload:
        inner_cart = cart_payload.get("cart")
        if isinstance(inner_cart, dict):
            cart_payload = inner_cart

    # Consolidate inputs: use cart or draft, whichever is provided
    # We default to empty dict {} if neither is passed to prevent crashes
    target_data = cart_payload or draft or {}

    # Now point your internal variables to this target_data
    items = target_data.get("items") or target_data.get("lines") or target_data.get("skus") or []
    basket_items = []
    basket = target_data.get("basket")
    if isinstance(basket, dict) and isinstance(basket.get("items"), list):
        basket_items = basket.get("items") or []

    def _has_rich_fields(items_list):
        for it in items_list:
            if not isinstance(it, dict):
                continue
            has_price = it.get("base_price") or it.get("final_price") or it.get("line_total")
            name = it.get("name")
            # treat placeholder/empty names as missing so we can fall back to basket items that often have prices
            has_meaningful_name = isinstance(name, str) and name.strip() and name.strip().lower() not in {"item", "items"}
            if has_meaningful_name or has_price:
                return True
        return False

    if basket_items and not _has_rich_fields(items):
        if debug_enabled():
            try:
                logger.info(
                    "order_draft_template.basket_fallback",
                    items_count=len(items),
                    basket_items=len(basket_items),
                    has_totals=bool(target_data.get("totals")),
                )
            except Exception:
                pass
        items = basket_items

    # Handle totals (ensure safe access if key is missing)
    cart_totals = target_data.get("totals") if isinstance(target_data.get("totals"), dict) else {}
    subtotal = cart_totals.get("subtotal")
    grand_total = cart_totals.get("grand_total")
    discount_total = cart_totals.get("discount_total")

    # Handle total_amount fallback logic
    total = (
        target_data.get("total_amount")
        or target_data.get("total")
        or target_data.get("grand_total")
        or target_data.get("subtotal")
        or grand_total
        or subtotal
    )
    # ---------------- CHANGED LOGIC END ----------------

    company_name = _resolve_company_name(target_data if isinstance(target_data, dict) else None)

    if not items:
        return f"Your order draft is empty right now... should I add a {company_name} item?"

    out = []
    out.append("Alright,\n")
    out.append("Here are the items included in your order:")

    total_savings = 0.0
    computed_total = 0.0

    for idx, item in enumerate(items, 1):
        name = item.get("name", "item")
        qty = item.get("qty") or item.get("quantity") or 0

        # Extract details
        base_price = (
            item.get("base_price")
            or item.get("consumer_price")
            or item.get("list_price")
            or item.get("mrp")
            or item.get("unit_price")
            or item.get("price")
        )
        final_price = (
            item.get("final_price")
            or item.get("discounted_price")
            or item.get("unit_price_final")
            or item.get("unit_price")
            or item.get("price")
        )
        discount_value = item.get("discount_value")
        line_total = (
            item.get("line_total")
            or item.get("linetotal")
            or item.get("lineamount")
            or item.get("line_total_amount")
        )

        # Fallbacks
        if base_price is None:
            old_line_total = item.get("old_line_total")
            if old_line_total and qty:
                try:
                    base_price = float(old_line_total) / float(qty)
                except (ValueError, ZeroDivisionError):
                    base_price = item.get("price")
            else:
                base_price = item.get("price")

        if final_price is None:
            if line_total and qty:
                try:
                    final_price = float(line_total) / float(qty)
                except (ValueError, ZeroDivisionError):
                    final_price = base_price
            else:
                final_price = base_price

        if debug_enabled():
            try:
                logger.info(
                    "order_draft_template.line_render",
                    idx=idx,
                    name=name,
                    qty=qty,
                    base_price=base_price,
                    final_price=final_price,
                    line_total=line_total,
                    had_missing_price=base_price is None and final_price is None and line_total is None,
                )
            except Exception:
                pass

        # Ensure format_sku_price_block is available in your scope
        sku_lines, price_meta = format_sku_price_block(
            name,
            qty,
            base_price,
            final_price,
            line_total=line_total,
            discount_value=discount_value,
            index=idx,
        )
        out.extend(sku_lines)
        out.append("")

        meta_total = price_meta.get("line_total")
        if meta_total is not None:
            try:
                computed_total += float(meta_total)
            except (TypeError, ValueError):
                pass

        savings_meta = price_meta.get("savings_total")
        if savings_meta is not None:
            try:
                total_savings += float(savings_meta)
            except (TypeError, ValueError):
                pass

    totals_source = {}
    if isinstance(cart_totals, dict):
        totals_source.update(cart_totals)
    if isinstance(target_data, dict):
        for key in (
            "subtotal",
            "discount_total",
            "total",
            "grand_total",
            # "profit",
            # "profit_total",
            # "profit_margin",
            # "profit_margin_pct",
        ):
            if totals_source.get(key) is None and key in target_data:
                totals_source[key] = target_data.get(key)

    subtotal_breakdown = _coerce_float(totals_source.get("subtotal"))
    discount_breakdown = _coerce_float(totals_source.get("discount_total"))
    total_breakdown = _coerce_float(totals_source.get("total"))
    grand_total_breakdown = _coerce_float(totals_source.get("grand_total"))
    # profit_breakdown = _coerce_float(totals_source.get("profit"))
    # profit_total_breakdown = _coerce_float(totals_source.get("profit_total"))
    # profit_margin_breakdown = _coerce_float(totals_source.get("profit_margin"))
    # profit_margin_pct_breakdown = _coerce_float(totals_source.get("profit_margin_pct"))

    show_breakdown = any(
        val is not None
        for val in (
            subtotal_breakdown,
            discount_breakdown,
            total_breakdown,
            grand_total_breakdown,
            # profit_breakdown,
            # profit_total_breakdown,
            # profit_margin_breakdown,
            # profit_margin_pct_breakdown,
        )
    )

    # Summary
    # Prefer API totals when provided
    if discount_total is not None:
        disc_val = _coerce_float(discount_total)
        if disc_val is not None:
            total_savings = disc_val

    subtotal_val = _coerce_float(subtotal)
    grand_total_val = _coerce_float(grand_total)

    if total is None:
        total = grand_total if grand_total is not None else subtotal
    if total is None and computed_total > 0:
        total = computed_total

    total_val = _coerce_float(total)

    old_total_for_display = None
    if subtotal_val is not None and total_val is not None and subtotal_val > total_val:
        old_total_for_display = subtotal_val
        if total_savings < subtotal_val - total_val:
            total_savings = subtotal_val - total_val
    elif total_val is not None and total_savings > 0:
        old_total_for_display = total_val + total_savings

    # if total_val is not None:
    #     if total_savings > 1.0 and old_total_for_display is not None:
    #         out.append(f"Total ~{old_total_for_display:,.2f} Rs~  *{total_val:,.2f} Rs*")
    #     else:
    #         out.append(f"Total:  *{total_val:,.2f} Rs*")
    # elif total is not None:
    #     out.append(f"Total:  *{total} Rs*")

    # Profit / margin view
    # Updated containers to look at target_data instead of 'draft'
    profit_containers = [
        {**target_data, **(cart_totals or {})} if isinstance(target_data, dict) else target_data,
        target_data,
        cart_totals,
    ]
    total_sell = profit = margin_pct = None
    for container in profit_containers:
        # Ensure helper functions _compute_profit_fields and _coerce_float are imported/available
        total_sell, profit, margin_pct = _compute_profit_fields(container, total_key="total_amount")
        if total_sell is None:
            total_sell, profit, margin_pct = _compute_profit_fields(container, total_key="total")
        if total_sell is None:
            total_sell, profit, margin_pct = _compute_profit_fields(container, total_key="grand_total")
        if any(v is not None for v in (total_sell, profit, margin_pct)):
            break
    if margin_pct is None:
        margin_pct = cart_totals.get("margin_pct") if isinstance(cart_totals, dict) else None

    # if total_sell is not None and profit is not None:
    #     if margin_pct is not None:
    #         out.append(
    #             f"Selling this, your sale will be around *{total_sell} Rs*, "
    #             f"and your profit about *{profit} Rs* (~{margin_pct:.1f}% margin)."
    #         )
    #     else:
    #         out.append(
    #             f"Selling this, your sale will be around *{total_sell} Rs*, "
    #             f"and your profit about *{profit} Rs*."
    #         )
    # elif margin_pct is not None:
    #     out.append(f"Approx margin on this order is ~{margin_pct:.1f}%. ")

    if show_breakdown:
        out.append("-----------------------------")
        if subtotal_breakdown is not None:
            out.append(f"Subtotal: Rs {subtotal_breakdown:,.2f}")
        if discount_breakdown is not None:
            out.append(f"Total Discount: Rs {discount_breakdown:,.2f}")
        # if total_breakdown is not None:
        #     out.append(f"Total:          Rs {total_breakdown:,.2f}")
        if grand_total_breakdown is not None:
            out.append("Grand Total (incl. GST):")
            out.append(f"Rs {grand_total_breakdown:,.2f}")
        # if profit_breakdown is not None:
        #     out.append(f"Profit:         Rs {profit_breakdown:,.2f}")
        # # if profit_total_breakdown is not None:
        # #     out.append(f"Profit Total:   Rs {profit_total_breakdown:,.2f}")
        # margin_display = (
        #     profit_margin_pct_breakdown
        #     if profit_margin_pct_breakdown is not None
        #     else profit_margin_breakdown
        # )
        # if margin_display is not None:
        #     out.append(f"Profit Margin:  {margin_display:.2f}%")
        out.append("-----------------------------")

    out.append("")
    out.append("Confirm now and get *10% off* with our *Hari Raya promotion*.")
    out.append("")

    follow_up = "Should I confirm this for you?"
    main_body = "\n".join(out).strip()
    if follow_up:
        return MULTI_MESSAGE_DELIMITER.join([part for part in (main_body, follow_up) if part])
    return main_body


def vn_order_draft_template(args: dict) -> str:
    """
    Short, voice-note friendly version of the current order draft.
    - English spoken style.
    - Says quantity as: "total {qty} boxes"
    - Keeps all amounts exactly as provided (no rounding)
    - Designed to be sent as text *and* converted to audio.
    - If total_sell / profit are provided on the draft, we also speak out expected sale
      and profit in simple words.
    """
    draft = args.get("draft") or {}
    lines = draft.get("items") or draft.get("lines") or []
    total = draft.get("total_amount") or draft.get("total")

    if not lines:
        return "I don't have any order draft right now."

    parts = []

    customer_name = extract_first_name(args.get("customer_name"))
    if customer_name:
        parts.append(f"Alright {customer_name},")
    else:
        parts.append("Alright,")

    parts.append("your order includes these items,")

    for line in lines:
        name = line.get("name", "item")
        qty = line.get("qty", 0)
        line_total = line.get("line_total")
        qty_str = str(qty)
        unit_word = "boxes"

        if line_total is not None:
            parts.append(f"{name}, total {qty_str} {unit_word}, {line_total} rupees")
        else:
            parts.append(f"{name}, total {qty_str} {unit_word}")

    if total is not None:
        parts.append(f"total comes to {total} rupees.")

    # Profit line for VN
    total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total_amount")
    if total_sell is None:
        total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total")
    if total_sell is not None and profit is not None:
        parts.append(
            f"Selling all of this, your total sale will be {total_sell} rupees, "
            f"and your profit will be about {profit} rupees."
        )
    else:
        parts.append("Your profit should be good, InshaAllah.")

    parts.append("Confirm now for 10% off with our Hari Raya promotion.")
    parts.append("Should I confirm this for you?")

    return " ".join(p.strip() for p in parts if p and str(p).strip())


# 6) Order Status -> Invoice
def invoice_status_template(args: dict) -> str:
    """
    Invoice / order status summary in English.
    """
    company_name = _resolve_company_name(args)
    count = args.get("count")
    total = args.get("total")
    old_total = args.get("old_total")
    suggest_topup = args.get("suggest_topup", False)
    topup_gap = args.get("topup_gap")

    if not count or total is None:
        return "I can't get the order status clearly right now... I'll check and update you."

    lines = []
    lines.append(f"Here is the status of your {company_name} order:")

    lines.append(f"• items: {count}")

    if old_total:
        lines.append(f"• previous value: ~Rs {old_total}~")

    lines.append(f"• final total: *Rs {total}*")

    # Profit / margin overview
    total_sell, profit, margin_pct = _compute_profit_fields(args, total_key="total")
    if total_sell is not None and profit is not None:
        if margin_pct is not None:
            lines.append(
                f"• Selling this, your sale should be around *Rs {total_sell}*, "
                f"and your profit roughly *Rs {profit}* (~{margin_pct:.1f}% margin)."
            )
        else:
            lines.append(
                f"• Selling this, your sale should be around *Rs {total_sell}*, "
                f"and your profit roughly *Rs {profit}*."
            )

    if suggest_topup and topup_gap:
        lines.append(
            f"If you add around Rs {topup_gap} more, the mix will improve... should I show a small SKU?"
        )

    lines.append("Should I send the invoice, or do you want to add something?")

    return "\n".join(lines).strip()


# 7) Reorder (Repeat Last Order)
def reorder_template(args: dict) -> str:
    """
    Shows last order for quick repeat in English.
    """
    company_name = _resolve_company_name(args)
    last_order = args.get("last_order") or {}
    lines_data = last_order.get("lines") or []

    if not lines_data:
        return f"I can't find your last {company_name} order clearly right now."

    out = []
    out.append(f"Here's your last {company_name} order:")

    for idx, line in enumerate(lines_data, 1):
        name = line.get("name", "item")
        qty = line.get("qty", 0)
        out.append(f"{idx}) *{name}* × {qty}")

    out.append("")
    out.append("Should I repeat the same, or do you want changes?")

    return "\n".join(out).strip()


# 8) Bundles / Combos (simple, no-arg)
def bundles_template():
    """
    Static-ish bundles message in English.
    """
    company_name = _resolve_company_name()
    intro_options = [
        f"Here are a couple of {company_name} combos doing really well:",
        "Want a couple of combo ideas that lift shop sales:",
    ]
    reason_lines = [
        "good margin",
        "kids and families both like it",
        "moves fast on shelf",
    ]

    intro = random.choice(intro_options)

    bundles = [
        {
            "title": "Evening combo",
            "line": random.choice(reason_lines),
        },
        {
            "title": "Kids combo",
            "line": random.choice(reason_lines),
        },
    ]

    out = [intro]
    for b in bundles:
        out.append(f"• *{b['title']}*")
        out.append(f"  👉 {b['line']}")

    out.append("")
    out.append("Which combo should I lock for you?")

    return "\n".join(out).strip()


# 9) Product Info
def product_info_template(args: dict) -> str:
    """
    Product info for SKUs in English.
    """
    sku = args.get("sku") or {}
    highlights = args.get("highlights") or []
    promo_hint = args.get("promo_hint", False)

    name = sku.get("name", "item")
    variant = sku.get("variant", "")

    out = [f"Here are a few details for *{name}*:"]

    for point in highlights[:3]:
        out.append(f"• {point}")

    if variant:
        out.append(f"• pack size: *{variant}*")

    if promo_hint:
        out.append("• There's a good deal on this right now... want the details?")

    out.append("Should I add this to your order draft?")

    return "\n".join(out).strip()


# 10) Top Sellers / Upsell
def recommendations_template(args: dict) -> str:
    """
    Top sellers / upsell list in English.
    """
    company_name = _resolve_company_name(args)
    intro_pool = [
        f"In your area, these {company_name} items move the fastest:",
        "Here are the top-selling biscuits and cakes near you:",
        "These SKUs are moving very fast in your area:",
    ]

    cta_pool = [
        "Which ones should I add to the order",
        "Tell me which ones to add",
        "Which 2-3 SKUs should we start with",
    ]

    intro_line = random.choice(intro_pool)
    cta_line = random.choice(cta_pool)

    products = args.get("area_top_sellers") or []

    if not products:
        return (
            "The best sellers list is refreshing... "
            "I can share general top deals meanwhile - want that?"
        )

    out = [intro_line]

    for i, item in enumerate(products[:5], 1):
        name = item.get("name", "item")
        price = item.get("price")
        note = item.get("note")

        if price is not None:
            price_line = f"*{price} Rs*"
        else:
            price_line = "(price not available)"

        line = f"{i}) *{name}* - {price_line}"
        if note:
            line += f"\n   👉 _{note}_"

        out.append(line)

    out.append("")
    out.append(cta_line + "?")

    return "\n".join(out).strip()


# 11) Seasonal Advice (simple, no-arg)
def seasonal_advice_template():
    """
    Simple seasonal nudge in English.
    """
    company_name = _resolve_company_name()
    season_lines = [
        "During Ramzan and Eid, demand for biscuits and cakes rises quickly",
        f"In winter season, tea-time sales of {company_name} naturally go up",
        "In school season, snack pack rotation is very fast",
    ]
    cta_lines = [
        f"If you keep a little extra {company_name} stock now, you'll be safe",
        "If you want, I can share 3-4 seasonal SKUs that will move fast",
    ]

    season_line = random.choice(season_lines)
    cta_line = random.choice(cta_lines)

    out = []
    out.append(f"{season_line}.")
    out.append("Want me to recommend a few for best stock?")
    out.append(cta_line + "?")

    return "\n".join(out).strip()


def current_total_reminder(total: float, casual: bool = True) -> str:
    """
    Quick reminder of current order total in English.
    """
    if casual:
        phrases = [
            f"Your order is currently at {total} Rs",
            f"Current total is {total} Rs",
            f"So far your order is {total} Rs",
        ]
        return random.choice(phrases)
    else:
        return f"Current order total: {total} Rs"


# 12) Objection Handling (simple, no-arg)
def objection_handling_template():
    """
    Generic objection handling lines for Ayesha, English.
    """
    blocks = []

    company_name = _resolve_company_name()

    # Price objection
    blocks.append(
        "I understand - budgets can feel tight sometimes.\n"
        "But when a deal kicks in, the story is this:\n"
        "first the old rate... then the promo rate, and the difference is your savings.\n"
        "Buying at this rate often pays off, and if you take a small trial, the risk is lower."
    )

    # Dont need more
    blocks.append(
        "If you feel stock is enough, that's fine.\n"
        f"But if nearby shops pick up fresh {company_name} stock, "
        "customers can shift there.\n"
        "Keeping 1-2 cartons of fast movers keeps the shelf full and sales safer."
    )

    # Customers won't buy
    blocks.append(
        "If you think customers won't pick it up, my suggestion is a small trial... "
        "one or two cartons or a few boxes.\n"
        "If it sells, we increase quantity next time; otherwise you learn the local taste."
    )

    blocks.append("I'm not forcing it - just sharing options... the decision is yours.")

    return "\n\n".join(blocks).strip()


def personalized_greeting_template(args: dict) -> str:
    """
    Personalized greeting AFTER sending catalog.
    English.
    Expected args example:
    {
        "customer_name": "Rohail",
        "store_name": "Al-Madina Store",
        "catalog_sent": True
    }
    """
    company_name = _resolve_company_name(args)
    customer_name = (
        args.get("customer_name")
        or args.get("retailer_name")
        or args.get("owner_name")
        or "there"
    )
    customer_name = extract_first_name(customer_name) or "there"

    store_name = (
        args.get("store_name")
        or args.get("outlet_name")
        or args.get("shop_name")
        or ""
    )

    catalog_sent = args.get("catalog_sent", True)

    # Line 1 - salam + name (+ optional shop)
    if store_name:
        line1 = f"Assalam o Alaikum {customer_name} ({store_name}),"
    else:
        line1 = f"Assalam o Alaikum {customer_name},"

    # Line 2 - identity
    line2 = f"I'm Ayesha from {company_name}."

    # Line 3 - catalog status
    if catalog_sent:
        line3 = "I've sent the catalogue - please check when you're free."
    else:
        line3 = f"Opening the {company_name} catalogue for you now."

    # Line 4 - next step
    line4 = "Tell me, what would you like to see first?"

    return "\n".join([line1, line2, line3, line4])
