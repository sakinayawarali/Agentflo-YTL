
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
from utils.logging import logger

_DEFAULT_COMPANY_NAME = "Peek Freans"


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


def _is_sku_like(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    token = value.strip()
    if not token:
        return False
    upper = token.upper()
    return upper.startswith("SKU") and any(ch.isdigit() for ch in upper) and " " not in upper



# 1) Greeting / General Queries
def greeting_template(
    user_message: Optional[str] = None,
    customer_name: Optional[str] = None,
):
    """
    Warm, street-smart greeting for the retailer.
    Roman Urdu version, Ayesha persona.
    always call send_product_catalogue tool after running this template.
    """
    company_name = _resolve_company_name()
    greeting_lines = [
        "Assalam o Alaikum bhai",
        "Salam jee",
        "Assalam o Alaikum",
    ]
    salam_lines = [
        "Waalikum Salam bhai",
        "Waalikum Assalam jee",
        "Walaikum Salam bhai",
    ]
    hello_lines = [
        "Hello bhai",
        "Hello jee",
        "Hi bhai",
    ]
    smalltalk_lines = [
        "kaam kaisa chal raha hai",
        "dukaan ka kya scene hai aaj kal",
        "biscuit ki sale kaisi ja rahi hai",
    ]
    value_hooks = [
        f"{company_name} ke top sellers",
        f"{company_name} kay fresh stock",
        f"{company_name} biscuits aur cakes ki best range",
        "aap ke area ki fast moving items",
    ]
    help_options = [
        "kya main order banane main help karoon",
        "kya main aaj ke top sellers bataoon",
        "kya main naye items bataoon",
        "aaj ki best deals check karein",
    ]
    cta_lines = [
        "batain bhai, aaj kya check karna chahenge",
        "kis item se start karein aaj",
        "jee batain, pehle konsa product dekhna chahenge?",
    ]

    first_name = extract_first_name(customer_name)
    if first_name:
        greeting_line = f"Hi {first_name} bhai"
    else:
        greeting_line = _smart_greeting_line(user_message, salam_lines, hello_lines, greeting_lines)
    smalltalk_line = random.choice(smalltalk_lines)
    value_hook = random.choice(value_hooks)
    help_line = random.choice(help_options)
    cta_line = random.choice(cta_lines)

    # "jee" and "aap" used as per persona
    final_response = (
        f"{greeting_line},\n"
        f"{smalltalk_line}?\n"
        f"Main Ayesha baat kar rahi hoon {company_name} se... {value_hook} available hain.\n"
        f"{help_line}?\n"
        f"{cta_line}?"
    )

    return f"{final_response.strip()}"


# 2) Manual Order Flow
def manual_order_template(args: dict) -> str:
    """
    Manual ordering helper:
    - Confirms SKU
    - Shows close matches if multiple
    - Mentions totals / promos using Rs.
    - Always in Roman Urdu
    """

    output = []
    company_name = _resolve_company_name(args)

    # Confirm main SKU
    sku = args.get("sku") or {}
    if sku:
        name = sku.get("name", "item")
        price = sku.get("price")
        qty = sku.get("qty") or 1

        output.append("Jee bhai, yeh item mil gaya hai:")
        sku_lines, _ = format_sku_price_block(name, qty, price, price)
        output.extend(sku_lines)

        output.append("Kya yehi confirm karoon?")

    # Multiple matches
    matches = args.get("multiple_matches") or []
    if matches:
        if not sku:
            output.append("Jee dekhein, system main is se milti julti yeh options hain:")
        else:
            output.append("Is ke sath yeh options bhi aa rahi hain:")

        for idx, m in enumerate(matches, 1):
            m_name = m.get("name", "item")
            m_price = m.get("price")

            line_lines, _ = format_sku_price_block(m_name, 1, m_price, m_price, index=idx)
            output.extend(line_lines)

        output.append("Batain bhai, in main se kaunsa final karoon aap ke liye?")

    # Quantity ask for confirmed sku
    if sku:
        name = sku.get("name", "item")
        price = sku.get("price")
        if price is not None:
            output.append(f"Jee theek, *{name}* ka rate *{price} Rs* hai.")
        else:
            output.append(f"Jee theek, *{name}* select ho gaya.")

        output.append("Ab batain, is ke kitne cartons / boxes kar doon?")

    # Promo already applied
    promo_applied = args.get("promo_applied")
    if promo_applied:
        old_total = promo_applied.get("old_total")
        new_total = promo_applied.get("new_total")
        savings = promo_applied.get("savings")
        output.append("")
        output.append("Bhai is pe achi offer lagi hui hai:")
        if old_total is not None and new_total is not None:
            output.append(f"Pehle ~{old_total} Rs~ ka tha, ab *{new_total} Rs* ka parega.")
        if savings is not None:
            output.append(f"Matlab aap ki *{savings} Rs* ki bachat.")
        output.append("Faida hai jee... is rate pe done kar dein?")

    # Promo needs more quantity
    promo_needs_more = args.get("promo_needs_more")
    if promo_needs_more:
        min_qty = promo_needs_more.get("min_qty")
        old_bulk_total = promo_needs_more.get("old_bulk_total")
        new_bulk_total = promo_needs_more.get("new_bulk_total")
        save_bulk = promo_needs_more.get("save_bulk")

        output.append("")
        if min_qty:
            output.append(f"Dekhein bhai, agar aap {min_qty} quantity kar lein to deal aur achi ho jayegi:")
        if old_bulk_total is not None and new_bulk_total is not None:
            output.append(f"Total ~{old_bulk_total} Rs~ ke bajaye *{new_bulk_total} Rs* ho jayega.")
        if save_bulk is not None:
            output.append(f"Yani seedhi seedhi *{save_bulk} Rs* ki extra bachat.")
        output.append("Kya main aap ke liye quantity barha doon?")

    # Simple total info (no promo)
    no_promo = args.get("no_promo")
    if no_promo:
        total = no_promo.get("total")
        if total is not None:
            output.append(f"Jee theek hai, abhi total *{total} Rs* ban raha hai.")
        output.append(f"Agar aap chahen to main kuch aur {company_name} ki items bataoon munafay ke liye?")

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
            name = it.get("name")
            has_meaningful_name = isinstance(name, str) and name.strip() and name.strip().lower() not in {"item", "items"}
            has_price = (
                it.get("base_price")
                or it.get("final_price")
                or it.get("line_total")
                or it.get("unit_price")
                or it.get("discounted_price")
                or it.get("lineamount")
                or it.get("linetotal")
                or it.get("line_total_amount")
                or it.get("price")
            )
            if has_meaningful_name or has_price:
                return True
        return False

    if basket_items and not _has_rich_fields(items):
        items = basket_items

    try:
        preview = []
        for idx, it in enumerate(items[:25], 1):
            if not isinstance(it, dict):
                continue
            preview.append(
                {
                    "idx": idx,
                    "sku_code": it.get("sku_code") or it.get("sku") or it.get("skucode"),
                    "name": it.get("name") or it.get("sku_name") or it.get("product_name"),
                    "qty": it.get("qty") or it.get("quantity"),
                    "base_price": it.get("base_price") or it.get("consumer_price") or it.get("list_price"),
                    "final_price": it.get("final_price") or it.get("discounted_price") or it.get("unit_price"),
                    "line_total": it.get("line_total") or it.get("lineamount") or it.get("linetotal"),
                }
            )
        logger.info(
            "order_draft_template.render_input",
            item_count=len(items),
            preview=preview,
            from_basket=bool(basket_items),
        )
    except Exception:
        pass
    
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
    customer_first_name = None
    if isinstance(target_data, dict):
        nested_customer = target_data.get("customer")
        nested_customer_name = nested_customer.get("name") if isinstance(nested_customer, dict) else None
        customer_first_name = extract_first_name(
            target_data.get("customer_name")
            or target_data.get("contact_name")
            or nested_customer_name
        )

    if not items:
        return "jee aapki cart khaali kardi hai, koi nai items add karoon?"

    out = []
    out.append("Jee theek bhai,\n")
    out.append("Aap ke order main yeh cheezein shamil hain:")

    total_savings = 0.0
    computed_total = 0.0

    for idx, item in enumerate(items, 1):
        raw_name = item.get("name")
        name = raw_name if (isinstance(raw_name, str) and raw_name.strip() and raw_name.strip().lower() != "item") else None
        if _is_sku_like(name):
            name = None
        name = name or "item"
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

        try:
            logger.info(
                "order_draft_template.render_line",
                idx=idx,
                sku=item.get("sku_code") or item.get("sku") or item.get("skucode"),
                name=name,
                qty=qty,
                base_price=base_price,
                final_price=final_price,
                line_total=line_total,
            )
        except Exception:
            pass

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
    #             f"Isko bech kar aap ki sale taqreeban *{total_sell} Rs* hogi, "
    #             f"aur is order pe aap ka munafa roughly *{profit} Rs* (~{margin_pct:.1f}% margin) banega."
    #         )
    #     else:
    #         out.append(
    #             f"Isko bech kar aap ki sale taqreeban *{total_sell} Rs* hogi, "
    #             f"aur is order pe aap ka munafa roughly *{profit} Rs* banega."
    #         )
    # elif margin_pct is not None:
    #     out.append(f"Is order pe approx margin ~{margin_pct:.1f}% aa raha hai jee.")

    # Backfill footer totals from rendered line items when API totals are partial/missing.
    if grand_total_breakdown is None and total_val is not None:
        grand_total_breakdown = round(float(total_val), 2)
    if discount_breakdown is None and total_savings > 0:
        discount_breakdown = round(float(total_savings), 2)
    if subtotal_breakdown is None:
        if old_total_for_display is not None:
            subtotal_breakdown = round(float(old_total_for_display), 2)
        elif grand_total_breakdown is not None and discount_breakdown is not None:
            subtotal_breakdown = round(float(grand_total_breakdown) + float(discount_breakdown), 2)
        elif grand_total_breakdown is not None:
            subtotal_breakdown = round(float(grand_total_breakdown), 2)
    if discount_breakdown is None and subtotal_breakdown is not None and grand_total_breakdown is not None:
        discount_breakdown = round(
            max(float(subtotal_breakdown) - float(grand_total_breakdown), 0.0),
            2,
        )
    if total_breakdown is None and grand_total_breakdown is not None:
        total_breakdown = round(float(grand_total_breakdown), 2)
    # if profit_breakdown is None and profit is not None:
    #     profit_breakdown = _coerce_float(profit)
    # if profit_margin_pct_breakdown is None and margin_pct is not None:
    #     profit_margin_pct_breakdown = _coerce_float(margin_pct)

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

    if customer_first_name:
        follow_up = f"Kya main confirm kar doon {customer_first_name} bhai?"
    else:
        follow_up = "Kya main confirm kar doon bhai?"
    main_body = "\n".join(out).strip()
    if follow_up:
        return MULTI_MESSAGE_DELIMITER.join([part for part in (main_body, follow_up) if part])
    return main_body


def vn_order_draft_template(args: dict) -> str:
    """
    Short, voice-note friendly version of the current order draft.
    - Roman Urdu Spoken style.   
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
        return "Jee bhai, abhi mere paas koi order bana hua nahi hai."

    parts = []

    customer_name = extract_first_name(args.get("customer_name"))
    parts.append("Jee bhai,")

    parts.append("aap ke order main yeh items hain,")

    for line in lines:
        raw_name = line.get("name")
        name = raw_name if (isinstance(raw_name, str) and raw_name.strip() and raw_name.strip().lower() != "item") else None
        if _is_sku_like(name):
            name = None
        name = name or "item"
        qty = line.get("qty", 0)
        line_total = line.get("line_total")
        qty_str = str(qty)
        unit_word = "boxes"

        if line_total is not None:
            parts.append(f"{name}, total {qty_str} {unit_word}, {line_total} rupay")
        else:
            parts.append(f"{name}, total {qty_str} {unit_word}")

    if total is not None:
        parts.append(f"total ban raha hai {total} rupay.")

    # Profit line for VN
    total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total_amount")
    if total_sell is None:
        total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total")
    if total_sell is not None and profit is not None:
        parts.append(
            f"yeh sab bech kar aap ki total sale {total_sell} rupay hogi, "
            f"aur is pe aap ka munafa taqreeban {profit} rupay banega."
        )
    else:
        parts.append(
            "aap ka profit bhi acha banega InshaAllah."
        )

    if customer_name:
        parts.append(f"Kya main confirm kar doon {customer_name} bhai?")
    else:
        parts.append("Kya main confirm kar doon bhai?")

    return " ".join(p.strip() for p in parts if p and str(p).strip())


# 6) Order Status → Invoice
def invoice_status_template(args: dict) -> str:
    """
    Invoice / order status summary in Roman Urdu.
    """
    company_name = _resolve_company_name(args)
    count = args.get("count")
    total = args.get("total")
    old_total = args.get("old_total")
    suggest_topup = args.get("suggest_topup", False)
    topup_gap = args.get("topup_gap")

    if not count or total is None:
        return "Bhai, order status abhi clear nahi ho raha... main check kar ke batati hoon."

    lines = []
    lines.append(f"Jee bhai, aap ke {company_name} order ka status yeh hai:")

    lines.append(f"• items: {count}")

    if old_total:
        lines.append(f"• pehli value: ~{old_total} Rs~")

    lines.append(f"• final total: *{total} Rs*")

    # Profit / margin overview
    total_sell, profit, margin_pct = _compute_profit_fields(args, total_key="total")
    if total_sell is not None and profit is not None:
        if margin_pct is not None:
            lines.append(
                f"• Is maal ki sale taqreeban *{total_sell} Rs* ki hogi, "
                f"aur aap ka munafa roughly *{profit} Rs* aur (~{margin_pct:.1f}% margin) hoga."
            )
        else:
            lines.append(
                f"• Is maal ki sale taqreeban *{total_sell} Rs* ki hogi, "
                f"aur aap ka munafa roughly *{profit} Rs* hoga."
            )

    if suggest_topup and topup_gap:
        lines.append(
            f"Agar aap taqreeban {topup_gap} Rs ki item aur daal lein "
            f"to mix behtar ho jayega... kya main koi choti SKU dikhaoon?"
        )

    lines.append("Kya main invoice bhej doon jee, ya kuch aur add karna hai?")

    return "\n".join(lines).strip()


# 7) Reorder (Repeat Last Order)
def reorder_template(args: dict) -> str:
    """
    Shows last order for quick repeat in Roman Urdu.
    """
    company_name = _resolve_company_name(args)
    last_order = args.get("last_order") or {}
    lines_data = last_order.get("lines") or []

    if not lines_data:
        return f"Bhai, abhi system main mujhe pichla {company_name} order theek se nahi mil raha."

    out = []
    out.append(f"Jee bhai, aap ka last {company_name} order kuch aisa tha:")

    for idx, line in enumerate(lines_data, 1):
        name = line.get("name", "item")
        qty = line.get("qty", 0)
        out.append(f"{idx}) *{name}* × {qty}")

    out.append("")
    out.append("Kya same repeat kar doon jee, ya kuch change karna hai?")

    return "\n".join(out).strip()


# 8) Bundles / Combos (simple, no-arg)
def bundles_template():
    """
    Static-ish bundles message in Roman Urdu.
    """
    company_name = _resolve_company_name()
    intro_options = [
        f"Dekhein bhai, {company_name} ke kuch combos bahut achay chal rahe hain:",
        "Jee bhai, kya main wo combo ideas bataoon jin se dukaan ki sale barhti hai:",
    ]
    reason_lines = [
        "margin acha hai",
        "bachay aur families dono pasand karte hain",
        "shelf pe rakhte hi bik jata hai",
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
    out.append("Batain bhai, in main se kaunsa combo lock karoon?")

    return "\n".join(out).strip()


# 9) Product Info
def product_info_template(args: dict) -> str:
    """
    Product info for SKUs in Roman Urdu.
    """
    sku = args.get("sku") or {}
    highlights = args.get("highlights") or []
    promo_hint = args.get("promo_hint", False)

    name = sku.get("name", "item")
    variant = sku.get("variant", "")

    out = [f"Jee bhai, *{name}* ki kuch details yeh hain:"]

    for point in highlights[:3]:
        out.append(f"• {point}")

    if variant:
        out.append(f"• pack size: *{variant}*")

    if promo_hint:
        out.append("• Is waqt is pe achi deal bhi chal rahi hai... kya main details bataoon?")

    out.append("Kya main isko order draft main add kar doon bhai?")

    return "\n".join(out).strip()


# 10) Top Sellers / Upsell
def recommendations_template(args: dict) -> str:
    """
    Top sellers / upsell list in Roman Urdu.
    """
    company_name = _resolve_company_name(args)
    intro_pool = [
        f"Bhai, aap ke area main yeh {company_name} items sab se zyada bik rahi hain:",
        "Yahan ke top-seller biscuits aur cakes yeh hain:",
        "Dekhein bhai, yeh SKUs yahan bahut tez move ho rahe hain:",
    ]

    cta_pool = [
        "In main se kaunsi order main daaloon",
        "Batain bhai, kaunsi add karni hain",
        "Aaj kin 2-3 SKUs se start karein",
    ]

    intro_line = random.choice(intro_pool)
    cta_line = random.choice(cta_pool)

    products = args.get("area_top_sellers") or []

    if not products:
        return (
            "Bhai, area ki best sellers list abhi refresh ho rahi hai... "
            "Main general top deals bata sakti hoon, dekhna chahenge?"
        )

    out = [intro_line]

    for i, item in enumerate(products[:5], 1):
        name = item.get("name", "item")
        price = item.get("price")
        note = item.get("note")

        if price is not None:
            price_line = f"*{price} Rs*"
        else:
            price_line = "(price system se nahi aa raha)"

        line = f"{i}) *{name}* — {price_line}"
        if note:
            line += f"\n   👉 _{note}_"

        out.append(line)

    out.append("")
    out.append(cta_line + "?")

    return "\n".join(out).strip()


# 11) Seasonal Advice (simple, no-arg)
def seasonal_advice_template():
    """
    Simple seasonal nudge in Roman Urdu.
    """
    company_name = _resolve_company_name()
    season_lines = [
        "Ramzan aur Eid ke dino main biscuits aur cakes ki demand tez ho jati hai bhai",
        f"Sardiyon ke season main chai ke sath {company_name} ki sale naturally barh jati hai",
        "School season main snack pack ki rotation bahut fast hoti hai",
    ]
    cta_lines = [
        f"Agar aap abhi se thora extra {company_name} ka stock rakh lein to safe rahenge",
        "Agar aap chahen to main 3-4 seasonal SKUs bata sakti hoon jo tez bikengi",
    ]

    season_line = random.choice(season_lines)
    cta_line = random.choice(cta_lines)

    out = []
    out.append(f"{season_line}.")
    out.append("Best stock ke liye kuch Recommned kr don?")
    out.append(cta_line + "?")

    return "\n".join(out).strip()


def current_total_reminder(total: float, casual: bool = True) -> str:
    """
    Quick reminder of current order total in Roman Urdu.
    """
    if casual:
        phrases = [
            f"Abhi aap ka order {total} Rs pe hai",
            f"Current total {total} Rs ban raha hai bhai",
            f"Order abhi tak {total} Rs ka hua hai",
        ]
        return random.choice(phrases)
    else:
        return f"Current order total: {total} Rs"


# 12) Objection Handling (simple, no-arg)
def objection_handling_template():
    """
    Generic objection handling lines for Ayesha, Roman Urdu.
    """
    blocks = []

    company_name = _resolve_company_name()

    # Price objection
    blocks.append(
        "Samajh sakti hoon bhai, budget kabhi kabhi tight hota hai.\n"
        "Magar jab deal lagti hai to scene aisa hota hai:\n"
        "pehle purana rate hota hai... phir promo ka naya rate aata hai, "
        "aur beech ka farq aap ki bachat hoti hai.\n"
        "Is rate pe khareedna aksar faida deta hai, agar aap chota trial rakh lein to risk bhi kam hai."
    )

    # Dont need more
    blocks.append(
        "Agar aap ko lagta hai stock kafi hai, to theek hai jee.\n"
        f"Bas yeh sochen ke agar aas paas ki dukanon ne fresh {company_name} stock utha liya "
        "to customer wahan shift ho sakta hai.\n"
        "Agar aap sirf ek do carton fast moving SKUs ke rakh lein, to shelf bhara lagega aur sale safe rahegi."
    )

    # Customers won't buy
    blocks.append(
        "Agar aap ko lagta hai customer nahi uthayega, to meri suggestion hai ke sirf chota trial rakh lein... "
        "ek do carton ya kuch boxes.\n"
        "Agar bik gaya to agli baar quantity barha denge, warna aap ko area ke taste ka idea ho jayega."
    )

    blocks.append("Main force nahi kar rahi bhai, sirf options bata rahi hoon... faisla aap ka apna hai.")

    return "\n\n".join(blocks).strip()


def personalized_greeting_template(args: dict) -> str:
    """
    Personalized greeting AFTER sending catalog.
    Roman Urdu.
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
        or "bhai"
    )
    customer_name = extract_first_name(customer_name) or "bhai"

    store_name = (
        args.get("store_name")
        or args.get("outlet_name")
        or args.get("shop_name")
        or ""
    )

    catalog_sent = args.get("catalog_sent", True)

    # Line 1 – salam + name (+ optional shop)
    if store_name:
        line1 = f"Assalam o Alaikum {customer_name} bhai ({store_name}),"
    else:
        line1 = f"Assalam o Alaikum {customer_name} bhai,"

    # Line 2 – identity
    line2 = f"Main Ayesha baat kar rahi hoon {company_name} se."

    # Line 3 – catalog status
    if catalog_sent:
        line3 = "Catalogue bhej diya hai jee, aap fursat main dekh lein."
    else:
        line3 = f"{company_name} ka catalogue aap ke liye open kar rahi hoon."

    # Line 4 – next step
    line4 = "Batain bhai, pehle kya dekhna chahengy??"

    return "\n".join([line1, line2, line3, line4])
