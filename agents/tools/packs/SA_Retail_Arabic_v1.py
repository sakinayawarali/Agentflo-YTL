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


# 1) Greeting / General Queries
def greeting_template(
    user_message: Optional[str] = None,
    customer_name: Optional[str] = None,
):
    """
    Warm, street-smart greeting for the retailer.
    Arabic version, Aisha persona.
    always call send_product_catalogue tool after running this template.
    """
    company_name = _resolve_company_name()
    greeting_lines = [
        "السلام عليكم يا أخي",
        "أهلاً وسهلاً",
        "السلام عليكم",
    ]
    salam_lines = [
        "وعليكم السلام يا أخي",
        "وعليكم السلام",
        "وعليكم السلام ورحمة الله",
    ]
    hello_lines = [
        "أهلاً وسهلاً",
        "مرحبا يا أخي",
        "أهلاً بك",
    ]
    smalltalk_lines = [
        "كيف الشغل",
        "كيف وضع المحل هذه الأيام",
        "كيف مبيعات البسكويت",
    ]
    value_hooks = [
        f"أكثر منتجات {company_name} مبيعاً",
        f"مخزون {company_name} الجديد",
        f"أفضل تشكيلة من بسكويت وكعك {company_name}",
        "المنتجات السريعة الحركة في منطقتك",
    ]
    help_options = [
        "هل أساعدك في تجهيز الطلب",
        "هل أشاركك أفضل المنتجات اليوم",
        "هل أخبرك عن الأصناف الجديدة",
        "هل نراجع أفضل العروض اليوم",
    ]
    cta_lines = [
        "قل لي ماذا تحب أن تراجع اليوم",
        "بأي صنف نبدأ اليوم",
        "ما هو أول منتج تود رؤيته",
    ]

    first_name = extract_first_name(customer_name)
    if first_name:
        greeting_line = f"مرحبا {first_name} يا أخي"
    else:
        greeting_line = _smart_greeting_line(user_message, salam_lines, hello_lines, greeting_lines)
    smalltalk_line = random.choice(smalltalk_lines)
    value_hook = random.choice(value_hooks)
    help_line = random.choice(help_options)
    cta_line = random.choice(cta_lines)

    final_response = (
        f"{greeting_line},\n"
        f"{smalltalk_line}؟\n"
        f"أنا عائشة من {company_name}... {value_hook} متوفرة.\n"
        f"{help_line}؟\n"
        f"{cta_line}؟"
    )

    return f"{final_response.strip()}"


# 2) Manual Order Flow
def manual_order_template(args: dict) -> str:
    """
    Manual ordering helper:
    - Confirms SKU
    - Shows close matches if multiple
    - Mentions totals / promos using Rs.
    - Always in Arabic
    """

    output = []
    company_name = _resolve_company_name(args)

    # Confirm main SKU
    sku = args.get("sku") or {}
    if sku:
        name = sku.get("name", "منتج")
        price = sku.get("price")
        qty = sku.get("qty") or 1

        output.append("تمام يا أخي، هذا المنتج موجود:")
        sku_lines, _ = format_sku_price_block(name, qty, price, price)
        output.extend(sku_lines)

        output.append("هل أؤكد هذا المنتج؟")

    # Multiple matches
    matches = args.get("multiple_matches") or []
    if matches:
        if not sku:
            output.append("شوف يا أخي، هذه خيارات مشابهة في النظام:")
        else:
            output.append("معه ظهرت هذه الخيارات أيضاً:")

        for idx, m in enumerate(matches, 1):
            m_name = m.get("name", "منتج")
            m_price = m.get("price")

            line_lines, _ = format_sku_price_block(m_name, 1, m_price, m_price, index=idx)
            output.extend(line_lines)

        output.append("قل لي أي خيار أعتمده لك؟")

    # Quantity ask for confirmed sku
    if sku:
        name = sku.get("name", "منتج")
        price = sku.get("price")
        if price is not None:
            output.append(f"تمام، سعر *{name}* هو *Rs {price}*.")
        else:
            output.append(f"تمام، تم اختيار *{name}*.")

        output.append("كم كرتون / باك تريد أن أضيف؟")

    # Promo already applied
    promo_applied = args.get("promo_applied")
    if promo_applied:
        old_total = promo_applied.get("old_total")
        new_total = promo_applied.get("new_total")
        savings = promo_applied.get("savings")
        output.append("")
        output.append("يا أخي، عليه عرض ممتاز:")
        if old_total is not None and new_total is not None:
            output.append(f"قبل كان ~Rs {old_total}~ والآن صار *Rs {new_total}*.")
        if savings is not None:
            output.append(f"يعني التوفير *Rs {savings}*.")
        output.append("عرض جيد... أؤكد بهذا السعر؟")

    # Promo needs more quantity
    promo_needs_more = args.get("promo_needs_more")
    if promo_needs_more:
        min_qty = promo_needs_more.get("min_qty")
        old_bulk_total = promo_needs_more.get("old_bulk_total")
        new_bulk_total = promo_needs_more.get("new_bulk_total")
        save_bulk = promo_needs_more.get("save_bulk")

        output.append("")
        if min_qty:
            output.append(f"يا أخي، إذا أخذت كمية {min_qty} يكون العرض أفضل:")
        if old_bulk_total is not None and new_bulk_total is not None:
            output.append(f"الإجمالي بدل ~Rs {old_bulk_total}~ يصير *Rs {new_bulk_total}*.")
        if save_bulk is not None:
            output.append(f"يعني توفير إضافي *Rs {save_bulk}*.")
        output.append("أزيد لك الكمية؟")

    # Simple total info (no promo)
    no_promo = args.get("no_promo")
    if no_promo:
        total = no_promo.get("total")
        if total is not None:
            output.append(f"تمام، المجموع الحالي *Rs {total}*.")
        output.append(f"لو تحب، أقدر أقترح لك أصناف {company_name} إضافية لربح أفضل؟")

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
    Formats the current order draft like natural WhatsApp Aisha style.
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
        return f"يا أخي، مسودة الطلب فاضية... أضيف لك أي صنف من {company_name}؟"

    out = []
    out.append("تمام يا أخي,\n")
    out.append("هذه هي الأصناف الموجودة في طلبك:")

    total_savings = 0.0
    computed_total = 0.0

    for idx, item in enumerate(items, 1):
        name = item.get("name", "منتج")
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
    #             f"إذا بعت هذا، المبيعات تقريباً *{total_sell} Rs*، "
    #             f"والربح تقريباً *{profit} Rs* (~{margin_pct:.1f}% هامش)."
    #         )
    #     else:
    #         out.append(
    #             f"إذا بعت هذا، المبيعات تقريباً *{total_sell} Rs*، "
    #             f"والربح تقريباً *{profit} Rs*."
    #         )
    # elif margin_pct is not None:
    #     out.append(f"هامش الربح تقريباً ~{margin_pct:.1f}%. ")

    if show_breakdown:
        out.append("-----------------------------")
        if subtotal_breakdown is not None:
            out.append(f"المجموع الفرعي: Rs {subtotal_breakdown:,.2f}")
        if discount_breakdown is not None:
            out.append(f"إجمالي الخصم: Rs {discount_breakdown:,.2f}")
        # if total_breakdown is not None:
        #     out.append(f"الإجمالي:      Rs {total_breakdown:,.2f}")
        if grand_total_breakdown is not None:
            out.append("الإجمالي النهائي (incl. GST):")
            out.append(f"Rs {grand_total_breakdown:,.2f}")
        # if profit_breakdown is not None:
        #     out.append(f"الربح:         Rs {profit_breakdown:,.2f}")
        # # if profit_total_breakdown is not None:
        # #     out.append(f"إجمالي الربح:  Rs {profit_total_breakdown:,.2f}")
        # margin_display = (
        #     profit_margin_pct_breakdown
        #     if profit_margin_pct_breakdown is not None
        #     else profit_margin_breakdown
        # )
        # if margin_display is not None:
        #     out.append(f"هامش الربح:    {margin_display:.2f}%")
        out.append("-----------------------------")

    follow_up = "هل أؤكد لك الطلب يا أخي؟"
    main_body = "\n".join(out).strip()
    if follow_up:
        return MULTI_MESSAGE_DELIMITER.join([part for part in (main_body, follow_up) if part])
    return main_body


def vn_order_draft_template(args: dict) -> str:
    """
    Short, voice-note friendly version of the current order draft.
    - Arabic spoken style.
    - Says quantity as: "total {qty} packets"
    - Keeps all amounts exactly as provided (no rounding)
    - Designed to be sent as text *and* converted to audio.
    - If total_sell / profit are provided on the draft, we also speak out expected sale
      and profit in simple words.
    """
    draft = args.get("draft") or {}
    lines = draft.get("items") or draft.get("lines") or []
    total = draft.get("total_amount") or draft.get("total")

    if not lines:
        return "يا أخي، ليس لدي أي طلب حالياً."

    parts = []

    customer_name = extract_first_name(args.get("customer_name"))
    if customer_name:
        parts.append(f"تمام {customer_name}،")
    else:
        parts.append("تمام يا أخي،")

    parts.append("في طلبك هذه الأصناف،")

    for line in lines:
        name = line.get("name", "منتج")
        qty = line.get("qty", 0)
        line_total = line.get("line_total")
        qty_str = str(qty)
        unit_word = "باك"

        if line_total is not None:
            parts.append(f"{name}، الإجمالي {qty_str} {unit_word}، {line_total} روبية")
        else:
            parts.append(f"{name}، الإجمالي {qty_str} {unit_word}")

    if total is not None:
        parts.append(f"الإجمالي صار {total} روبية.")

    # Profit line for VN
    total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total_amount")
    if total_sell is None:
        total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total")
    if total_sell is not None and profit is not None:
        parts.append(
            f"لو تبيع هذا كله، المبيعات ستكون {total_sell} روبية، "
            f"والربح تقريباً {profit} روبية."
        )
    else:
        parts.append("الربح إن شاء الله سيكون جيداً.")

    parts.append("هل أؤكد الطلب يا أخي؟")

    return " ".join(p.strip() for p in parts if p and str(p).strip())


# 6) Order Status -> Invoice
def invoice_status_template(args: dict) -> str:
    """
    Invoice / order status summary in Arabic.
    """
    company_name = _resolve_company_name(args)
    count = args.get("count")
    total = args.get("total")
    old_total = args.get("old_total")
    suggest_topup = args.get("suggest_topup", False)
    topup_gap = args.get("topup_gap")

    if not count or total is None:
        return "يا أخي، حالة الطلب غير واضحة الآن... سأراجع وأخبرك."

    lines = []
    lines.append(f"يا أخي، حالة طلب {company_name} كالتالي:")

    lines.append(f"• عدد الأصناف: {count}")

    if old_total:
        lines.append(f"• القيمة السابقة: ~Rs {old_total}~")

    lines.append("• الإجمالي النهائي (incl. GST):")
    lines.append(f"  *Rs {total}*")

    # Profit / margin overview
    total_sell, profit, margin_pct = _compute_profit_fields(args, total_key="total")
    if total_sell is not None and profit is not None:
        if margin_pct is not None:
            lines.append(
                f"• لو تبيع هذا، المبيعات تقريباً *Rs {total_sell}*، "
                f"والربح تقريباً *Rs {profit}* (~{margin_pct:.1f}% هامش)."
            )
        else:
            lines.append(
                f"• لو تبيع هذا، المبيعات تقريباً *Rs {total_sell}*، "
                f"والربح تقريباً *Rs {profit}*."
            )

    if suggest_topup and topup_gap:
        lines.append(
            f"إذا أضفت تقريباً Rs {topup_gap} من الأصناف، المزيج سيصبح أفضل... "
            "هل أعرض لك صنف صغير؟"
        )

    lines.append("هل أرسل الفاتورة يا أخي، أم تريد إضافة شيء؟")

    return "\n".join(lines).strip()


# 7) Reorder (Repeat Last Order)
def reorder_template(args: dict) -> str:
    """
    Shows last order for quick repeat in Arabic.
    """
    company_name = _resolve_company_name(args)
    last_order = args.get("last_order") or {}
    lines_data = last_order.get("lines") or []

    if not lines_data:
        return f"يا أخي، ما لقيت آخر طلب {company_name} بشكل واضح."

    out = []
    out.append(f"يا أخي، آخر طلب {company_name} كان هكذا:")

    for idx, line in enumerate(lines_data, 1):
        name = line.get("name", "منتج")
        qty = line.get("qty", 0)
        out.append(f"{idx}) *{name}* × {qty}")

    out.append("")
    out.append("هل أكرر نفس الطلب أم تريد تعديل؟")

    return "\n".join(out).strip()


# 8) Bundles / Combos (simple, no-arg)
def bundles_template():
    """
    Static-ish bundles message in Arabic.
    """
    company_name = _resolve_company_name()
    intro_options = [
        f"شوف يا أخي، بعض كومبوهات {company_name} ماشية جداً:",
        "يا أخي، تحب أشاركك أفكار كومبو تزيد مبيعات المحل:",
    ]
    reason_lines = [
        "هامش ربح جيد",
        "الأطفال والعائلات يحبونه",
        "يمشي بسرعة على الرف",
    ]

    intro = random.choice(intro_options)

    bundles = [
        {
            "title": "كومبو المساء",
            "line": random.choice(reason_lines),
        },
        {
            "title": "كومبو الأطفال",
            "line": random.choice(reason_lines),
        },
    ]

    out = [intro]
    for b in bundles:
        out.append(f"• *{b['title']}*")
        out.append(f"  👉 {b['line']}")

    out.append("")
    out.append("أي كومبو تحب أعتمده لك؟")

    return "\n".join(out).strip()


# 9) Product Info
def product_info_template(args: dict) -> str:
    """
    Product info for SKUs in Arabic.
    """
    sku = args.get("sku") or {}
    highlights = args.get("highlights") or []
    promo_hint = args.get("promo_hint", False)

    name = sku.get("name", "منتج")
    variant = sku.get("variant", "")

    out = [f"يا أخي، هذه بعض تفاصيل *{name}*:"]

    for point in highlights[:3]:
        out.append(f"• {point}")

    if variant:
        out.append(f"• حجم العبوة: *{variant}*")

    if promo_hint:
        out.append("• حالياً عليه عرض جيد... تحب التفاصيل؟")

    out.append("هل أضيفه إلى مسودة الطلب؟")

    return "\n".join(out).strip()


# 10) Top Sellers / Upsell
def recommendations_template(args: dict) -> str:
    """
    Top sellers / upsell list in Arabic.
    """
    company_name = _resolve_company_name(args)
    intro_pool = [
        f"يا أخي، هذه أصناف {company_name} الأسرع مبيعاً في منطقتك:",
        "هذه أفضل البسكويت والكيك مبيعاً قربك:",
        "هذه الأصناف تتحرك بسرعة كبيرة في منطقتك:",
    ]

    cta_pool = [
        "أيها تريد أن أضيف للطلب",
        "قل لي أيها أضيف",
        "نبدأ بأي 2-3 أصناف",
    ]

    intro_line = random.choice(intro_pool)
    cta_line = random.choice(cta_pool)

    products = args.get("area_top_sellers") or []

    if not products:
        return (
            "قائمة الأفضل مبيعاً يتم تحديثها... "
            "أقدر أشاركك أفضل العروض العامة الآن، تحب؟"
        )

    out = [intro_line]

    for i, item in enumerate(products[:5], 1):
        name = item.get("name", "منتج")
        price = item.get("price")
        note = item.get("note")

        if price is not None:
            price_line = f"*{price} Rs*"
        else:
            price_line = "(السعر غير متوفر)"

        line = f"{i}) *{name}* — {price_line}"
        if note:
            line += f"\n   👉 _{note}_"

        out.append(line)

    out.append("")
    out.append(cta_line + "؟")

    return "\n".join(out).strip()


# 11) Seasonal Advice (simple, no-arg)
def seasonal_advice_template():
    """
    Simple seasonal nudge in Arabic.
    """
    company_name = _resolve_company_name()
    season_lines = [
        "في رمضان والعيد، الطلب على البسكويت والكيك يزيد بسرعة",
        f"في موسم الشتاء، مبيعات {company_name} مع الشاي تزيد طبيعي",
        "في موسم المدارس، حركة باك السناك تكون سريعة جداً",
    ]
    cta_lines = [
        f"إذا وفرت كمية إضافية بسيطة من {company_name} الآن تكون أأمن",
        "إذا تحب، أقدر أشاركك 3-4 أصناف موسمية تتحرك بسرعة",
    ]

    season_line = random.choice(season_lines)
    cta_line = random.choice(cta_lines)

    out = []
    out.append(f"{season_line}.")
    out.append("لأفضل مخزون، هل تحب توصيات؟")
    out.append(cta_line + "؟")

    return "\n".join(out).strip()


def current_total_reminder(total: float, casual: bool = True) -> str:
    """
    Quick reminder of current order total in Arabic.
    """
    if casual:
        phrases = [
            f"طلبك حالياً عند {total} Rs",
            f"الإجمالي الحالي {total} Rs",
            f"حتى الآن الإجمالي {total} Rs",
        ]
        return random.choice(phrases)
    else:
        return f"إجمالي الطلب الحالي: {total} Rs"


# 12) Objection Handling (simple, no-arg)
def objection_handling_template():
    """
    Generic objection handling lines for Aisha, Arabic.
    """
    blocks = []

    company_name = _resolve_company_name()

    # Price objection
    blocks.append(
        "أتفهم يا أخي، أحياناً الميزانية تكون ضيقة.\n"
        "لكن لما يكون في عرض، القصة تصير كذا:\n"
        "أولاً السعر القديم... بعدين سعر العرض، والفرق هذا هو التوفير.\n"
        "الشراء بهذا السعر غالباً مفيد، وإذا أخذت كمية صغيرة فالمخاطرة أقل."
    )

    # Dont need more
    blocks.append(
        "إذا تحس المخزون كافي، تمام.\n"
        f"لكن إذا المحلات القريبة أخذت مخزون {company_name} جديد، ممكن الزبون يتحول لهم.\n"
        "لو تحتفظ بكرتون أو اثنين من الأصناف السريعة، الرف يصير ممتلئ والبيع يكون أأمن."
    )

    # Customers won't buy
    blocks.append(
        "إذا تخاف ما يمشي مع الزبائن، اقتراحي تجربة صغيرة... "
        "كرتونين أو بعض الباكات.\n"
        "إذا تحرك، نزيد الكمية المرة الجاية، وإذا ما تحرك تعرف ذوق المنطقة."
    )

    blocks.append("أنا ما أضغط عليك يا أخي، فقط أعطيك خيارات... القرار لك.")

    return "\n\n".join(blocks).strip()


def personalized_greeting_template(args: dict) -> str:
    """
    Personalized greeting AFTER sending catalog.
    Arabic.
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
        or "أخي"
    )
    customer_name = extract_first_name(customer_name) or "أخي"

    store_name = (
        args.get("store_name")
        or args.get("outlet_name")
        or args.get("shop_name")
        or ""
    )

    catalog_sent = args.get("catalog_sent", True)

    # Line 1 – salam + name (+ optional shop)
    if store_name:
        line1 = f"السلام عليكم {customer_name} ({store_name})،"
    else:
        line1 = f"السلام عليكم {customer_name}،"

    # Line 2 – identity
    line2 = f"أنا عائشة من {company_name}."

    # Line 3 – catalog status
    if catalog_sent:
        line3 = "تم إرسال الكتالوج، شوفه وقت ما تحب."
    else:
        line3 = f"أفتح كتالوج {company_name} لك الآن."

    # Line 4 – next step
    line4 = "قل لي، ماذا تحب أن ترى أولاً؟"

    return "\n".join([line1, line2, line3, line4])
