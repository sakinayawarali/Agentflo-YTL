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
    Bahasa Melayu version, Aishah persona.
    always call send_product_catalogue tool after running this template.
    """
    company_name = _resolve_company_name()
    greeting_lines = [
        "Assalamualaikum Boss",
        "Salam Boss",
        "Assalamualaikum",
    ]
    salam_lines = [
        "Waalaikumsalam Boss",
        "Waalaikumussalam Boss",
    ]
    hello_lines = [
        "Hello Boss",
        "Hai Boss",
    ]
    smalltalk_lines = [
        "macam mana bisnes sekarang",
        "kedai macam mana kebelakangan ni",
        "jualan biskut macam mana",
    ]
    value_hooks = [
        f"top seller {company_name}",
        f"stok baru {company_name}",
        f"range biskut dan kek {company_name} yang best",
        "barang laju di kawasan Boss",
    ]
    help_options = [
        "nak saya bantu buat order",
        "nak saya kongsi top seller hari ni",
        "nak saya tunjuk item baru",
        "nak check best deal hari ni",
    ]
    cta_lines = [
        "Boss, nak check apa dulu",
        "Kita mula dengan item mana hari ni",
        "Boss, produk mana nak tengok dulu",
    ]

    first_name = extract_first_name(customer_name)
    if first_name:
        greeting_line = f"Hi {first_name} Boss"
    else:
        greeting_line = _smart_greeting_line(user_message, salam_lines, hello_lines, greeting_lines)
    smalltalk_line = random.choice(smalltalk_lines)
    value_hook = random.choice(value_hooks)
    help_line = random.choice(help_options)
    cta_line = random.choice(cta_lines)

    final_response = (
        f"{greeting_line},\n"
        f"{smalltalk_line}?\n"
        f"Saya Aishah dari {company_name}... {value_hook} ada.\n"
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
    - Mentions totals / promos using RM.
    - Always in Bahasa Melayu
    """

    output = []
    company_name = _resolve_company_name(args)

    # Confirm main SKU
    sku = args.get("sku") or {}
    if sku:
        name = sku.get("name", "item")
        price = sku.get("price")
        qty = sku.get("qty") or 1

        output.append("Baik Boss, item ni saya jumpa:")
        sku_lines, _ = format_sku_price_block(name, qty, price, price)
        output.extend(sku_lines)

        output.append("Betul ke nak confirm item ni?")

    # Multiple matches
    matches = args.get("multiple_matches") or []
    if matches:
        if not sku:
            output.append("Boss, ini antara pilihan yang hampir sama dalam sistem:")
        else:
            output.append("Sekali ni ada pilihan lain juga:")

        for idx, m in enumerate(matches, 1):
            m_name = m.get("name", "item")
            m_price = m.get("price")

            line_lines, _ = format_sku_price_block(m_name, 1, m_price, m_price, index=idx)
            output.extend(line_lines)

        output.append("Boss, yang mana satu nak saya finalize?")

    # Quantity ask for confirmed sku
    if sku:
        name = sku.get("name", "item")
        price = sku.get("price")
        if price is not None:
            output.append(f"Baik, harga *{name}* ialah *RM {price}*.")
        else:
            output.append(f"Baik, *{name}* dah dipilih.")

        output.append("Berapa karton / pack Boss nak?")

    # Promo already applied
    promo_applied = args.get("promo_applied")
    if promo_applied:
        old_total = promo_applied.get("old_total")
        new_total = promo_applied.get("new_total")
        savings = promo_applied.get("savings")
        output.append("")
        output.append("Boss, item ni ada offer bagus:")
        if old_total is not None and new_total is not None:
            output.append(f"Dulu ~RM {old_total}~, sekarang jadi *RM {new_total}*.")
        if savings is not None:
            output.append(f"Maksudnya Boss jimat *RM {savings}*.")
        output.append("Bagus ni Boss... nak confirm harga ni?")

    # Promo needs more quantity
    promo_needs_more = args.get("promo_needs_more")
    if promo_needs_more:
        min_qty = promo_needs_more.get("min_qty")
        old_bulk_total = promo_needs_more.get("old_bulk_total")
        new_bulk_total = promo_needs_more.get("new_bulk_total")
        save_bulk = promo_needs_more.get("save_bulk")

        output.append("")
        if min_qty:
            output.append(f"Boss, kalau ambil {min_qty} kuantiti, deal lagi best:")
        if old_bulk_total is not None and new_bulk_total is not None:
            output.append(f"Total dari ~RM {old_bulk_total}~ jadi *RM {new_bulk_total}*.")
        if save_bulk is not None:
            output.append(f"Jimat extra *RM {save_bulk}*.")
        output.append("Nak saya tambah kuantiti?")

    # Simple total info (no promo)
    no_promo = args.get("no_promo")
    if no_promo:
        total = no_promo.get("total")
        if total is not None:
            output.append(f"Baik, sekarang total *RM {total}*.")
        output.append(f"Kalau Boss nak, saya boleh cadang item {company_name} lain untuk margin lebih?")

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
    Formats the current order draft like natural WhatsApp Aishah style.
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
        return f"Boss, order draft kosong... nak saya tambah item {company_name}?"

    out = []
    out.append("Baik Boss,\n")
    out.append("Dalam order Boss ada item berikut:")

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
    #         out.append(f"Total ~{old_total_for_display:,.2f} RM~  *{total_val:,.2f} RM*")
    #     else:
    #         out.append(f"Total:  *{total_val:,.2f} RM*")
    # elif total is not None:
    #     out.append(f"Total:  *{total} RM*")

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
    #             f"Jika dijual, sales sekitar *{total_sell} RM*, "
    #             f"dan untung lebih kurang *{profit} RM* (~{margin_pct:.1f}% margin)."
    #         )
    #     else:
    #         out.append(
    #             f"Jika dijual, sales sekitar *{total_sell} RM*, "
    #             f"dan untung lebih kurang *{profit} RM*."
    #         )
    # elif margin_pct is not None:
    #     out.append(f"Margin anggaran ~{margin_pct:.1f}%. ")

    if show_breakdown:
        out.append("-----------------------------")
        if subtotal_breakdown is not None:
            out.append(f"Subtotal: RM {subtotal_breakdown:,.2f}")
        if discount_breakdown is not None:
            out.append(f"Total Diskaun: RM {discount_breakdown:,.2f}")
        # if total_breakdown is not None:
        #     out.append(f"Total:          RM {total_breakdown:,.2f}")
        if grand_total_breakdown is not None:
            out.append("Grand Total (incl. GST):")
            out.append(f"RM {grand_total_breakdown:,.2f}")
        # if profit_breakdown is not None:
        #     out.append(f"Untung:         RM {profit_breakdown:,.2f}")
        # # if profit_total_breakdown is not None:
        # #     out.append(f"Profit Total:   RM {profit_total_breakdown:,.2f}")
        # margin_display = (
        #     profit_margin_pct_breakdown
        #     if profit_margin_pct_breakdown is not None
        #     else profit_margin_breakdown
        # )
        # if margin_display is not None:
        #     out.append(f"Margin Untung:  {margin_display:.2f}%")
        out.append("-----------------------------")

    follow_up = "Boss, nak saya confirmkan?"
    main_body = "\n".join(out).strip()
    if follow_up:
        return MULTI_MESSAGE_DELIMITER.join([part for part in (main_body, follow_up) if part])
    return main_body


def vn_order_draft_template(args: dict) -> str:
    """
    Short, voice-note friendly version of the current order draft.
    - Bahasa Melayu spoken style.
    - Says quantity as: "total {qty} packs"
    - Keeps all amounts exactly as provided (no rounding)
    - Designed to be sent as text *and* converted to audio.
    - If total_sell / profit are provided on the draft, we also speak out expected sale
      and profit in simple words.
    """
    draft = args.get("draft") or {}
    lines = draft.get("items") or draft.get("lines") or []
    total = draft.get("total_amount") or draft.get("total")

    if not lines:
        return "Boss, sekarang belum ada order."

    parts = []

    customer_name = extract_first_name(args.get("customer_name"))
    if customer_name:
        parts.append(f"Baik {customer_name} Boss,")
    else:
        parts.append("Baik Boss,")

    parts.append("dalam order Boss ada item ni,")

    for line in lines:
        name = line.get("name", "item")
        qty = line.get("qty", 0)
        line_total = line.get("line_total")
        qty_str = str(qty)
        unit_word = "pack"

        if line_total is not None:
            parts.append(f"{name}, total {qty_str} {unit_word}, {line_total} Ringgit")
        else:
            parts.append(f"{name}, total {qty_str} {unit_word}")

    if total is not None:
        parts.append(f"total jadi {total} Ringgit.")

    # Profit line for VN
    total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total_amount")
    if total_sell is None:
        total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total")
    if total_sell is not None and profit is not None:
        parts.append(
            f"Kalau semua ni dijual, total sales sekitar {total_sell} Ringgit "
            f"dan untung lebih kurang {profit} Ringgit."
        )
    else:
        parts.append("Untung Insya-Allah cantik.")

    parts.append("Boleh saya confirm Boss?")

    return " ".join(p.strip() for p in parts if p and str(p).strip())


# 6) Order Status -> Invoice
def invoice_status_template(args: dict) -> str:
    """
    Invoice / order status summary in Bahasa Melayu.
    """
    company_name = _resolve_company_name(args)
    count = args.get("count")
    total = args.get("total")
    old_total = args.get("old_total")
    suggest_topup = args.get("suggest_topup", False)
    topup_gap = args.get("topup_gap")

    if not count or total is None:
        return "Boss, status order belum clear... saya check lagi."

    lines = []
    lines.append(f"Boss, status order {company_name} macam ni:")

    lines.append(f"• item: {count}")

    if old_total:
        lines.append(f"• nilai asal: ~RM {old_total}~")

    lines.append(f"• total akhir: *RM {total}*")

    # Profit / margin overview
    total_sell, profit, margin_pct = _compute_profit_fields(args, total_key="total")
    if total_sell is not None and profit is not None:
        if margin_pct is not None:
            lines.append(
                f"• Sale sekitar *RM {total_sell}* dan untung lebih kurang *RM {profit}* "
                f"(~{margin_pct:.1f}% margin)."
            )
        else:
            lines.append(
                f"• Sale sekitar *RM {total_sell}* dan untung lebih kurang *RM {profit}*."
            )

    if suggest_topup and topup_gap:
        lines.append(
            f"Kalau Boss tambah lebih kurang RM {topup_gap}, mix jadi lebih baik... "
            "nak saya tunjuk SKU kecil?"
        )

    lines.append("Nak saya hantar invoice, atau Boss nak tambah lagi?")

    return "\n".join(lines).strip()


# 7) Reorder (Repeat Last Order)
def reorder_template(args: dict) -> str:
    """
    Shows last order for quick repeat in Bahasa Melayu.
    """
    company_name = _resolve_company_name(args)
    last_order = args.get("last_order") or {}
    lines_data = last_order.get("lines") or []

    if not lines_data:
        return f"Boss, saya tak jumpa order {company_name} yang lepas."

    out = []
    out.append(f"Boss, order terakhir {company_name} macam ni:")

    for idx, line in enumerate(lines_data, 1):
        name = line.get("name", "item")
        qty = line.get("qty", 0)
        out.append(f"{idx}) *{name}* × {qty}")

    out.append("")
    out.append("Nak ulang sama, atau nak ubah?")

    return "\n".join(out).strip()


# 8) Bundles / Combos (simple, no-arg)
def bundles_template():
    """
    Static-ish bundles message in Bahasa Melayu.
    """
    company_name = _resolve_company_name()
    intro_options = [
        f"Boss, {company_name} ada beberapa combo yang memang jalan:",
        "Boss, nak saya share idea combo yang boleh naikkan sale kedai:",
    ]
    reason_lines = [
        "margin bagus",
        "budak dan keluarga sama-sama suka",
        "letak rak terus laju",
    ]

    intro = random.choice(intro_options)

    bundles = [
        {
            "title": "Combo Petang",
            "line": random.choice(reason_lines),
        },
        {
            "title": "Combo Budak",
            "line": random.choice(reason_lines),
        },
    ]

    out = [intro]
    for b in bundles:
        out.append(f"• *{b['title']}*")
        out.append(f"  👉 {b['line']}")

    out.append("")
    out.append("Boss, combo mana nak saya lock?")

    return "\n".join(out).strip()


# 9) Product Info
def product_info_template(args: dict) -> str:
    """
    Product info for SKUs in Bahasa Melayu.
    """
    sku = args.get("sku") or {}
    highlights = args.get("highlights") or []
    promo_hint = args.get("promo_hint", False)

    name = sku.get("name", "item")
    variant = sku.get("variant", "")

    out = [f"Boss, ini detail *{name}*:"]

    for point in highlights[:3]:
        out.append(f"• {point}")

    if variant:
        out.append(f"• saiz pack: *{variant}*")

    if promo_hint:
        out.append("• Sekarang ada deal bagus untuk item ni... nak detail?")

    out.append("Boss, nak saya tambah dalam order draft?")

    return "\n".join(out).strip()


# 10) Top Sellers / Upsell
def recommendations_template(args: dict) -> str:
    """
    Top sellers / upsell list in Bahasa Melayu.
    """
    company_name = _resolve_company_name(args)
    intro_pool = [
        f"Boss, item {company_name} paling laju di kawasan Boss:",
        "Ini top seller biskut dan kek di kawasan Boss:",
        "SKU ni memang laju di kawasan Boss:",
    ]

    cta_pool = [
        "Yang mana nak saya tambah dalam order",
        "Boss nak saya add yang mana",
        "Kita mula dengan 2-3 SKU mana",
    ]

    intro_line = random.choice(intro_pool)
    cta_line = random.choice(cta_pool)

    products = args.get("area_top_sellers") or []

    if not products:
        return (
            "Boss, list top seller tengah refresh... "
            "saya boleh share top deal umum dulu, nak?"
        )

    out = [intro_line]

    for i, item in enumerate(products[:5], 1):
        name = item.get("name", "item")
        price = item.get("price")
        note = item.get("note")

        if price is not None:
            price_line = f"*RM {price}*"
        else:
            price_line = "(harga belum ada)"

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
    Simple seasonal nudge in Bahasa Melayu.
    """
    company_name = _resolve_company_name()
    season_lines = [
        "Bulan Ramzan dan Eid, permintaan biskut dan kek biasanya naik laju",
        f"Musim sejuk, jualan {company_name} dengan teh selalunya naik",
        "Musim sekolah, snack pack laju sangat",
    ]
    cta_lines = [
        f"Kalau Boss simpan stok {company_name} lebih sikit sekarang, lagi selamat",
        "Kalau Boss nak, saya boleh cadang 3-4 SKU seasonal yang laju",
    ]

    season_line = random.choice(season_lines)
    cta_line = random.choice(cta_lines)

    out = []
    out.append(f"{season_line}.")
    out.append("Nak saya rekomen untuk stok terbaik?")
    out.append(cta_line + "?")

    return "\n".join(out).strip()


def current_total_reminder(total: float, casual: bool = True) -> str:
    """
    Quick reminder of current order total in Bahasa Melayu.
    """
    if casual:
        phrases = [
            f"Sekarang order Boss RM {total}",
            f"Total sekarang RM {total} Boss",
            f"Order setakat ni RM {total}",
        ]
        return random.choice(phrases)
    else:
        return f"Total order semasa: RM {total}"


# 12) Objection Handling (simple, no-arg)
def objection_handling_template():
    """
    Generic objection handling lines for Aishah, Bahasa Melayu.
    """
    blocks = []

    company_name = _resolve_company_name()

    # Price objection
    blocks.append(
        "Faham Boss, bajet kadang-kadang ketat.\n"
        "Tapi bila ada deal, ceritanya macam ni:\n"
        "mula-mula harga lama... lepas tu harga promo, beza tu jimat Boss.\n"
        "Ambil pada harga ni biasanya berbaloi, kalau ambil sikit dulu pun risiko rendah."
    )

    # Dont need more
    blocks.append(
        "Kalau Boss rasa stok cukup, ok je.\n"
        f"Cuma kalau kedai lain ambil stok {company_name} yang fresh, "
        "customer boleh lari ke sana.\n"
        "Kalau simpan 1-2 karton SKU laju, rak nampak penuh dan sale lebih selamat."
    )

    # Customers won't buy
    blocks.append(
        "Kalau risau customer tak ambil, cadangan saya cuba kuantiti kecil dulu... "
        "satu dua karton atau beberapa pack.\n"
        "Kalau jalan, next time kita tambah, kalau tak jalan pun Boss dapat rasa kawasan."
    )

    blocks.append("Saya tak paksa Boss, saya cuma bagi pilihan... keputusan Boss.")

    return "\n\n".join(blocks).strip()


def personalized_greeting_template(args: dict) -> str:
    """
    Personalized greeting AFTER sending catalog.
    Bahasa Melayu.
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
        or "Boss"
    )
    customer_name = extract_first_name(customer_name) or "Boss"

    store_name = (
        args.get("store_name")
        or args.get("outlet_name")
        or args.get("shop_name")
        or ""
    )

    catalog_sent = args.get("catalog_sent", True)

    # Line 1 - salam + name (+ optional shop)
    if store_name:
        line1 = f"Assalamualaikum {customer_name} Boss ({store_name}),"
    else:
        line1 = f"Assalamualaikum {customer_name} Boss,"

    # Line 2 - identity
    line2 = f"Saya Aishah dari {company_name}."

    # Line 3 - catalog status
    if catalog_sent:
        line3 = "Katalog dah hantar Boss, boleh tengok bila free."
    else:
        line3 = f"Saya buka katalog {company_name} untuk Boss sekarang."

    # Line 4 - next step
    line4 = "Boss, nak tengok apa dulu?"

    return "\n".join([line1, line2, line3, line4])
