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
    Standard Chinese version, Xiao Li persona.
    always call send_product_catalogue tool after running this template.
    """
    company_name = _resolve_company_name()
    greeting_lines = [
        "老板好",
        "哈喽老板",
        "您好",
    ]
    salam_lines = [
        "Waalaikumsalam 老板",
        "Waalaikum salam, 老板",
    ]
    hello_lines = [
        "哈喽老板",
        "Hi 老板",
        "你好老板",
    ]
    smalltalk_lines = [
        "生意怎么样",
        "店里最近如何",
        "饼干销量怎么样",
    ]
    value_hooks = [
        f"{company_name} 的爆款",
        f"{company_name} 的新鲜库存",
        f"{company_name} 饼干和蛋糕的最佳系列",
        "你所在区域的快动销",
    ]
    help_options = [
        "要不要我帮你下单",
        "要不要我分享今天的热卖",
        "要不要我介绍新产品",
        "要不要看看今天的最好优惠",
    ]
    cta_lines = [
        "老板，今天想先看什么",
        "我们先从哪个产品开始",
        "先看哪款产品比较好",
    ]

    first_name = extract_first_name(customer_name)
    if first_name:
        greeting_line = f"{first_name}老板，你好"
    else:
        greeting_line = _smart_greeting_line(user_message, salam_lines, hello_lines, greeting_lines)
    smalltalk_line = random.choice(smalltalk_lines)
    value_hook = random.choice(value_hooks)
    help_line = random.choice(help_options)
    cta_line = random.choice(cta_lines)

    final_response = (
        f"{greeting_line}，\n"
        f"{smalltalk_line}？\n"
        f"我是 {company_name} 的小李... {value_hook} 都有。\n"
        f"{help_line}？\n"
        f"{cta_line}？"
    )

    return f"{final_response.strip()}"


# 2) Manual Order Flow
def manual_order_template(args: dict) -> str:
    """
    Manual ordering helper:
    - Confirms SKU
    - Shows close matches if multiple
    - Mentions totals / promos using RMB.
    - Always in Chinese
    """

    output = []
    company_name = _resolve_company_name(args)

    # Confirm main SKU
    sku = args.get("sku") or {}
    if sku:
        name = sku.get("name", "商品")
        price = sku.get("price")
        qty = sku.get("qty") or 1

        output.append("好的老板，这个商品找到了：")
        sku_lines, _ = format_sku_price_block(name, qty, price, price)
        output.extend(sku_lines)

        output.append("这个帮您确认吗？")

    # Multiple matches
    matches = args.get("multiple_matches") or []
    if matches:
        if not sku:
            output.append("老板，系统里有这些相近的选项：")
        else:
            output.append("同时也有这些选项：")

        for idx, m in enumerate(matches, 1):
            m_name = m.get("name", "商品")
            m_price = m.get("price")

            line_lines, _ = format_sku_price_block(m_name, 1, m_price, m_price, index=idx)
            output.extend(line_lines)

        output.append("老板，您要我确认哪一个？")

    # Quantity ask for confirmed sku
    if sku:
        name = sku.get("name", "商品")
        price = sku.get("price")
        if price is not None:
            output.append(f"好的，*{name}* 的价格是 *¥ {price}*。")
        else:
            output.append(f"好的，*{name}* 已经选好了。")

        output.append("要几箱 / 几包？")

    # Promo already applied
    promo_applied = args.get("promo_applied")
    if promo_applied:
        old_total = promo_applied.get("old_total")
        new_total = promo_applied.get("new_total")
        savings = promo_applied.get("savings")
        output.append("")
        output.append("老板，这个有个不错的优惠：")
        if old_total is not None and new_total is not None:
            output.append(f"之前是 ~¥ {old_total}~，现在是 *¥ {new_total}*。")
        if savings is not None:
            output.append(f"等于省 *¥ {savings}*。")
        output.append("很划算... 要不要按这个价确认？")

    # Promo needs more quantity
    promo_needs_more = args.get("promo_needs_more")
    if promo_needs_more:
        min_qty = promo_needs_more.get("min_qty")
        old_bulk_total = promo_needs_more.get("old_bulk_total")
        new_bulk_total = promo_needs_more.get("new_bulk_total")
        save_bulk = promo_needs_more.get("save_bulk")

        output.append("")
        if min_qty:
            output.append(f"老板，如果拿到 {min_qty} 的数量，优惠更好：")
        if old_bulk_total is not None and new_bulk_total is not None:
            output.append(f"总额从 ~¥ {old_bulk_total}~ 变成 *¥ {new_bulk_total}*。")
        if save_bulk is not None:
            output.append(f"相当于再省 *¥ {save_bulk}*。")
        output.append("要不要我帮你加数量？")

    # Simple total info (no promo)
    no_promo = args.get("no_promo")
    if no_promo:
        total = no_promo.get("total")
        if total is not None:
            output.append(f"好的，目前总额是 *¥ {total}*。")
        output.append(f"如果需要，我可以再推荐一些 {company_name} 的产品，利润更好。")

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
    Formats the current order draft like natural WhatsApp Xiao Li style.
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
        return f"老板，订单草稿现在是空的... 要不要加点 {company_name} 的货？"

    out = []
    out.append("好的老板，\n")
    out.append("你的订单里包含这些商品：")

    total_savings = 0.0
    computed_total = 0.0

    for idx, item in enumerate(items, 1):
        name = item.get("name", "商品")
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
    #         out.append(f"Total ~{old_total_for_display:,.2f} ¥~  *{total_val:,.2f} ¥*")
    #     else:
    #         out.append(f"Total:  *{total_val:,.2f} ¥*")
    # elif total is not None:
    #     out.append(f"Total:  *{total} ¥*")

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
    #             f"卖完后，销售大约 *{total_sell} ¥*，"
    #             f"利润大约 *{profit} ¥* (~{margin_pct:.1f}% 利润率)。"
    #         )
    #     else:
    #         out.append(
    #             f"卖完后，销售大约 *{total_sell} ¥*，"
    #             f"利润大约 *{profit} ¥*。"
    #         )
    # elif margin_pct is not None:
    #     out.append(f"大概利润率 ~{margin_pct:.1f}%。")

    if show_breakdown:
        out.append("-----------------------------")
        if subtotal_breakdown is not None:
            out.append(f"小计: ¥ {subtotal_breakdown:,.2f}")
        if discount_breakdown is not None:
            out.append(f"总折扣: ¥ {discount_breakdown:,.2f}")
        # if total_breakdown is not None:
        #     out.append(f"总额:         ¥ {total_breakdown:,.2f}")
        if grand_total_breakdown is not None:
            out.append("最终总额 (incl. GST):")
            out.append(f"¥ {grand_total_breakdown:,.2f}")
        # if profit_breakdown is not None:
        #     out.append(f"利润:         ¥ {profit_breakdown:,.2f}")
        # # if profit_total_breakdown is not None:
        # #     out.append(f"利润总额:     ¥ {profit_total_breakdown:,.2f}")
        # margin_display = (
        #     profit_margin_pct_breakdown
        #     if profit_margin_pct_breakdown is not None
        #     else profit_margin_breakdown
        # )
        # if margin_display is not None:
        #     out.append(f"利润率:       {margin_display:.2f}%")
        out.append("-----------------------------")

    follow_up = "老板，要不要我确认订单？"
    main_body = "\n".join(out).strip()
    if follow_up:
        return MULTI_MESSAGE_DELIMITER.join([part for part in (main_body, follow_up) if part])
    return main_body


def vn_order_draft_template(args: dict) -> str:
    """
    Short, voice-note friendly version of the current order draft.
    - Chinese spoken style.
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
        return "老板，我这边还没有订单。"

    parts = []

    customer_name = extract_first_name(args.get("customer_name"))
    if customer_name:
        parts.append(f"好的{customer_name}老板，")
    else:
        parts.append("好的老板，")

    parts.append("你的订单里有这些商品，")

    for line in lines:
        name = line.get("name", "商品")
        qty = line.get("qty", 0)
        line_total = line.get("line_total")
        qty_str = str(qty)
        unit_word = "包"

        if line_total is not None:
            parts.append(f"{name}，总共 {qty_str} {unit_word}，{line_total} 元")
        else:
            parts.append(f"{name}，总共 {qty_str} {unit_word}")

    if total is not None:
        parts.append(f"总额是 {total} 元。")

    # Profit line for VN
    total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total_amount")
    if total_sell is None:
        total_sell, profit, margin_pct = _compute_profit_fields(draft, total_key="total")
    if total_sell is not None and profit is not None:
        parts.append(
            f"如果全部卖完，总销售大概 {total_sell} 元，"
            f"利润大概 {profit} 元。"
        )
    else:
        parts.append("利润应该不错。")

    parts.append("老板，我可以确认吗？")

    return " ".join(p.strip() for p in parts if p and str(p).strip())


# 6) Order Status -> Invoice
def invoice_status_template(args: dict) -> str:
    """
    Invoice / order status summary in Chinese.
    """
    company_name = _resolve_company_name(args)
    count = args.get("count")
    total = args.get("total")
    old_total = args.get("old_total")
    suggest_topup = args.get("suggest_topup", False)
    topup_gap = args.get("topup_gap")

    if not count or total is None:
        return "老板，订单状态现在不太清楚... 我再查一下。"

    lines = []
    lines.append(f"老板，{company_name} 订单状态如下：")

    lines.append(f"• 商品数量：{count}")

    if old_total:
        lines.append(f"• 之前金额：~¥ {old_total}~")

    lines.append(f"• 最终总额：*¥ {total}*")

    # Profit / margin overview
    total_sell, profit, margin_pct = _compute_profit_fields(args, total_key="total")
    if total_sell is not None and profit is not None:
        if margin_pct is not None:
            lines.append(
                f"• 预计销售约 *¥ {total_sell}*，利润约 *¥ {profit}* "
                f"(~{margin_pct:.1f}% 利润率)。"
            )
        else:
            lines.append(
                f"• 预计销售约 *¥ {total_sell}*，利润约 *¥ {profit}*。"
            )

    if suggest_topup and topup_gap:
        lines.append(
            f"老板，如果再加大约 ¥ {topup_gap} 的货，组合会更好... "
            "要不要看看小SKU？"
        )

    lines.append("要不要我发票据，还是再加点？")

    return "\n".join(lines).strip()


# 7) Reorder (Repeat Last Order)
def reorder_template(args: dict) -> str:
    """
    Shows last order for quick repeat in Chinese.
    """
    company_name = _resolve_company_name(args)
    last_order = args.get("last_order") or {}
    lines_data = last_order.get("lines") or []

    if not lines_data:
        return f"老板，系统里暂时找不到你上次的 {company_name} 订单。"

    out = []
    out.append(f"老板，你上次的 {company_name} 订单是这样：")

    for idx, line in enumerate(lines_data, 1):
        name = line.get("name", "商品")
        qty = line.get("qty", 0)
        out.append(f"{idx}) *{name}* × {qty}")

    out.append("")
    out.append("要我照旧下单，还是要改一下？")

    return "\n".join(out).strip()


# 8) Bundles / Combos (simple, no-arg)
def bundles_template():
    """
    Static-ish bundles message in Chinese.
    """
    company_name = _resolve_company_name()
    intro_options = [
        f"老板，{company_name} 这几个组合卖得很好：",
        "老板，我给你说几个能提升销量的组合：",
    ]
    reason_lines = [
        "利润不错",
        "孩子和家庭都喜欢",
        "上架就走得快",
    ]

    intro = random.choice(intro_options)

    bundles = [
        {
            "title": "傍晚组合",
            "line": random.choice(reason_lines),
        },
        {
            "title": "儿童组合",
            "line": random.choice(reason_lines),
        },
    ]

    out = [intro]
    for b in bundles:
        out.append(f"• *{b['title']}*")
        out.append(f"  👉 {b['line']}")

    out.append("")
    out.append("老板，要锁定哪个组合？")

    return "\n".join(out).strip()


# 9) Product Info
def product_info_template(args: dict) -> str:
    """
    Product info for SKUs in Chinese.
    """
    sku = args.get("sku") or {}
    highlights = args.get("highlights") or []
    promo_hint = args.get("promo_hint", False)

    name = sku.get("name", "商品")
    variant = sku.get("variant", "")

    out = [f"老板，*{name}* 的一些信息："]

    for point in highlights[:3]:
        out.append(f"• {point}")

    if variant:
        out.append(f"• 包装规格：*{variant}*")

    if promo_hint:
        out.append("• 现在这个有很不错的优惠... 要不要详细？")

    out.append("老板，要不要加进订单草稿？")

    return "\n".join(out).strip()


# 10) Top Sellers / Upsell
def recommendations_template(args: dict) -> str:
    """
    Top sellers / upsell list in Chinese.
    """
    company_name = _resolve_company_name(args)
    intro_pool = [
        f"老板，你这个区域 {company_name} 最快动销的是：",
        "这里是附近最热卖的饼干和蛋糕：",
        "这些 SKU 在你这里动得很快：",
    ]

    cta_pool = [
        "要我加哪几个",
        "老板想加哪几个",
        "先从哪 2-3 个 SKU 开始",
    ]

    intro_line = random.choice(intro_pool)
    cta_line = random.choice(cta_pool)

    products = args.get("area_top_sellers") or []

    if not products:
        return (
            "老板，热卖榜正在刷新... "
            "我可以先分享通用热卖，要不要？"
        )

    out = [intro_line]

    for i, item in enumerate(products[:5], 1):
        name = item.get("name", "商品")
        price = item.get("price")
        note = item.get("note")

        if price is not None:
            price_line = f"*¥ {price}*"
        else:
            price_line = "(价格暂时没有)"

        line = f"{i}) *{name}* — {price_line}"
        if note:
            line += f"\n   👉 _{note}_"

        out.append(line)

    out.append("")
    out.append(cta_line + "？")

    return "\n".join(out).strip()


# 11) Seasonal Advice (simple, no-arg)
def seasonal_advice_template():
    """
    Simple seasonal nudge in Chinese.
    """
    company_name = _resolve_company_name()
    season_lines = [
        "斋月和开斋节期间，饼干和蛋糕的需求会明显上升",
        f"冬季喝茶时间，{company_name} 的销售会自然增加",
        "开学季，小包装动销很快",
    ]
    cta_lines = [
        f"如果现在多备一点 {company_name} 的库存，会更稳",
        "如果你愿意，我可以推荐 3-4 个季节性热卖",
    ]

    season_line = random.choice(season_lines)
    cta_line = random.choice(cta_lines)

    out = []
    out.append(f"{season_line}。")
    out.append("想不想我推荐些更适合备货的？")
    out.append(cta_line + "？")

    return "\n".join(out).strip()


def current_total_reminder(total: float, casual: bool = True) -> str:
    """
    Quick reminder of current order total in Chinese.
    """
    if casual:
        phrases = [
            f"目前订单在 ¥ {total}",
            f"现在总额 ¥ {total}",
            f"到目前为止总额 ¥ {total}",
        ]
        return random.choice(phrases)
    else:
        return f"当前订单总额：¥ {total}"


# 12) Objection Handling (simple, no-arg)
def objection_handling_template():
    """
    Generic objection handling lines for Xiao Li, Chinese.
    """
    blocks = []

    company_name = _resolve_company_name()

    # Price objection
    blocks.append(
        "我理解老板，预算有时候会紧。\n"
        "但是有优惠时，情况是这样的：\n"
        "先是旧价格... 再是促销价，中间的差额就是你的节省。\n"
        "这个价位通常更划算，先拿一点试水风险也低。"
    )

    # Dont need more
    blocks.append(
        "如果你觉得库存够了也可以。\n"
        f"但如果附近店铺拿了新的 {company_name} 库存，顾客可能会转过去。\n"
        "备 1-2 箱动销快的 SKU，货架更满，销量更稳。"
    )

    # Customers won't buy
    blocks.append(
        "如果担心顾客不买，建议先做小单试试... "
        "一两箱或者几包。\n"
        "卖得动的话下次再加，不动也能了解本地口味。"
    )

    blocks.append("我不是强推，只是给你更多选择... 最终决定还是你。")

    return "\n\n".join(blocks).strip()


def personalized_greeting_template(args: dict) -> str:
    """
    Personalized greeting AFTER sending catalog.
    Chinese.
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
        or "老板"
    )
    customer_name = extract_first_name(customer_name) or "老板"

    store_name = (
        args.get("store_name")
        or args.get("outlet_name")
        or args.get("shop_name")
        or ""
    )

    catalog_sent = args.get("catalog_sent", True)

    # Line 1 - salam + name (+ optional shop)
    if store_name:
        line1 = f"{customer_name}老板（{store_name}），您好！"
    else:
        line1 = f"{customer_name}老板，您好！"

    # Line 2 - identity
    line2 = f"我是 {company_name} 的小李。"

    # Line 3 - catalog status
    if catalog_sent:
        line3 = "目录已经发给你了，有空看看。"
    else:
        line3 = f"我现在为你打开 {company_name} 的目录。"

    # Line 4 - next step
    line4 = "老板，先看什么？"

    return "\n".join([line1, line2, line3, line4])
