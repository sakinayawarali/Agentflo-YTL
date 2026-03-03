STRINGS = {
    # Basket headers
    "basket_header_budget": "帮您精打细算，预算内最划算的组合：",
    "basket_header_cart": "已为您选购的商品应用了优惠活动：",
    "basket_header_reorder": "根据您最近的进货记录，推荐补货：",
    "basket_header_recommendation": "根据本地热销趋势，智能推荐：",

    # Labels
    "mode_label": "模式",
    "promo_label": "促销",
    "offer_label": "优惠",
    "discount_label": "折扣",
    "price_label": "价格",
    "discounted_price_label": "折后价",
    "subtotal_label": "小计",
    "total_discount_label": "总优惠",
    "grand_total_label": "应付总额",
    "profit_label": "利润",
    "profit_margin_label": "利润率",
    "item_total_label": "单项总计",
    "saving_label": "节省",

    # CTA
    "cta_added_to_cart": "已帮您加入购物车。要修改或再加点其他商品吗？",
    "cta_cart_empty_add_recommendations": "您的购物车是空的。要我把这些推荐加入购物车吗？",
    "cta_recommendations_choose_items": "这些是给您的推荐。要我把哪些商品加入订单？",
    "cta_finalize": "老板，确认下单还是需要调整？",

    # Basket fallback text
    "basket_no_suitable_prefix": "按当前条件无法生成合适订单",
    "basket_no_suitable_message": "请调整预算、添加SKU，或稍后再试。",
    "item_fallback_index": "商品 {index}",
    "item_fallback_generic": "商品",

    # Promotions output
    "promotions_none": "这些商品目前没有可用促销。",
    "promotions_unavailable": "当前无法获取促销数据，请稍后再试。",
    "promotions_intro_pdf_sent": "促销 PDF 已通过 WhatsApp 发送（按最高折扣排序）。",
    "promotions_intro_pdf_failed": "促销 PDF 发送失败，先看重点：",
    "promotions_intro_pdf_generation_failed": "暂时无法生成促销 PDF，重点如下：",
    "promotions_intro_top_with_pdf_hint": "以下是热门促销（发送 PDF 需要有效 user_id）：",
    "promotions_intro_active": "当前促销如下：",
    "promotions_more_skus": "另有 {count} 个 SKU",
    "promotions_more_offers_pdf": "...另外还有 {count} 个优惠已包含在 PDF 中。",
    "promotions_more_offers_active": "...另外还有 {count} 个进行中的优惠。",
    "promotions_special_price": "特价",
    "promotions_special_offer": "特别优惠",
    "promotion_item_fallback": "促销商品",
    "promotions_pdf_caption": "促销按最高折扣排序。",
    "promotions_pdf_title": "当前促销",
    "promotions_pdf_subtitle": "按最高折扣优先排序",
    "pdf_column_item_name": "商品名称",

    # Top sellers output
    "top_sellers_unavailable": "当前无法获取热销数据，请稍后再试。",
    "top_sellers_intro": "当前热销商品如下：",
    "top_sellers_intro_with_limit": "当前前 {limit} 个热销商品如下：",

    # Generic fallback
    "unknown_error": "未知错误",
    "auth_missing_user_id": "需要认证：缺少 user_id。",
    "system_error_missing_api_token": "系统错误：缺少 API_JWT_TOKEN。",
    "system_error_missing_tenant_id": "系统错误：缺少 TENANT_ID。",
    "budget_invalid_max_budget": "预算参数无效：请提供正数 max_budget。",
    "reorder_not_enabled": "补货智能功能尚未启用。后端上线后请设置 SALES_INTEL_REORDER_ENABLED=1。",
    "invalid_request_verify_inputs": "请求参数可能无效，请检查输入。",
    "invalid_request": "请求无效。",
    "system_error_status": "系统错误（{status}）：{detail}",
    "connection_error": "连接错误：系统可能出现问题。（{detail}）",
    "err_known_2001": "预测数据不足 - 已使用兜底模板",
    "err_known_2002": "没有可用促销",
    "err_known_3001": "预算参数无效",
    "err_known_4001": "未找到历史订单",
    "err_known_4002": "日期范围无效",
}
