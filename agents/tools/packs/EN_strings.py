STRINGS = {
    # Basket headers
    "basket_header_budget": "Budget-optimised basket maximising savings within your limit:",
    "basket_header_cart": "Promotions applied on your selected cart items:",
    "basket_header_reorder": "Reorder recommendations based on your recent baskets:",
    "basket_header_recommendation": "Smart recommendations based on forecasts and area bestsellers:",

    # Labels
    "mode_label": "Mode",
    "promo_label": "Promo",
    "offer_label": "Offer",
    "discount_label": "Discount",
    "price_label": "Price",
    "discounted_price_label": "Discounted",
    "subtotal_label": "Subtotal",
    "total_discount_label": "Total Discount",
    "grand_total_label": "Grand Total",
    "profit_label": "Profit",
    "profit_margin_label": "Profit Margin",
    "item_total_label": "Item Total",
    "saving_label": "Saving",

    # CTA
    "cta_added_to_cart": "I added these to your cart. Want to change anything or add something else?",
    "cta_cart_empty_add_recommendations": "Your cart was empty. Want me to add these recommendations to your cart?",
    "cta_recommendations_choose_items": "Here are your recommendations. Which items should I add to your order?",
    "cta_finalize": "Proceed to finalize, or tell me what to change.",

    # Basket fallback text
    "basket_no_suitable_prefix": "No suitable order could be generated with the current inputs",
    "basket_no_suitable_message": "Please adjust the budget, add SKUs, or try again shortly.",
    "item_fallback_index": "Item {index}",
    "item_fallback_generic": "Item",

    # Promotions output
    "promotions_none": "No active promotions are available for these items right now.",
    "promotions_unavailable": "Promotions data is unavailable at the moment. Please try again shortly.",
    "promotions_intro_pdf_sent": "Sharing the promotions PDF on WhatsApp (highest discounts first).",
    "promotions_intro_pdf_failed": "Unable to send the promotions PDF, but here are the highlights:",
    "promotions_intro_pdf_generation_failed": "Promotions PDF could not be generated right now; highlights are below:",
    "promotions_intro_top_with_pdf_hint": "Here are the top promotions (PDF requires a valid user_id):",
    "promotions_intro_active": "Here are the active promotions:",
    "promotions_more_skus": "+{count} more SKUs",
    "promotions_more_offers_pdf": "...plus {count} more offers covered in the PDF.",
    "promotions_more_offers_active": "...plus {count} more active offers.",
    "promotions_special_price": "Special price",
    "promotions_special_offer": "Special Offer",
    "promotion_item_fallback": "Promotion item",
    "promotions_pdf_caption": "Promotions sorted by highest discount first.",
    "promotions_pdf_title": "Current Promotions",
    "promotions_pdf_subtitle": "Sorted by highest discount first",
    "pdf_column_item_name": "Item Name",

    # Top sellers output
    "top_sellers_unavailable": "Top sellers data is unavailable at the moment. Please try again shortly.",
    "top_sellers_intro": "Here are the top sellers right now:",
    "top_sellers_intro_with_limit": "Here are the top {limit} sellers right now:",

    # Generic fallback
    "unknown_error": "Unknown error",
    "auth_missing_user_id": "Authentication required: missing user_id.",
    "system_error_missing_api_token": "System Error: API_JWT_TOKEN missing.",
    "system_error_missing_tenant_id": "System Error: TENANT_ID missing.",
    "budget_invalid_max_budget": "Budget parameters invalid: please provide a positive max_budget.",
    "reorder_not_enabled": "Reorder Intelligence is not active yet. Set SALES_INTEL_REORDER_ENABLED=1 after the backend is live to enable this mode.",
    "invalid_request_verify_inputs": "The request appears invalid; please verify the inputs.",
    "invalid_request": "The request appears invalid.",
    "system_error_status": "System Error ({status}): {detail}",
    "connection_error": "Connection Error: There may be a system issue. ({detail})",
    "err_known_2001": "Insufficient forecast data - fallback template used",
    "err_known_2002": "No applicable promotions",
    "err_known_3001": "Budget parameters invalid",
    "err_known_4001": "No order history found",
    "err_known_4002": "Invalid date range",
}
