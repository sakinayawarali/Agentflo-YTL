# import datetime
# from pydantic import BaseModel, Field
# from typing import Any, List, Union
# import datetime

# class OrderDraftItem(BaseModel):
#     sku_code: str = Field(..., description="Unique SKU code for the product")
#     name: str = Field(..., description="Product name")
#     qty: int = Field(..., description="Quantity purchased")
#     price: float = Field(..., description="Price per unit")

# class OrderDraft(BaseModel):
#     store_id: str = Field(..., description="Identifier for the store")
#     skus: List[OrderDraftItem] = Field(..., description="List of SKU items purchased")
#     total_amount: float = Field(..., description="Total amount for the purchase")
#     last_updated: str = Field(datetime.datetime.now().isoformat(), description="Timestamp of last update")
import datetime
from typing import List, Optional

from pydantic import AliasChoices, BaseModel, Field


class OrderDraftItem(BaseModel):
    sku_code: str = Field(
        ...,
        description="Unique SKU code for the product",
        validation_alias=AliasChoices(
            "sku_code",
            "sku",
            "skucode",
            "sku_id",
            "item_number",
            "variant_code",
            "id",
        ),
        serialization_alias="sku",
    )
    name: Optional[str] = Field(
        default=None,
        description="Item name",
        validation_alias=AliasChoices(
            "name",
            "official_name",
            "product_name",
            "sku_name",
            "description",
            "description_en",
            "title",
            "sku_desc",
        ),
    )
    qty: int = Field(
        ...,
        description="Quantity purchased",
        validation_alias=AliasChoices("qty", "quantity", "forecast_qty"),
    )
    price: Optional[float] = Field(
        default=None,
        description="Price per unit",
        validation_alias=AliasChoices(
            "price",
            "unit_price",
            "final_price",
            "base_price",
            "consumer_price",
            "list_price",
            "mrp",
            "total_buy_price_virtual_pack",
        ),
    )

    # Optional fields for detailed pricing
    base_price: Optional[float] = Field(
        None,
        description="Base price before discount",
        validation_alias=AliasChoices(
            "base_price",
            "consumer_price",
            "list_price",
            "mrp",
            "unit_price",
            "price",
        ),
    )
    final_price: Optional[float] = Field(
        None,
        description="Final price after discount",
        validation_alias=AliasChoices(
            "final_price",
            "discounted_price",
            "unit_price_final",
            "unit_price",
            "price",
            "total_buy_price_virtual_pack",
        ),
    )
    discount_value: Optional[float] = Field(
        None,
        description="Discount amount",
        validation_alias=AliasChoices("discount_value", "unit_discount", "line_discount", "discount"),
    )
    discount_value_line: Optional[float] = Field(
        None,
        description="Total discount amount for the full line",
        validation_alias=AliasChoices("discount_value_line", "line_discount"),
    )
    discount_pct: Optional[float] = Field(
        None,
        description="Discount percentage",
        validation_alias=AliasChoices("discount_pct", "discountvalue", "discount_percentage"),
    )
    line_total: Optional[float] = Field(
        None,
        description="Total line amount",
        validation_alias=AliasChoices("line_total", "linetotal", "lineamount", "line_total_amount"),
    )
    profit: Optional[float] = Field(
        None,
        description="Line-level profit from sales intelligence",
        validation_alias=AliasChoices("profit", "line_profit"),
    )
    profit_margin: Optional[float] = Field(
        None,
        description="Line-level profit margin percentage",
        validation_alias=AliasChoices("profit_margin", "profit_margin_pct", "margin_pct"),
    )

    # NEW: required for WhatsApp multi-product cart
    product_retailer_id: Optional[str] = Field(
        default=None,
        description=(
            "WhatsApp catalog product_retailer_id for use in multi-product messages. "
            "If missing, this item may be skipped or looked up when building the MPM cart."
        ),
        validation_alias=AliasChoices("product_retailer_id", "productid", "product_id", "retailer_id", "id"),
    )

    # Optional: qty adjustment (delta). If provided, merge logic will add this delta to existing qty.
    adjust_qty_by: Optional[int] = Field(
        default=None,
        description="If set, represents a delta to apply to current qty (e.g., -2 to reduce by 2).",
    )


class OrderDraft(BaseModel):
    store_id: str = Field(
        ...,
        description="Identifier for the store",
        validation_alias=AliasChoices("store_id", "customer_id", "store_code"),
    )
    items: List[OrderDraftItem] = Field(
        ...,
        description="List of items in the order draft",
        validation_alias=AliasChoices("items", "skus", "lines"),
        serialization_alias="items",
    )
    total_amount: float = Field(
        ...,
        description="Total amount for the purchase",
        validation_alias=AliasChoices("total_amount", "total", "grand_total", "subtotal"),
    )
    subtotal: Optional[float] = Field(
        None,
        description="Subtotal before discounts from sales intelligence",
        validation_alias=AliasChoices("subtotal", "total_list_price"),
    )
    discount_total: Optional[float] = Field(
        None,
        description="Total discount value from sales intelligence",
        validation_alias=AliasChoices("discount_total", "discount", "total_discount"),
    )
    grand_total: Optional[float] = Field(
        None,
        description="Grand total after discounts from sales intelligence",
        validation_alias=AliasChoices("grand_total", "total"),
    )
    profit: Optional[float] = Field(
        None,
        description="Total profit from sales intelligence",
        validation_alias=AliasChoices("profit", "profit_total"),
    )
    profit_margin: Optional[float] = Field(
        None,
        description="Overall profit margin percentage from sales intelligence",
        validation_alias=AliasChoices("profit_margin", "profit_margin_pct", "margin_pct"),
    )
    last_updated: str = Field(
        default_factory=lambda: datetime.datetime.now().isoformat(),
        description="Timestamp of last update",
    )
