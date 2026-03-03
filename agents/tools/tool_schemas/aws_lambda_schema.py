import asyncio
from google.adk.agents import LlmAgent, SequentialAgent
from google.adk.tools.agent_tool import AgentTool
import datetime
from pydantic import BaseModel, Extra, Field
from typing import List, Optional
class InvoiceItemAWS(BaseModel):
    model_config = {"extra": "forbid"}  # ✅ Pydantic v2 way

    """Model for individual invoice items"""
    description_en: str = Field(..., description="Item description in English")
    item_number: str = Field(..., description="SKU Code")
    qty: int = Field(..., description="Item quantity")
    unit_price: float = Field(..., description="Unit price")
    value: float = Field(..., description="Unit Price before discount")
    discount: float = Field(..., description="Discount amount")
    total: float = Field(..., description="Total after discount")
    vat_code: str = Field("S", description="VAT code")


class InvoiceItemTotalsAWS(BaseModel):
    """Model for item totals summary"""
    model_config = {"extra": "forbid"}  # ✅ Pydantic v2 way

    total_qty: int = Field(..., description="Total quantity of all items")
    total_value: float = Field(..., description="Total value of all items")
    total_discount: float = Field(..., description="Total discount amount")

class InvoiceDataAWS(BaseModel):
    """
    Pydantic model for variable invoice data used in Lambda endpoint
    """
    # model_config = {"extra": "forbid"}  # ✅ Pydantic v2 way

    store_name_en: str = Field(
        #default="Almarai Store",
        description="Store name in English"
    )

    customer_name: str = Field(
        #default="Unknown",
        description="The name of the customer"
    )

    store_id: str = Field(
        #default="2401",
        description="Unique store identifier"
    )

    date: str = Field(
        datetime.datetime.now().strftime("%d-%m-%Y"),
        description="Date in DD-MM-YYYY format",
    )

    time: str = Field(
        datetime.datetime.now().strftime("%H:%M:%S"),
        description="Time in HH:MM:SS format",
    )

    customer_phone: str = Field(
        # default="",
        description="Customer phone number"
    )

    items: List[InvoiceItemAWS] = Field(
        # default_factory=list,
        description="List of invoice items"
    )

    item_totals: InvoiceItemTotalsAWS = Field(
        ...,
        description="Summary of item totals"
    )


# ---------- SIMPLE DEBUG HELPER ----------
def build_invoice_payload(raw_payload: dict) -> dict:
    """
    Call this right before sending to Lambda.
    Prints validation issues directly into Cloud Run logs.
    """
    print("=== [SCHEMA] Building invoice payload ===")
    print("[SCHEMA] Raw payload keys:", list(raw_payload.keys()))

    try:
        model = InvoiceDataAWS.model_validate(raw_payload)
        payload = model.model_dump()
        print("[SCHEMA] Validation SUCCESS")
        print("[SCHEMA] Items count:", len(payload.get("items", [])))
        return payload

    except ValidationError as e:
        print("!!! [SCHEMA] VALIDATION FAILED !!!", file=sys.stderr)
        print(json.dumps(e.errors(), indent=2), file=sys.stderr)
        raise


# asyncio.run(invoiceGenerationTool.run_async({"input":"The customer is John Doe and he bought 2 items."}))

# if __name__ == "__main__":
#     test_invoice_data = {
#         "store_name_en": "Test Store",
#         "cashier": "John Doe",
#         "store_id": "TEST-001",
#         "date": "01-01-2024",
#         "time": "09:00:00",
#         "customer_phone": "+1234567890",
#         "items": [
#             {
#                 "description_en": "Test Item 1",
#                 "item_number": "ITEM-001",
#                 "qty": 2,
#                 "unit_price": 10.0,
#                 "value": 20.0,
#                 "discount": 2.0,
#                 "total": 18.0,
#                 "vat_code": "VAT-01"
#             }
#         ],
#         "item_totals": {
#             "total_qty": 2,
#             "total_value": 20.0,
#             "total_discount": 2.0
#         }
#     }
#     print(InvoiceVariableData(**test_invoice_data))
 