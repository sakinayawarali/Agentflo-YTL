import os
from typing import Any, Dict, List, Optional
from dotenv import load_dotenv
from pydantic import BaseModel
import requests
import datetime
import json
import logging
from utils.logging import debug_enabled
from agents.tools.tool_schemas.order_draft_schema import OrderDraft, OrderDraftItem
from agents.tools.api_tools import semantic_product_search, unwrap_tool_response
from agents.tools.cart_tools import agentflo_cart_tool

# Initialize logger
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class CartOrderItem(BaseModel):
    product_retailer_id: Optional[str] = None
    item_price: float
    quantity: int
    currency: str   

class ProductDetailResponse(BaseModel):
    id: str
    name: str
    retailer_id: Optional[str]
    price: Optional[str] = None

class OrderHelper:
    def __init__(self):
        self.access_token = os.getenv("WHATSAPP_ACCESS_TOKEN")
        self.phone_number_id = os.getenv("WHATSAPP_PHONE_NUMBER_ID")
        self.product_set_id = os.getenv("WHATSAPP_PRODUCT_SET_ID")
        self.headers = {'Authorization': f'Bearer {self.access_token}'}
        self.WHATSAPP_PRODUCTS_URL = f"https://graph.facebook.com/v23.0/{self.product_set_id}/products"


    def get_product_details(self, product_ids: List[str], limit: int = 1000) -> Dict[str, ProductDetailResponse]:
        """
        Retrieves product details for given product IDs from WhatsApp Products API.
        Includes a fallback full-scan if the filtered request silently fails.
        """
        if not product_ids:
            return {}

        import json
        
        target_ids = {str(pid) for pid in product_ids}
        results = {}

        # Attempt 1: Filtered API call (Using correct is_any syntax)
        filter_param = json.dumps({"retailer_id": {"is_any": list(target_ids)}})
        params = {
            "filter": filter_param,
            "limit": limit,
            "fields": "id,name,retailer_id,price"
        }
        
        try:
            response = requests.get(
                self.WHATSAPP_PRODUCTS_URL,
                headers=self.headers,
                params=params,
                timeout=10,
            )
            if response.ok:
                products = response.json().get("data", [])
                for product in products:
                    rid = product.get("retailer_id")
                    if rid in target_ids:
                        results[rid] = ProductDetailResponse(**product)
        except Exception as e:
            print(f"Filtered request failed: {e}")

        # If we found all requested items, return immediately
        if len(results) >= len(target_ids):
            return results

        # Attempt 2: Fallback full catalog scan (Meta filter bug workaround)
        print(f"Fallback: Scanning catalog for missing items...")
        fallback_params = {
            "fields": "id,name,retailer_id,price",
            "limit": limit
        }
        url = self.WHATSAPP_PRODUCTS_URL
        
        try:
            while url:
                response = requests.get(url, headers=self.headers, params=fallback_params, timeout=10)
                if not response.ok:
                    break
                
                data = response.json()
                products = data.get("data", [])
                
                for product in products:
                    rid = product.get("retailer_id")
                    if rid in target_ids and rid not in results:
                        results[rid] = ProductDetailResponse(**product)
                
                if len(results) >= len(target_ids):
                    break
                    
                # Handle Meta's cursor-based pagination
                url = data.get("paging", {}).get("next")
                fallback_params = None # Don't pass params on next page, URL already has them built-in
        except Exception as e:
            print(f"Fallback scan failed: {e}")

        return results


    def parse_order_message(self, order_json: Dict) -> List[CartOrderItem]:
        """
        Parses the order message JSON and extracts order items.

        Args:
            order_json (Dict): The order message JSON to parse

        Returns:
            List[CartOrderItem]: A list of cart items
        """
        items = []
        for item in order_json.get("product_items", []):
            order_item = CartOrderItem.model_validate(item)
            items.append(order_item)
        return items


    def _resolve_store_id_from_api(self, user_id: str) -> str:
        """
        Best-effort: resolve a real store code from search_customer_by_phone.
        Returns empty string if resolution fails (caller should fall back to user_id).
        """
        try:
            from agents.tools.api_tools import search_customer_by_phone, unwrap_tool_response
            raw = search_customer_by_phone(user_id)
            ok, payload, err = unwrap_tool_response(raw, system_name="search_customer_by_phone")
            if ok and isinstance(payload, dict):
                data = payload.get("data") or {}
                store_code = (
                    (data.get("additional_info") or {}).get("storecode")
                    or data.get("storecode")
                    or data.get("store_key")
                )
                if not store_code:
                    for entry in data.get("external_ids") or []:
                        if isinstance(entry, dict) and entry.get("ref"):
                            store_code = entry["ref"]
                            break
                if store_code and str(store_code).strip():
                    return str(store_code).strip()
        except Exception as e:
            logger.warning("order_helper.store_id_resolution_failed user_id=%s error=%s", user_id, str(e))
        return ""

    def convert_order_json_to_order_draft(self, order_json: dict, user_id: Optional[str] = None) -> OrderDraft:
        """
        Converts the incoming order JSON from WhatsApp to an OrderDraft object.

        Args:
            order_json (dict): The order JSON to convert

        Returns:
            OrderDraft: The converted OrderDraft object
        """

        if debug_enabled():
            try:
                item_count = len(order_json.get("product_items") or [])
                logger.info(
                    "order_helper.convert.start items=%s keys=%s",
                    item_count,
                    list(order_json.keys())[:10],
                )
            except Exception:
                pass

        # Parse cart items and retailer_ids
        whatsapp_order_items = self.parse_order_message(order_json)
        if not whatsapp_order_items:
            raise ValueError("No valid order items found")

        # Collect only non-empty retailer_ids for the WA Products API
        retailer_ids = [
            item.product_retailer_id
            for item in whatsapp_order_items
            if item.product_retailer_id
        ]

        # Fetch product details only if we actually have retailer_ids
        product_details_dict: Dict[str, ProductDetailResponse] = {}
        if retailer_ids:
            product_details_dict = self.get_product_details(retailer_ids)

        order_draft_items: List[OrderDraftItem] = []
        total_amount = 0.0

        # Map product details to order items and enrich with internal SKU data via semantic search
        for idx, cart_item in enumerate(whatsapp_order_items):
            product_details: Optional[ProductDetailResponse] = None

            if cart_item.product_retailer_id:
                product_details = product_details_dict.get(cart_item.product_retailer_id)

            # Use the WhatsApp product name if available; otherwise a generic fallback
            if product_details and product_details.name:
                product_name = product_details.name
            else:
                if cart_item.product_retailer_id:
                    product_name = f"SKU {cart_item.product_retailer_id}"
                else:
                    product_name = f"Catalog Item {idx + 1}"

            # Defaults from WhatsApp
            sku_code = cart_item.product_retailer_id or product_name
            mapped_name = product_name

            # Parse WhatsApp catalog price if we have product_details
            whatsapp_price: Optional[float] = None
            if product_details and product_details.price:
                try:
                    whatsapp_price = float(product_details.price)
                except (ValueError, TypeError):
                    logger.warning(
                        "order_helper.price_parse_failed retailer_id=%s raw_price=%s",
                        cart_item.product_retailer_id,
                        product_details.price,
                    )

            # Use WhatsApp price as primary, fall back to cart item price
            mapped_price: Optional[float] = whatsapp_price or cart_item.item_price

            # Try to enrich with internal SKU via semantic search.
            #
            # CRITICAL: Only run semantic search when the WhatsApp Products API
            # returned a real product name. If the lookup failed we only have a
            # placeholder like "SKU 36" or "Catalog Item 1". Passing those strings
            # to semantic_product_search returns the highest-scoring unrelated
            # product in the catalogue (e.g. "MARIE Half Roll"), silently replacing
            # the correct item. When two items both fail the WA lookup they can
            # also map to the *same* internal SKU and merge in the cart, which is
            # why 3 ordered items can appear as only 2 in the confirmation.
            #
            # Minimum similarity required even when a real name is available:
            SEMANTIC_MATCH_MIN_SIMILARITY = 0.75

            if product_details is None:
                # WhatsApp Products API had no record for this retailer_id.
                # Keep sku_code = retailer_id (already set above) and skip search.
                # Log so the team can investigate the catalog-sync gap.
                logger.warning(
                    "order_helper.wa_product_lookup_miss retailer_id=%s "
                    "- skipping semantic search to avoid misidentification",
                    cart_item.product_retailer_id,
                )
            else:
                # We have a real product name from WhatsApp — safe to enrich.
                try:
                    semantic_raw = semantic_product_search(product_name)
                    ok, payload, err = unwrap_tool_response(
                        semantic_raw,
                        system_name="semantic_product_search",
                    )
                    if not ok or not payload:
                        logger.warning(
                            "order_helper.semantic_search_failed name=%s error=%s",
                            product_name,
                            err,
                        )
                        payload = {}

                    # semantic_product_search returns JSON text; normalize to dict
                    if isinstance(payload, str):
                        try:
                            payload = json.loads(payload)
                        except json.JSONDecodeError:
                            logger.warning(
                                "order_helper.semantic_search_json_decode_failed name=%s preview=%s",
                                product_name,
                                payload[:200] if len(payload) > 200 else payload,
                            )

                    # Expect final shape: {"data": [ {...}, {...} ]}
                    data = None
                    if isinstance(payload, dict):
                        data = payload.get("data")

                    if isinstance(data, list) and data:
                        def _similarity(item: Dict[str, Any]) -> float:
                            try:
                                return float(item.get("similarity") or 0.0)
                            except (TypeError, ValueError):
                                return 0.0

                        best_match = max(data, key=_similarity)
                        best_score = _similarity(best_match)

                        # Reject low-confidence matches — a score below the
                        # threshold means the search found no real equivalent and
                        # we must not substitute an unrelated product.
                        if best_score < SEMANTIC_MATCH_MIN_SIMILARITY:
                            logger.warning(
                                "order_helper.semantic_match_below_threshold "
                                "name=%s best_score=%.3f threshold=%.3f - keeping original",
                                product_name,
                                best_score,
                                SEMANTIC_MATCH_MIN_SIMILARITY,
                            )
                        else:
                            product_payload = best_match.get("product") or best_match or {}
                            pricing = product_payload.get("pricing") or {}

                            # Override with internal system data if available
                            sku_code = (
                                product_payload.get("skucode")
                                or product_payload.get("sku_code")
                                or sku_code
                            )
                            candidate_name = (
                                product_payload.get("official_name")
                                or product_payload.get("name")
                                or mapped_name
                            )
                            # Avoid wiping an existing catalog name when official_name is null/blank
                            if isinstance(candidate_name, str) and candidate_name.strip():
                                mapped_name = candidate_name.strip()

                            # Prefer internal pricing; keep WhatsApp/cart price as fallback
                            internal_price = pricing.get("total_buy_price_virtual_pack")
                            if internal_price is not None:
                                try:
                                    mapped_price = float(internal_price)
                                except (TypeError, ValueError):
                                    pass

                            logger.info(
                                "order_helper.semantic_match_accepted "
                                "name=%s → sku=%s score=%.3f",
                                product_name,
                                sku_code,
                                best_score,
                            )

                except Exception as e:
                    logger.error(
                        "order_helper.semantic_search_error name=%s error=%s",
                        product_name,
                        str(e),
                    )

            # FINAL SAFETY: Ensure price is never None
            if mapped_price is None:
                logger.error(
                    "order_helper.no_price_available sku_code=%s retailer_id=%s whatsapp_price=%s cart_price=%s",
                    sku_code,
                    cart_item.product_retailer_id,
                    whatsapp_price,
                    cart_item.item_price,
                )
                mapped_price = 0.0

            qty_val = cart_item.quantity or 0
            try:
                line_total_val = float(mapped_price) * float(qty_val)
            except Exception:
                try:
                    line_total_val = float(cart_item.item_price) * float(qty_val)
                except Exception:
                    line_total_val = 0.0

            # Build OrderDraftItem (product_retailer_id can be None)
            order_draft_item = OrderDraftItem(
                sku_code=sku_code,
                name=mapped_name,
                qty=qty_val,
                price=mapped_price,
                base_price=mapped_price,
                final_price=mapped_price,
                line_total=line_total_val,
                product_retailer_id=cart_item.product_retailer_id,
            )

            # total_amount based on normalized line totals (fallback to 0)
            try:
                total_amount += float(line_total_val)
            except Exception:
                total_amount += 0.0
            order_draft_items.append(order_draft_item)

        if debug_enabled():
            try:
                logger.info(
                    "order_helper.convert.done items=%s total=%s",
                    len(order_draft_items),
                    round(total_amount, 2),
                )
            except Exception:
                pass

        # Resolve real store_id from the customer API so downstream cart/pricing tools
        # never fall back to the phone number as a store code.
        resolved_store_id = ""
        if user_id:
            resolved_store_id = self._resolve_store_id_from_api(user_id)
            if resolved_store_id:
                logger.info(
                    "order_helper.store_id_resolved user_id=%s store_id=%s",
                    user_id,
                    resolved_store_id,
                )
            else:
                logger.warning(
                    "order_helper.store_id_unresolved user_id=%s - store_id will be empty; "
                    "cart_tools will attempt resolution on its own",
                    user_id,
                )

        final_order_draft = OrderDraft(
            last_updated=datetime.datetime.now().isoformat(),
            store_id=resolved_store_id,  # empty string → cart_tools will re-attempt via Firestore/API
            items=order_draft_items,
            total_amount=total_amount,
        )
        return final_order_draft


if __name__ == "__main__":
    load_dotenv()
    order_helper = OrderHelper()

    # Minimal sample payload similar to WhatsApp catalog order webhook
    sample_order_json = {
        "product_items": [
            {
                "product_retailer_id": "2",
                "item_price": 80.0,
                "quantity": 2,
                "currency": "PKR",
            },
            {
                "product_retailer_id": "5",
                "item_price": 50.0,
                "quantity": 1,
                "currency": "PKR",
            },
        ]
    }

    # Run full processing to convert to OrderDraft
    final_order_draft = order_helper.convert_order_json_to_order_draft(sample_order_json)

    # Print the resulting OrderDraft as JSON
    print(final_order_draft.model_dump_json(indent=2))
    operations = [{"op": "CLEAR_CART"}]
    for item in final_order_draft.items:
        operations.append(
            {
                "op": "ADD_ITEM",
                "sku_code": item.sku_code,
                "qty": item.qty,
                "merge_strategy": "INCREMENT",
                "name": item.name,
                "product_retailer_id": item.product_retailer_id,
            }
        )
    test_update = agentflo_cart_tool(
        {"user_id": "923161620950", "store_id": "923161620950", "operations": operations}
    )
    print(test_update)