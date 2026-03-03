import os
import re
import time
import requests
from typing import Optional, Dict, List
from dotenv import load_dotenv
load_dotenv()

GRAPH_VER = "v23.0"
ACCESS_TOKEN=os.getenv("WHATSAPP_ACCESS_TOKEN")
PRODUCT_SET_ID = os.getenv("WHATSAPP_PRODUCT_SET_ID")
CATALOG_ID = os.getenv("ENGRO_CATALOG_ID")


class GraphAPIError(Exception):
    pass

def _get(url: str, token: str, params: dict | None ) -> dict:
    """Basic GET with error handling and minimal retry on 5xx."""
    headers = {"Authorization": f"Bearer {token}"}
    for attempt in range(3):
        r = requests.get(url, headers=headers, params=params, timeout=30)
        if r.status_code >= 500:
            time.sleep(1.2 * (attempt + 1))
            continue
        if r.ok:
            return r.json()
        try:
            j = r.json()
        except Exception:
            j = {"error": {"message": r.text}}
        msg = j.get("error", {}).get("message", r.text)
        raise GraphAPIError(f"HTTP {r.status_code}: {msg}")
    raise GraphAPIError(f"Graph API temporarily unavailable: {url}")

def _paginate(url: str, token: str, params: dict ):
    """Yield pages following paging.next."""
    while True:
        data = _get(url, token, params=params)
        yield data
        next_url = (data.get("paging") or {}).get("next")
        if not next_url:
            break
        # after first call, params must not be re-sent (next already contains them)
        url = next_url
        params = None

def _score(name: str, query: str) -> int:
    """
    Simple relevance score:
      +100 if exact case-insensitive match
      +60 if startswith
      +40 if substring
      else 0
    """
    n = name.lower()
    q = query.lower()
    if n == q:
        return 100
    if n.startswith(q):
        return 60
    if q in n:
        return 40
    return 0

def search_products_by_name(
    search_term: str,
    *,
    limit: int = 100,
) -> List[Dict]:
    """
    Return a sorted list of matching products with fields:
    { 'id': <PRODUCT_ITEM_ID>, 'name': <NAME>, 'retailer_id': <RETAILER_ID>, 'score': <int> }

    One of product_set_id or catalog_id must be provided.
    """
    product_set_id , catalog_id , access_token= PRODUCT_SET_ID , CATALOG_ID , ACCESS_TOKEN
    if not (product_set_id or catalog_id):
        raise ValueError("Provide either product_set_id or catalog_id.")

    # Decide which edge to use
    if product_set_id:
        base_url = f"https://graph.facebook.com/{GRAPH_VER}/{product_set_id}/products"
    else:
        # Some catalogs expose /products, some expose /items. Try /products first, then fallback to /items.
        base_url = f"https://graph.facebook.com/{GRAPH_VER}/{catalog_id}/products"

    fields = "id,name,retailer_id, price"
    params = {"fields": fields, "limit": limit}

    matches: List[Dict] = []

    try:
        for page in _paginate(base_url, access_token, params):
            for item in page.get("data", []):
                name = item.get("name") or ""
                rid = item.get("retailer_id")
                if not name or not rid:
                    continue
                score = _score(name, search_term)
                if score > 0:
                    matches.append(
                        {"id": item.get("id"), "name": name, "retailer_id": rid, "score": score}
                    )
    except GraphAPIError as e:
        # Fallback to /items if /products is not available on this catalog type
        msg = str(e)
        if "Unknown path components" in msg and catalog_id and "/products" in base_url:
            alt_url = f"https://graph.facebook.com/{GRAPH_VER}/{catalog_id}/items"
            for page in _paginate(alt_url, access_token, {"fields": fields, "limit": limit}):
                for item in page.get("data", []):
                    name = item.get("name") or ""
                    rid = item.get("retailer_id")
                    if not name or not rid:
                        continue
                    score = _score(name, search_term)
                    if score > 0:
                        matches.append(
                            {"id": item.get("id"), "name": name, "retailer_id": rid, "score": score}
                        )
        else:
            raise

    # Sort best match first
    matches.sort(key=lambda x: (-x["score"], x["name"]))
    return matches

def lookup_names_by_retailer_ids(
    retailer_ids: List[str],
    *,
    limit: int = 200,
    catalog_id_override: Optional[str] = None,
) -> Dict[str, str]:
    """
    Given product_retailer_id values from a native WhatsApp order (e.g. ["36","50","70"]),
    return a dict mapping retailer_id -> product name from the Meta catalog.

    catalog_id_override: if provided, query this catalog directly (use the
    catalog_id from the WhatsApp order payload — it's the authoritative source).
    """
    if not retailer_ids:
        return {}
    import json as _json
    product_set_id, catalog_id, access_token = PRODUCT_SET_ID, CATALOG_ID, ACCESS_TOKEN
    if not access_token:
        return {}

    # Priority: override from order payload > product_set_id env > catalog_id env
    if catalog_id_override:
        base_url = f"https://graph.facebook.com/{GRAPH_VER}/{catalog_id_override}/products"
    elif product_set_id:
        base_url = f"https://graph.facebook.com/{GRAPH_VER}/{product_set_id}/products"
    elif catalog_id:
        base_url = f"https://graph.facebook.com/{GRAPH_VER}/{catalog_id}/products"
    else:
        return {}

    target_ids = {str(r).strip() for r in retailer_ids if r}
    results: Dict[str, str] = {}

    # Attempt 1: filtered API call
    try:
        filter_param = _json.dumps({"retailer_id": {"is_any": list(target_ids)}})
        params = {"fields": "id,name,retailer_id", "limit": limit, "filter": filter_param}
        for page in _paginate(base_url, access_token, params):
            for item in page.get("data", []):
                rid = str(item.get("retailer_id") or "").strip()
                name = (item.get("name") or "").strip()
                if rid in target_ids and name:
                    results[rid] = name
            if len(results) >= len(target_ids):
                break
        if results:
            return results
    except GraphAPIError:
        pass

    # Attempt 2: full paginated scan (Meta filter bug workaround)
    try:
        params = {"fields": "id,name,retailer_id", "limit": limit}
        for page in _paginate(base_url, access_token, params):
            for item in page.get("data", []):
                rid = str(item.get("retailer_id") or "").strip()
                name = (item.get("name") or "").strip()
                if rid in target_ids and name:
                    results[rid] = name
            if len(results) >= len(target_ids):
                break
    except GraphAPIError:
        pass

    # Attempt 3: if catalog_id_override was a catalog (not a product set),
    # try /items endpoint as fallback (some catalog types use /items)
    if not results and catalog_id_override:
        alt_url = f"https://graph.facebook.com/{GRAPH_VER}/{catalog_id_override}/items"
        try:
            params = {"fields": "id,name,retailer_id", "limit": limit}
            for page in _paginate(alt_url, access_token, params):
                for item in page.get("data", []):
                    rid = str(item.get("retailer_id") or "").strip()
                    name = (item.get("name") or "").strip()
                    if rid in target_ids and name:
                        results[rid] = name
                if len(results) >= len(target_ids):
                    break
        except GraphAPIError:
            pass

    return results

def find_first_retailer_id(
    search_term: str,
    *,
    limit: int = 100,
) -> Optional[Dict]:
    """
    Convenience: return the best match (or None). Dict includes name, retailer_id, id.
    """
    results = search_products_by_name(
        search_term,
        limit=limit,
    )
    return results[0]['retailer_id'] if results else None

# def debug_meta_catalog_inventory():
#     """
#     DEBUG: Fetches all items from the configured product set 
#     to see exactly how many SKUs Meta considers active.
#     """
#     target_id = PRODUCT_SET_ID or CATALOG_ID
#     if not target_id:
#         print("DEBUG: No PRODUCT_SET_ID or CATALOG_ID found in env.")
#         return
        
#     url = f"https://graph.facebook.com/{GRAPH_VER}/{target_id}/products"
#     params = {"fields": "id,name,retailer_id,review_status,availability", "limit": 100}
    
#     total_items, approved_items, in_stock_items = 0, 0, 0
    
#     try:
#         print(f"DEBUG: Querying Meta API for ID: {target_id}")
#         for page in _paginate(url, ACCESS_TOKEN, params):
#             for item in page.get("data", []):
#                 total_items += 1
#                 status = str(item.get("review_status", "")).lower()
#                 stock = str(item.get("availability", "")).lower()
                
#                 if status == "approved": approved_items += 1
#                 if stock == "in stock": in_stock_items += 1
                    
#                 if total_items <= 5:
#                     print(f"DEBUG ITEM: {item.get('name')} | SKU: {item.get('retailer_id')} | Status: {status} | Stock: {stock}")
                    
#         print("\n--- META CATALOG DEBUG SUMMARY ---")
#         print(f"Total Items Found: {total_items}")
#         print(f"Approved Items: {approved_items}")
#         print(f"In Stock Items: {in_stock_items}")
        
#         if approved_items <= 3 or in_stock_items <= 3:
#             print("\nCONCLUSION: Meta Commerce Manager only has 3 or fewer active/approved/in-stock items. The issue is on Facebook's end (Commerce Manager).")
#         else:
#             print("\nCONCLUSION: Meta has many active items. The issue is likely inside your `send_product_catalogue` function limiting the list.")
            
#     except Exception as e:
#         print(f"DEBUG ERROR: {e}")

# Quick check - paste your product_set_id
# import os, requests
# def debug_cat():
#     PRODUCT_SET_ID = os.getenv("WHATSAPP_PRODUCT_SET_ID")
#     r = requests.get(
#         f"https://graph.facebook.com/v23.0/{PRODUCT_SET_ID}/products",
#         headers={"Authorization": f"Bearer {os.getenv('WHATSAPP_ACCESS_TOKEN')}"},
#         params={"fields": "id,name,review_status,availability", "limit": 10}
#     )
#     print(r.json())
load_dotenv()

SET_ID = os.getenv("WHATSAPP_PRODUCT_SET_ID")
TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN")

def issues():
    print(f"DEBUG: Checking Product Set ID: {SET_ID}")
    
    # Added ?limit=100 to pull larger chunks, reducing API calls
    url = f"https://graph.facebook.com/v23.0/{SET_ID}/products?limit=100"
    headers = {"Authorization": f"Bearer {TOKEN}"}
    
    all_items = []
    
    # Loop to handle Meta's cursor-based pagination
    while url:
        r = requests.get(url, headers=headers)
        data = r.json()
        
        # Check for API errors
        if "error" in data:
            print(f"API ERROR: {data['error']['message']}")
            break
            
        items = data.get("data", [])
        all_items.extend(items)
        
        # Grab the 'next' URL from the paging dictionary. 
        # If it doesn't exist, url becomes None and the loop breaks.
        url = data.get("paging", {}).get("next")

    print(f"DEBUG: Found {len(all_items)} total items in this Product Set.")
    for i in all_items:
        print(f"- {i.get('name')} (Retailer ID: {i.get('retailer_id')})")

if __name__ == "__main__":
    issues()

# import os, requests
# def debuggg():
#     r = requests.get(
#         f"https://graph.facebook.com/v23.0/{os.getenv('WHATSAPP_PHONE_NUMBER_ID')}",
#         headers={"Authorization": f"Bearer {os.getenv('WHATSAPP_ACCESS_TOKEN')}"},
#         params={"fields": "whatsapp_business_account"}
#     )
#     print(r.json())

# if __name__ == "__main__":
#     # ---- Quick demo (fill these) ----
#     ACCESS_TOKEN = os.getenv("WHATSAPP_ACCESS_TOKEN") 
#     # Use one of these two:
#     PRODUCT_SET_ID = os.getenv("WHATSAPP_PRODUCT_SET_ID")
#     CATALOG_ID = os.getenv("ENGRO_CATALOG_ID")  # e.g. "1021739876643712"

#     SEARCH = "Olpers Mango FM 180ml"  # your search term

#     best = find_first_retailer_id(
#         SEARCH,
#         limit=100,
#     )
#     if best:
#         print(best)
#         # print(f"Retailer ID: {best['retailer_id']}")
#         # print(f"Product Item ID: {best['id']}")
#     else:
#         print("No match found.")