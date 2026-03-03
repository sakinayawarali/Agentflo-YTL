# YTL Cement Demo – Local Data

When **USE_LOCAL_DATA** is set to `true` (or `1`/`yes`), the agent uses the JSON files in this folder instead of calling external product/customer APIs. API integration code is unchanged; it is only bypassed in this mode.

## Files

- **products.json** – Concrete grades (GR20, GR25, GR30, GR40) and pump (PUMP) with RM pricing per m³.
- **customers.json** – Demo customers lookup by phone.
- **delivery_eta.json** – Sample delivery orders for ETA/tracking (e.g. order CN-8842, CN-7891).
- **promotions.json** – Optional promotions for sales intelligence (if used in demo).

## Running the YTL demo

1. Set in your environment (or `.env`):
   - `USE_LOCAL_DATA=true`
   - `TENANT_ID=ytl` (optional; ensures business name resolves to "YTL Cement")
2. Ensure Firestore (or your session backend) is configured if you use cart/order persistence.
3. Start the app and chat on WhatsApp: place concrete orders (grade, m³, address, pump) or ask "Where is my truck?" with order ID **CN-8842** or address "123 Jalan Puchong" to see ETA from demo data.
