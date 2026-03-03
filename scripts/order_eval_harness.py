import re
import json
import math
import os
from dataclasses import dataclass, field, asdict
from typing import List, Optional, Dict, Tuple, Any, Callable
from google import genai
from google.genai.types import HttpOptions
from dotenv import load_dotenv

# Load .env so GOOGLE_API_KEY / GENAI_API_KEY etc. are available for agent calls
load_dotenv()

# -----------------------------
# Data models
# -----------------------------


@dataclass
class OrderItem:
    name: str
    price_original: float
    price_discounted: float
    qty: int
    item_total: float
    saving: float

    def recalc_from_prices(self):
        self.item_total = round(self.price_discounted * self.qty, 2)
        self.saving = round((self.price_original - self.price_discounted) * self.qty, 2)


@dataclass
class OrderTotals:
    subtotal: float = 0.0
    total_discount: float = 0.0
    grand_total: float = 0.0
    profit: float = 0.0
    profit_margin_pct: float = 0.0


@dataclass
class OrderState:
    items: List[OrderItem] = field(default_factory=list)
    totals: OrderTotals = field(default_factory=OrderTotals)
    raw_message: str = ""
    history: List[Dict[str, Any]] = field(default_factory=list)  # snapshots for "go back"

    def snapshot(self, label: str):
        snap = {
            "label": label,
            "items": [asdict(i) for i in self.items],
            "totals": asdict(self.totals),
        }
        self.history.append(snap)

    def restore_last_snapshot(self) -> bool:
        if len(self.history) < 2:
            return False
        # Pop current state snapshot, restore previous
        self.history.pop()
        snap = self.history[-1]
        self.items = [OrderItem(**d) for d in snap["items"]]
        self.totals = OrderTotals(**snap["totals"])
        return True


# -----------------------------
# Parsing
# -----------------------------


def _to_float(s: str) -> float:
    s = s.replace(",", "").strip()
    return float(s)


def parse_order_message(text: str) -> OrderState:
    """
    Parses messages like:
      1) ITEM NAME
      Price: 232.64 218.41 x 10
      Item Total: 2,184.12 (Saving: 142.30)
      ...
      Subtotal: Rs 2,712.90
      Total Discount: Rs 175.97
      Grand Total: Rs 2,536.93
      Profit: Rs 263.07
      Profit Margin: 9.70%
    """
    state = OrderState(raw_message=text)

    # Item blocks: index + name line, then Price line, then Item Total line
    # Allow missing original/discounted values (dash) and optional saving section.
    item_pat = re.compile(
        r"(?ms)^\s*\d+\)\s*(?P<name>.+?)\s*$.*?"
        r"^\s*Price:\s*(?P<po>[\d,]+(?:\.\d+)?|-)\s+(?P<pd>[\d,]+(?:\.\d+)?|-)\s*x\s*(?P<qty>\d+)\s*$.*?"
        r"^\s*Item\s*Total:\s*(?P<total>[\d,]+(?:\.\d+)?|-)(?:\s*\(Saving:\s*(?P<saving>[\d,]+(?:\.\d+)?)\))?",
        re.IGNORECASE,
    )

    for m in item_pat.finditer(text):
        name = m.group("name").strip()
        po_raw = m.group("po")
        pd_raw = m.group("pd")
        total_raw = m.group("total")
        saving_raw = m.group("saving")

        qty = int(m.group("qty"))
        po = _to_float(po_raw) if po_raw and po_raw != "-" else None
        pd = _to_float(pd_raw) if pd_raw and pd_raw != "-" else po
        total = _to_float(total_raw) if total_raw and total_raw != "-" else None
        saving = _to_float(saving_raw) if saving_raw else 0.0

        if pd is None and po is not None:
            pd = po
        if total is None and pd is not None:
            total = round(pd * qty, 2)
        if saving == 0.0 and po is not None and pd is not None:
            saving = round((po - pd) * qty, 2)

        state.items.append(
            OrderItem(
                name=name,
                price_original=po or 0.0,
                price_discounted=pd or po or 0.0,
                qty=qty,
                item_total=total or 0.0,
                saving=saving,
            )
        )

    # Totals
    def find_money(label: str) -> Optional[float]:
        mm = re.search(rf"{re.escape(label)}\s*:\s*Rs\s*([\d,]+(?:\.\d+)?)", text, re.IGNORECASE)
        return _to_float(mm.group(1)) if mm else None

    subtotal = find_money("Subtotal")
    total_discount = find_money("Total Discount")
    grand_total = find_money("Grand Total")
    profit = find_money("Profit")

    pm = re.search(r"Profit\s*Margin\s*:\s*([\d,]+(?:\.\d+)?)\s*%", text, re.IGNORECASE)
    profit_margin = _to_float(pm.group(1)) if pm else None

    # Merge duplicates by normalized name (user items + recommendations often overlap)
    state.items = merge_duplicate_items(state.items)

    # Always normalize totals from item lines to avoid drift; provided totals are ignored if inconsistent.
    recalc_totals(state)

    # If profit/profit_margin were present in the text, keep them; else keep computed defaults
    if profit is not None:
        state.totals.profit = profit
    if profit_margin is not None:
        state.totals.profit_margin_pct = profit_margin

    state.snapshot("parsed")
    return state


# -----------------------------
# Normalization / fuzzy match
# -----------------------------


def normalize_name(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def simple_similarity(a: str, b: str) -> float:
    """
    Lightweight similarity: token overlap (Jaccard).
    Good enough for typos/partials in your messages without extra deps.
    """
    ta = set(normalize_name(a).split())
    tb = set(normalize_name(b).split())
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def find_item(state: OrderState, query_name: str) -> Optional[OrderItem]:
    q = query_name.strip()
    if not q:
        return None
    best = None
    best_score = 0.0
    for item in state.items:
        score = simple_similarity(item.name, q)
        if score > best_score:
            best_score = score
            best = item
    # threshold for partial names/typos
    return best if best_score >= 0.25 else None


def merge_duplicate_items(items: List[OrderItem]) -> List[OrderItem]:
    """
    Merge items with the same normalized name to prevent duplication
    when recommendations overlap with user-added items.
    """
    merged: Dict[str, OrderItem] = {}
    for it in items:
        key = normalize_name(it.name)
        if key in merged:
            existing = merged[key]
            existing.qty += it.qty
            # Keep the lower discounted price if both exist
            existing.price_discounted = min(existing.price_discounted, it.price_discounted)
            existing.price_original = max(existing.price_original, it.price_original)
            existing.recalc_from_prices()
        else:
            merged[key] = OrderItem(
                name=it.name,
                price_original=it.price_original,
                price_discounted=it.price_discounted,
                qty=it.qty,
                item_total=it.item_total,
                saving=it.saving,
            )
    return list(merged.values())


# -----------------------------
# Business logic / validations (extend these for your real rules)
# -----------------------------

DEFAULT_STOCK = {
    # Example stock; replace with real inventory
    "rio double chocolate ticky pack": 100,
    "rio strawbery vanilla packet": 20,
    "gluco half roll": 12,
}
DEFAULT_MIN_QTY = {
    # Example MOQ
    "gluco half roll": 2
}
DEFAULT_BUDGET_RS = None  # e.g., 5000


def validate_state(state: OrderState,
                   stock: Dict[str, int] = DEFAULT_STOCK,
                   min_qty: Dict[str, int] = DEFAULT_MIN_QTY,
                   budget_rs: Optional[float] = DEFAULT_BUDGET_RS) -> List[str]:
    issues = []
    # Consistency check: totals vs item sums
    expected_sub = round(sum(it.price_original * it.qty for it in state.items), 2)
    expected_disc = round(sum(it.saving for it in state.items), 2)
    expected_gt = round(sum(it.item_total for it in state.items), 2)
    if abs(state.totals.subtotal - expected_sub) > 0.01 or abs(state.totals.grand_total - expected_gt) > 0.01:
        issues.append(
            f"Totals mismatch (subtotal {state.totals.subtotal:.2f} vs items {expected_sub:.2f}, "
            f"grand total {state.totals.grand_total:.2f} vs items {expected_gt:.2f})."
        )
    if abs(state.totals.total_discount - expected_disc) > 0.01:
        issues.append(
            f"Discount mismatch (total discount {state.totals.total_discount:.2f} vs items {expected_disc:.2f})."
        )
    for it in state.items:
        key = normalize_name(it.name)
        if key in stock and it.qty > stock[key]:
            issues.append(f"Quantity for '{it.name}' exceeds stock (qty {it.qty} > stock {stock[key]}).")
        if key in stock and stock[key] <= 0:
            issues.append(f"Item '{it.name}' is out of stock.")
        if key in min_qty and it.qty < min_qty[key]:
            issues.append(f"Item '{it.name}' is below minimum order quantity (qty {it.qty} < MOQ {min_qty[key]}).")
    if budget_rs is not None and state.totals.grand_total > budget_rs:
        issues.append(f"Grand total Rs {state.totals.grand_total:,.2f} exceeds budget Rs {budget_rs:,.2f}.")
    return issues


def recalc_totals(state: OrderState):
    state.items = merge_duplicate_items(state.items)

    for it in state.items:
        it.recalc_from_prices()

    state.totals.subtotal = round(sum(it.price_original * it.qty for it in state.items), 2)
    state.totals.total_discount = round(sum(it.saving for it in state.items), 2)
    state.totals.grand_total = round(sum(it.item_total for it in state.items), 2)

    state.totals.profit = round(state.totals.grand_total * 0.10, 2)
    gt = state.totals.grand_total or 1.0
    state.totals.profit_margin_pct = round((state.totals.profit / gt) * 100.0, 2)


# -----------------------------
# Intent extraction (rule-based baseline; your LLM agent will replace this)
# -----------------------------


def extract_intent(user_text: str) -> Dict[str, Any]:
    """
    Minimal intent extractor to generate a structured "expected action".
    You can use this to compare vs what your agent outputs, OR to create gold labels.
    """
    t = user_text.strip()

    # Clear
    if re.search(r"\bclear\b|\bstart over\b", t, re.IGNORECASE):
        return {"intent": "clear"}

    # Go back / undo
    if re.search(r"\bgo back\b|\bundo\b|\brevert\b", t, re.IGNORECASE):
        return {"intent": "undo"}

    # Confirm
    if re.search(r"\bconfirm\b|\bproceed\b|\bfinalize\b", t, re.IGNORECASE):
        return {"intent": "confirm"}

    # Remove item
    m = re.search(r"\bremove\b\s+(?P<item>.+)$", t, re.IGNORECASE)
    if m:
        return {"intent": "remove", "item": m.group("item").strip()}

    # Change quantity: "Change X quantity to 15" / "set X to 15"
    m = re.search(r"(?:change|set)\s+(?P<item>.+?)\s+(?:quantity\s+)?to\s+(?P<qty>\d+)", t, re.IGNORECASE)
    if m:
        return {"intent": "set_qty", "item": m.group("item").strip(), "qty": int(m.group("qty"))}

    # Add more: "Add 5 of X" / "Add more of X"
    m = re.search(r"\badd\b\s+(?:(?P<qty>\d+)\s+(?:of\s+)?)?(?P<item>.+)$", t, re.IGNORECASE)
    if m:
        qty = m.group("qty")
        item = m.group("item").strip()
        if qty:
            return {"intent": "add_qty", "item": item, "qty": int(qty)}
        if re.search(r"\bmore\b", t, re.IGNORECASE):
            return {"intent": "add_qty", "item": item, "qty": None}
        return {"intent": "add_unknown", "raw": t}

    # Replace
    m = re.search(r"\breplace\b\s+(?P<a>.+?)\s+\bwith\b\s+(?P<b>.+)$", t, re.IGNORECASE)
    if m:
        return {"intent": "replace", "from": m.group("a").strip(), "to": m.group("b").strip()}

    # Informational queries (fallback)
    return {"intent": "info_query", "raw": t}


# -----------------------------
# Apply actions
# -----------------------------


def apply_action(state: OrderState, action: Dict[str, Any]) -> Tuple[OrderState, str]:
    """
    Applies rule-based state changes. Your LLM agent can instead output actions
    in this same schema, and you can reuse this function to execute them.
    """
    intent = action.get("intent")

    if intent == "clear":
        state.snapshot("before_clear")
        state.items = []
        recalc_totals(state)
        state.snapshot("after_clear")
        return state, "Cleared the order."

    if intent == "undo":
        ok = state.restore_last_snapshot()
        return state, "Reverted to previous state." if ok else "Nothing to undo."

    if intent == "remove":
        target = action.get("item", "")
        it = find_item(state, target)
        if not it:
            return state, f"Couldn't find an item matching '{target}'."
        state.snapshot("before_remove")
        state.items = [x for x in state.items if x is not it]
        recalc_totals(state)
        state.snapshot("after_remove")
        return state, f"Removed '{it.name}'."

    if intent == "set_qty":
        target = action.get("item", "")
        qty = int(action.get("qty", 0))
        it = find_item(state, target)
        if not it:
            return state, f"Couldn't find an item matching '{target}'."
        state.snapshot("before_set_qty")
        it.qty = qty
        recalc_totals(state)
        state.snapshot("after_set_qty")
        return state, f"Set quantity of '{it.name}' to {qty}."

    if intent == "add_qty":
        target = action.get("item", "")
        qty = action.get("qty")
        it = find_item(state, target)
        if not it:
            return state, f"Couldn't find an item matching '{target}'."
        if qty is None:
            return state, f"How many should I add for '{it.name}'?"
        state.snapshot("before_add_qty")
        it.qty += int(qty)
        recalc_totals(state)
        state.snapshot("after_add_qty")
        return state, f"Added {qty} to '{it.name}'. New qty: {it.qty}."

    if intent == "replace":
        a = action.get("from", "")
        b = action.get("to", "")
        it = find_item(state, a)
        if not it:
            return state, f"Couldn't find an item matching '{a}'."
        state.snapshot("before_replace")
        it.name = b
        recalc_totals(state)
        state.snapshot("after_replace")
        return state, f"Replaced '{a}' with '{b}' (simulated; prices unchanged)."

    if intent == "confirm":
        issues = validate_state(state)
        if issues:
            return state, "Can't confirm due to issues:\n- " + "\n- ".join(issues)
        return state, "Order confirmed (simulated)."

    # info_query / fallback
    return state, "No state change."


# -----------------------------
# Prompt builder (match your React SYSTEM_PROMPT pattern)
# -----------------------------

SYSTEM_PROMPT = """You are an intelligent order management assistant. You help users understand, modify, and optimize their orders.

Your capabilities:
1. Parse order confirmation and recommendation messages
2. Answer questions about items, pricing, savings, and margins
3. Help modify orders (change quantities, add/remove items)
4. Provide optimization suggestions
5. Compare orders and analyze profitability
6. Handle ambiguous requests by asking clarifying questions

Order Message Format:
- Items with: Name, Price (original discounted), Quantity, Item Total, Savings
- Subtotal, Total Discount, Grand Total, Profit, Profit Margin

When responding:
- Be concise and helpful
- For modifications, clearly state what will change
- For analysis, provide actionable insights
- Ask clarifying questions when needed
- Format numbers clearly with Rs currency
- Highlight important information

Current order context will be provided with each query.
"""


def build_prompt(order_message: str, user_query: str) -> str:
    return f"{SYSTEM_PROMPT}\n\nCurrent Order Message:\n{order_message}\n\nUser Query: {user_query}"


def call_your_agent(prompt: str) -> str:
    
    model_id = os.getenv("AGENT_MODEL_ID", "gemini-2.5-flash")
    try:
        # Check for API key first (prioritize this method)
        api_key = (
            os.getenv("GOOGLE_API_KEY")
            or os.getenv("GENAI_API_KEY")
            or os.getenv("GOOGLE_GENAI_API_KEY")
        )
        
        client_kwargs = {
            "vertexai": False,
            "http_options": HttpOptions(baseUrl="https://generativelanguage.googleapis.com", apiVersion="v1"),
        }
        
        # Use API key if available (simpler, recommended)
        if api_key:
            client_kwargs["api_key"] = api_key
        else:
            # Fall back to Vertex AI only if no API key is set
            project = (
                os.getenv("VERTEXAI_PROJECT")
                or os.getenv("GOOGLE_GENAI_PROJECT")
                or os.getenv("GOOGLE_CLOUD_PROJECT")
            )
            location = (
                os.getenv("VERTEXAI_LOCATION")
                or os.getenv("GOOGLE_GENAI_LOCATION")
                or "us-central1"
            )
            
            if project:
                client_kwargs["vertexai"] = {"project": project, "location": location}
            else:
                raise RuntimeError(
                    "Missing credentials: set GOOGLE_API_KEY (or GENAI_API_KEY / GOOGLE_GENAI_API_KEY) "
                    "OR VERTEXAI_PROJECT + VERTEXAI_LOCATION."
                )

        client = genai.Client(**client_kwargs)
        resp = client.models.generate_content(
            model=model_id,
            contents=prompt,
        )
        return (getattr(resp, "text", None) or "").strip() or "EMPTY_AGENT_RESPONSE"
    except Exception as e:
        return f"AGENT_CALL_FAILED: {e}"


# -----------------------------
# Evaluation harness
# -----------------------------

SAMPLE_ORDER = """Alright,
Here are the items included in your order:
1) RIO DOUBLE CHOCOLATE Ticky Pack
Price: 232.64 218.41 x 10
Item Total: 2,184.12 (Saving: 142.30)
2) RIO Strawbery VANILLA Packet
Price: 77.30 70.56 x 5
Item Total: 352.81 (Saving: 33.70)
-----------------------------
Subtotal: Rs 2,712.90
Total Discount: Rs 175.97
Grand Total: Rs 2,536.93
Profit: Rs 263.07
Profit Margin: 9.70%
-----------------------------
Should I confirm this for you?"""

SAMPLE_RECS = """Smart recommendations based on forecasts and area bestsellers:
1) SOOPER CLASSIC Half Roll
Price: 311.38 288.75 x 2
Item Total: 577.50 (Saving: 45.26)
2) SOOPER CLASSIC Snack Pack
Price: 311.69 288.69 x 3
Item Total: 866.06 (Saving: 69.00)
3) PEANUT PIK Half Roll
Price: 311.38 288.75 x 2
Item Total: 577.50 (Saving: 45.26)
4) GLUCO Half Roll
Price: 311.38 288.75 x 3
Item Total: 866.24 (Saving: 67.89)
5) GLUCO Snack Pack
Price: 311.69 288.69 x 2
Item Total: 577.37 (Saving: 46.00)
6) SOOPER CLASSIC CHOCOLATE Half Roll
Price: 311.38 288.75 x 3
Item Total: 866.24 (Saving: 67.89)
7) SOOPER CLASSIC Munch Pack
Price: 350.37 324.83 x 2
Item Total: 649.65 (Saving: 51.08)
8) RIO Strawbery VANILLA Half Roll
Price: 311.38 288.75 x 3
Item Total: 866.24 (Saving: 67.89)
9) LEMON SANDWICH Snack Pack
Price: 311.69 288.69 x 2
Item Total: 577.37 (Saving: 46.00)
-----------------------------
Subtotal: Rs 6,930.51
Total Discount: Rs 506.34
Grand Total: Rs 6,424.17
Profit: Rs 695.83
Profit Margin: 10.0%
-----------------------------
Proceed to finalize, or tell me what to change."""

SCENARIOS = [
    # Order confirmation flows
    {"name": "info_total_savings", "order": SAMPLE_ORDER, "query": "What's my total savings?"},
    {"name": "modify_set_qty", "order": SAMPLE_ORDER, "query": "Change RIO DOUBLE CHOCOLATE quantity to 15"},
    {"name": "remove_cheapest", "order": SAMPLE_ORDER, "query": "Remove the cheapest item"},
    {"name": "ambiguous_add_5", "order": SAMPLE_ORDER, "query": "add 5"},

    # Recommendation flows
    {"name": "why_recommended", "order": SAMPLE_RECS, "query": "Why is GLUCO Half Roll recommended?"},
    {"name": "accept_some", "order": SAMPLE_RECS, "query": "Remove LEMON SANDWICH Snack Pack and keep the rest"},
    {"name": "what_if_increase_20pct", "order": SAMPLE_RECS, "query": "What if I increase quantities by 20%?"},
]


def cheapest_item_name(state: OrderState) -> Optional[str]:
    if not state.items:
        return None
    it = min(state.items, key=lambda x: x.price_discounted)
    return it.name


def run_baseline_executor(order_message: str, query: str) -> Dict[str, Any]:
    """
    This is NOT your agent. It's a local baseline executor to:
      - parse + apply modifications for measurable state changes
      - detect ambiguous cases
    Use it as a comparator or for generating expected outcomes.
    """
    state = parse_order_message(order_message)

    # Special-case: "remove the cheapest item"
    if re.search(r"remove\s+the\s+cheapest", query, re.IGNORECASE):
        name = cheapest_item_name(state)
        action = {"intent": "remove", "item": name or ""}
    # Special-case: what-if increase quantities by 20% (simulation)
    elif re.search(r"increase\s+quantities?\s+by\s+20%|increase\s+by\s+20%", query, re.IGNORECASE):
        state.snapshot("before_whatif_20pct")
        for it in state.items:
            it.qty = int(math.ceil(it.qty * 1.2))
        recalc_totals(state)
        state.snapshot("after_whatif_20pct")
        return {
            "action": {"intent": "whatif_increase_20pct"},
            "state": asdict(state),
            "notes": "Applied 20% qty increase (ceil)."
        }
    else:
        action = extract_intent(query)

    state, msg = apply_action(state, action)
    issues = validate_state(state)

    return {
        "action": action,
        "apply_message": msg,
        "issues": issues,
        "state": asdict(state),
    }


# -----------------------------
# Scoring functions
# -----------------------------


def score_agent_response_with_llm(agent_text: str, scenario: Dict[str, Any], baseline: Dict[str, Any]) -> Dict[str, Any]:
    """
    Uses Gemini 2.0 Flash to score agent responses contextually.
    Evaluates: correctness, helpfulness, clarity, and tone.
    """
    scoring_prompt = f"""You are an expert evaluator for an order management AI assistant. Score the agent's response on a scale of 0-3 points based on the criteria below.

**Scenario Context:**
- User Query: "{scenario['query']}"
- Order Message: {scenario['order'][:500]}... (truncated)

**Agent's Response:**
{agent_text}

**Baseline Expected Behavior:**
- Action Intent: {baseline.get('action', {}).get('intent', 'N/A')}
- Expected Message: {baseline.get('apply_message', 'N/A')}
- Resulting Totals: Grand Total Rs {baseline['state']['totals'].get('grand_total', 'N/A')}

**Scoring Criteria (0-3 points total):**

1. **Correctness (0-1 point)**: Does the response correctly understand and address the query?
   - 1 point: Correct understanding and accurate information
   - 0 points: Misunderstands query or provides incorrect information

2. **Helpfulness (0-1 point)**: Does it provide actionable, useful information?
   - 1 point: Clearly states what will change, provides relevant details (new totals, impacts)
   - 0.5 points: Partially helpful but missing key details
   - 0 points: Vague or unhelpful

3. **Clarity & Professionalism (0-1 point)**: Is the response clear, well-formatted, and professional?
   - 1 point: Clear, well-structured, uses proper currency formatting (Rs), professional tone
   - 0.5 points: Somewhat clear but could be better formatted or more concise
   - 0 points: Confusing, poorly formatted, or unprofessional

**Special Cases:**
- For ambiguous queries (e.g., "add 5"): Full points if asks clarifying question, 0 if assumes
- For "why recommended": Should explain reasoning beyond just repeating "forecasts and bestsellers"
- For modifications: Should show before/after comparison or impact on totals

**Output Format (JSON only, no explanation):**
{{
  "score": <total points 0-3>,
  "breakdown": {{
    "correctness": <0-1>,
    "helpfulness": <0-1>,
    "clarity": <0-1>
  }},
  "notes": "<brief explanation of scoring>"
}}"""

    try:
        api_key = (
            os.getenv("GOOGLE_API_KEY")
            or os.getenv("GENAI_API_KEY")
            or os.getenv("GOOGLE_GENAI_API_KEY")
        )
        
        if not api_key:
            # Fallback to rule-based scoring if no API key
            return score_agent_response_rule_based(agent_text, scenario)
        
            client = genai.Client(
                api_key=api_key,
                vertexai=False,
                http_options=HttpOptions(baseUrl="https://generativelanguage.googleapis.com", apiVersion="v1")
            )
        
        resp = client.models.generate_content(
            model="gemini-2.5-flash",  # Fast and cost-effective for evaluation
            contents=scoring_prompt,
        )
        
        response_text = (getattr(resp, "text", None) or "").strip()
        
        # Parse JSON response
        # Remove markdown code blocks if present
        response_text = re.sub(r'```json\s*|\s*```', '', response_text).strip()
        result = json.loads(response_text)
        
        return {
            "score": result.get("score", 0),
            "max_score": 3,
            "notes": [result.get("notes", "LLM scoring completed")],
            "breakdown": result.get("breakdown", {})
        }
        
    except Exception as e:
        print(f"LLM scoring failed: {e}, falling back to rule-based")
        return score_agent_response_rule_based(agent_text, scenario)


def score_agent_response_rule_based(agent_text: str, scenario: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fallback rule-based scoring (original implementation).
    """
    q = scenario["query"].lower()
    t = agent_text.lower()

    score = 0
    max_score = 3
    notes = []

    if "add 5" in q:
        if ("which item" in t) or ("which" in t and "item" in t) or ("clarify" in t):
            score += 1
        else:
            notes.append("Ambiguous 'add 5' should trigger a clarifying question.")

    if "total savings" in q:
        if "rs" in t and ("saving" in t or "savings" in t):
            score += 1
        else:
            notes.append("Should mention Rs savings amount for total savings query.")

    if "quantity" in q or "change" in q or "remove" in q:
        if "will" in t or "updated" in t or "set" in t or "removed" in t:
            score += 1
        else:
            notes.append("For modifications, should clearly state what will change.")

    return {"score": score, "max_score": max_score, "notes": notes}


# -----------------------------
# Trajectory scoring (ADK-style scaffolding)
# -----------------------------

TRAJECTORY_EXPECTATIONS: Dict[str, Dict[str, Any]] = {
    # Expected tool/path steps per scenario. Extend with your real traces.
    # match_type: EXACT | IN_ORDER | ANY_ORDER (see ADK docs)
    # expected: ordered list of required steps/tool names
    "info_total_savings": {
        "match_type": "ANY_ORDER",
        "expected": ["parse_order", "respond_savings"],
    },
    "modify_set_qty": {
        "match_type": "IN_ORDER",
        "expected": ["parse_order", "resolve_item", "update_qty", "reprice", "summarize"],
    },
    "remove_cheapest": {
        "match_type": "IN_ORDER",
        "expected": ["parse_order", "find_cheapest", "remove_item", "reprice", "summarize"],
    },
    "ambiguous_add_5": {
        "match_type": "ANY_ORDER",
        "expected": ["parse_order", "clarify_item"],
    },
    "why_recommended": {
        "match_type": "ANY_ORDER",
        "expected": ["parse_recommendations", "explain_reason"],
    },
    "accept_some": {
        "match_type": "IN_ORDER",
        "expected": ["parse_recommendations", "resolve_item", "remove_item", "reprice", "summarize"],
    },
    "what_if_increase_20pct": {
        "match_type": "IN_ORDER",
        "expected": ["parse_recommendations", "simulate_change", "reprice", "summarize"],
    },
}


def _load_actual_trajectory(scenario_name: str, traj_dir: Optional[str]) -> Optional[List[str]]:
    """
    Loads an actual tool/step trajectory for a scenario from a JSON file.
    The file should contain a simple JSON array of step/tool names, e.g.:
      ["parse_order", "resolve_item", "update_qty", "reprice", "summarize"]
    Set ORDER_EVAL_TRAJ_DIR env var or pass traj_dir to run_suite.
    """
    if not traj_dir:
        return None
    path = os.path.join(traj_dir, f"{scenario_name}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, list):
            return [str(x) for x in data]
    except Exception:
        return None
    return None


def score_trajectory(expected: List[str], actual: List[str], match_type: str = "IN_ORDER") -> Tuple[float, str]:
    """
    Returns (score, note). Score is 1.0 or 0.0 for simplicity.
    """
    match_type = match_type.upper()
    if match_type == "EXACT":
        ok = expected == actual
        return (1.0 if ok else 0.0, "EXACT match" if ok else f"Mismatch: expected {expected}, got {actual}")

    if match_type == "IN_ORDER":
        # All expected must appear in order inside actual (extras allowed)
        idx = 0
        for step in actual:
            if idx < len(expected) and step == expected[idx]:
                idx += 1
        ok = idx == len(expected)
        return (1.0 if ok else 0.0, "In-order match" if ok else f"Missing sequence; expected {expected}, got {actual}")

    # ANY_ORDER
    missing = [s for s in expected if s not in actual]
    ok = len(missing) == 0
    return (1.0 if ok else 0.0, "Any-order match" if ok else f"Missing steps: {missing}")


def run_suite(
    agent_fn: Callable[[str], str] = call_your_agent,
    use_llm_scoring: bool = True,
    traj_dir: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Enhanced suite runner with optional LLM-based scoring.
    
    Args:
        agent_fn: Function to call the agent
        use_llm_scoring: If True, use LLM for scoring; if False, use rule-based
        traj_dir: Directory containing per-scenario JSON files of actual trajectories
    """
    traj_dir = traj_dir or os.getenv("ORDER_EVAL_TRAJ_DIR")
    results = []
    for sc in SCENARIOS:
        prompt = build_prompt(sc["order"], sc["query"])
        agent_text = agent_fn(prompt)

        baseline = run_baseline_executor(sc["order"], sc["query"])
        
        # Choose scoring method
        if use_llm_scoring:
            scoring = score_agent_response_with_llm(agent_text, sc, baseline)
        else:
            scoring = score_agent_response_rule_based(agent_text, sc)

        traj_info = TRAJECTORY_EXPECTATIONS.get(sc["name"])
        actual_traj = _load_actual_trajectory(sc["name"], traj_dir)
        traj_score = None
        traj_max = None
        traj_note = "No expectation configured."
        if traj_info and actual_traj:
            traj_score, traj_note = score_trajectory(
                expected=traj_info["expected"],
                actual=actual_traj,
                match_type=traj_info.get("match_type", "IN_ORDER"),
            )
            traj_max = 1.0
        elif traj_info and not actual_traj:
            traj_note = "Trajectory file not found; set ORDER_EVAL_TRAJ_DIR or pass traj_dir."

        results.append({
            "scenario": sc["name"],
            "query": sc["query"],
            "agent_response": agent_text,
            "baseline_action": baseline.get("action"),
            "baseline_apply_message": baseline.get("apply_message"),
            "baseline_issues": baseline.get("issues"),
            "baseline_totals": baseline["state"]["totals"],
            "score": scoring["score"],
            "max_score": scoring["max_score"],
            "score_notes": scoring["notes"],
            "score_breakdown": scoring.get("breakdown"),  # Only present with LLM scoring
            "trajectory_expected": (traj_info or {}).get("expected") if traj_info else None,
            "trajectory_actual": actual_traj,
            "trajectory_match_type": (traj_info or {}).get("match_type") if traj_info else None,
            "trajectory_score": traj_score,
            "trajectory_max": traj_max,
            "trajectory_notes": traj_note,
        })

    total = 0.0
    total_max = 0.0
    for r in results:
        total += r["score"]
        total_max += r["max_score"]
        if r.get("trajectory_max"):
            total += r.get("trajectory_score", 0.0) or 0.0
            total_max += r["trajectory_max"]
    return {"total_score": total, "total_max": total_max, "results": results}


# -----------------------------
# CLI runner
# -----------------------------


def pretty_print_state(state_dict: Dict[str, Any]):
    totals = state_dict["totals"]
    print("\nTotals:")
    print(f"  Subtotal      : Rs {totals['subtotal']:,.2f}")
    print(f"  Discount      : Rs {totals['total_discount']:,.2f}")
    print(f"  Grand Total   : Rs {totals['grand_total']:,.2f}")
    print(f"  Profit        : Rs {totals['profit']:,.2f}")
    print(f"  Profit Margin : {totals['profit_margin_pct']:.2f}%")
    print("\nItems:")
    for it in state_dict["items"]:
        print(f"  - {it['name']} | {it['qty']} x Rs {it['price_discounted']:.2f} "
              f"(orig {it['price_original']:.2f}) | total Rs {it['item_total']:,.2f} | saving Rs {it['saving']:,.2f}")


def print_report(report: Dict[str, Any], show_breakdown: bool = False):
    """Helper function to print evaluation results."""
    print("\n=== Suite Report ===")
    print(f"Total Score: {report['total_score']:.1f} / {report['total_max']}\n")
    
    for r in report["results"]:
        print(f"--- {r['scenario']} ---")
        print("Query:", r["query"])
        
        # Truncate long agent responses
        agent_resp = r["agent_response"]
        if len(agent_resp) > 200:
            agent_resp = agent_resp[:200] + "..."
        print("Agent:", agent_resp)
        
        print("Baseline action:", r["baseline_action"])
        
        # Display score with breakdown if available
        score_display = f"{r['score']:.1f}/{r['max_score']}"
        if show_breakdown and r.get("score_breakdown"):
            breakdown = r["score_breakdown"]
            score_display += (
                f" (Correctness:{breakdown.get('correctness', 0):.1f} "
                f"Helpfulness:{breakdown.get('helpfulness', 0):.1f} "
                f"Clarity:{breakdown.get('clarity', 0):.1f})"
            )
        
        print("Score:", score_display)
        print("Notes:", r["score_notes"])

        if r.get("trajectory_expected"):
            traj_disp = f"{r.get('trajectory_score', 0)}/{r.get('trajectory_max', 0) or 0}"
            print("Trajectory:", traj_disp, "|", r.get("trajectory_match_type"), "|", r.get("trajectory_notes"))
            if show_breakdown:
                print("  expected:", r.get("trajectory_expected"))
                print("  actual  :", r.get("trajectory_actual"))
        print("Totals:", r["baseline_totals"])
        print()


def main():
    print("== Order Agent Simulator ==")
    print("1) Run evaluation suite (LLM-based scoring)")
    print("2) Run evaluation suite (Rule-based scoring)")
    print("3) Interactive local baseline (no LLM)")
    choice = input("Choose (1/2/3): ").strip()

    if choice == "1":
        print("\nRunning evaluation with LLM-based contextual scoring...\n")
        report = run_suite(call_your_agent, use_llm_scoring=True)
        print_report(report, show_breakdown=True)
        return
    
    elif choice == "2":
        print("\nRunning evaluation with rule-based scoring...\n")
        report = run_suite(call_your_agent, use_llm_scoring=False)
        print_report(report, show_breakdown=False)
        return

    # Interactive baseline (option 3)
    order = SAMPLE_ORDER
    state = parse_order_message(order)
    while True:
        print("\nCommands: load_order | load_recs | show | query <text> | undo | exit")
        cmd = input("> ").strip()
        if cmd == "exit":
            break
        if cmd == "load_order":
            order = SAMPLE_ORDER
            state = parse_order_message(order)
            print("Loaded sample order.")
        elif cmd == "load_recs":
            order = SAMPLE_RECS
            state = parse_order_message(order)
            print("Loaded sample recommendations.")
        elif cmd == "show":
            pretty_print_state(asdict(state))
        elif cmd == "undo":
            ok = state.restore_last_snapshot()
            print("Undone." if ok else "Nothing to undo.")
        elif cmd.startswith("query "):
            q = cmd[len("query "):]
            baseline = run_baseline_executor(order, q)
            state = OrderState(
                items=[OrderItem(**it) for it in baseline["state"]["items"]],
                totals=OrderTotals(**baseline["state"]["totals"]),
                raw_message=order,
                history=baseline["state"].get("history", []),
            )
            print(baseline.get("apply_message", "OK"))
            if baseline.get("issues"):
                print("Issues:")
                for iss in baseline["issues"]:
                    print(" -", iss)
            pretty_print_state(baseline["state"])
        else:
            print("Unknown command.")


if __name__ == "__main__":
    main()
