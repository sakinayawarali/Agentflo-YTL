"""
Enhanced Conversation Evaluator for Sales Agent
Tracks business metrics, tool usage, and conversation quality.
"""

import os
import time
from typing import Optional, Dict, Any, List
from google.cloud import firestore
from google.genai import Client
from google.genai.types import HttpOptions
from utils.logging import logger
from agents.helpers.firestore_utils import get_tenant_id, user_root

class ConversationEvaluator:
    """
    Evaluates agent responses in real-time and stores comprehensive metrics in Firestore.
    Tracks both quality and business outcomes.
    """
    
    def __init__(self):
        self.db = firestore.Client()
        self.tenant_id = get_tenant_id()
        api_key = (
            os.getenv("GEMINI_API_KEY")
            or os.getenv("GOOGLE_API_KEY")
            or os.getenv("GENAI_API_KEY")
        )
        http_opts = HttpOptions(
            baseUrl="https://generativelanguage.googleapis.com",
            apiVersion="v1",
        )
        self.genai_client = Client(api_key=api_key, http_options=http_opts, vertexai=False) if api_key else Client(http_options=http_opts, vertexai=False)
        self.enabled = os.getenv("EVALUATION_ENABLED", "true").lower() == "true"
        
    def evaluate_turn(
        self,
        user_id: str,
        conversation_id: str,
        user_message: str,
        agent_response: str,
        message_type: str = "text",
        tools_used: Optional[List[str]] = None,
        response_time_ms: Optional[int] = None,
        gemini_usage: Optional[Dict] = None,
        eleven_tts_usage: Optional[Dict] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Evaluate a single turn with enhanced business metrics.
        
        Args:
            user_id: WhatsApp user ID
            conversation_id: Current conversation ID
            user_message: User's message
            agent_response: Agent's response
            message_type: text/audio/image/order
            tools_used: List of tool names used (optional)
            response_time_ms: How long agent took to respond (optional)
            gemini_usage: Gemini API usage data (optional)
            eleven_tts_usage: ElevenLabs usage data (optional)
        """
        if not self.enabled:
            return None
            
        try:
            # Get LLM evaluation with business context
            scores = self._score_with_llm(
                user_message, 
                agent_response, 
                message_type,
                tools_used or []
            )
            
            # Add metadata and business signals
            scores["timestamp"] = time.time()
            scores["user_id"] = user_id
            scores["conversation_id"] = conversation_id
            scores["message_type"] = message_type
            scores["tools_used"] = tools_used or []
            scores["response_time_ms"] = response_time_ms
            
            # Detect business events
            business_events = self._detect_business_events(
                user_message, 
                agent_response, 
                tools_used or []
            )
            scores["business_events"] = business_events
            
            # Add cost tracking
            if gemini_usage or eleven_tts_usage:
                scores["costs"] = {
                    "gemini_tokens": gemini_usage.get("total_tokens") if gemini_usage else 0,
                    "gemini_cost_usd": gemini_usage.get("cost", {}).get("total_cost_usd", 0) if gemini_usage else 0,
                    "tts_characters": eleven_tts_usage.get("input_characters") if eleven_tts_usage else 0,
                    "tts_cost_usd": 0  # Add calculation if needed
                }
            
            # Store in Firestore
            conv_ref = user_root(self.db, user_id, tenant_id=self.tenant_id).collection("conversations").document("meta")
            turn_ref = conv_ref.collection("turns").document()
            turn_ref.set({
                "user_message": user_message[:500],
                "agent_response": agent_response[:500],
                "evaluation": scores,
                "created_at": firestore.SERVER_TIMESTAMP
            })
            
            # Update conversation-level aggregates
            self._update_conversation_stats(user_id, scores)
            
            logger.info(
                "evaluation.completed",
                user_id=user_id,
                conversation_id=conversation_id,
                quality_score=scores.get("quality_score"),
                business_events=business_events,
                flags=scores.get("flags", [])
            )
            
            return scores
            
        except Exception as e:
            logger.error("evaluation.error", error=str(e), user_id=user_id)
            return None
    
    def _detect_business_events(
        self, 
        user_message: str, 
        agent_response: str,
        tools_used: List[str]
    ) -> List[str]:
        """
        Detect key business events in this turn.
        """
        events = []
        
        # Convert to lowercase for matching
        user_lower = user_message.lower()
        agent_lower = agent_response.lower()
        
        # Order-related events
        if "placeOrderTool" in tools_used:
            events.append("order_placed")
        if "confirmOrderDraftTool" in tools_used:
            events.append("order_confirmation_sent")
        if (
            "agentflo_cart_tool" in tools_used
            
        ):
            events.append("cart_updated")
        
        # Catalog events
        if "send_product_catalogue" in tools_used or "catalogue" in agent_lower or "catalog" in agent_lower:
            events.append("catalog_sent")
        
        # Product search/recommendation
        if "semantic_product_search" in tools_used or "search_products_by_sku" in tools_used:
            events.append("product_search")
        
        # User intent signals
        if any(word in user_lower for word in ["order", "place", "confirm", "kar do", "done"]):
            events.append("user_order_intent")
        if any(word in user_lower for word in ["nahi", "no", "cancel", "change"]):
            events.append("user_rejection")
        if any(word in user_lower for word in ["price", "rate", "kitna", "kya hai"]):
            events.append("user_price_query")
        if any(word in user_lower for word in ["hello", "hi", "salam", "assalam"]):
            events.append("user_greeting")
        if any(word in user_lower for word in ["thanks", "shukriya", "bye", "khuda hafiz"]):
            events.append("user_farewell")
        
        # Agent actions
        if "confirm" in agent_lower and "order" in agent_lower:
            events.append("agent_requesting_confirmation")
        if any(word in agent_lower for word in ["total", "amount", "rupay", "rs"]) and any(char.isdigit() for char in agent_response):
            events.append("agent_shared_pricing")
        
        return events
    
    def _score_with_llm(
        self,
        user_message: str,
        agent_response: str,
        message_type: str,
        tools_used: List[str]
    ) -> Dict[str, Any]:
        """
        Use Gemini to score the agent response with business context.
        """
        
        tools_str = ", ".join(tools_used) if tools_used else "none"
        
        prompt = f"""You are evaluating a WhatsApp sales conversation for Peek Freans biscuits. The agent is Ayesha (Roman Urdu, friendly tone).

USER MESSAGE: {user_message}

AGENT RESPONSE: {agent_response}

MESSAGE TYPE: {message_type}
TOOLS USED: {tools_str}

Evaluate on these criteria (1-5 scale, 5 = excellent):

**QUALITY SCORES:**
1. **Relevance** - Does response address user's query?
2. **Clarity** - Is it clear and easy to understand?
3. **Tone** - Appropriate Roman Urdu casual/friendly style?
4. **Sales Effectiveness** - Does it move toward a sale?
5. **Product Knowledge** - Accurate SKU/price/product info?
6. **Tool Usage** - Used right tools at right time?

**BUSINESS METRICS:**
- **order_progress** (0-100): How close to placing order? 0=just started, 50=cart building, 80=confirming, 100=order placed
- **customer_satisfaction** (1-5): How satisfied would customer be?
- **upsell_opportunity** (yes/no): Did agent miss chance to suggest more items?

**FLAGS** - Identify issues:
- `agent_confused` - Agent didn't understand or gave generic response
- `repetitive` - Agent repeating same info
- `wrong_product_info` - Incorrect SKU/price/details
- `missed_tool` - Should have used tool but didn't
- `tone_mismatch` - Wrong language style (too formal/English/etc)
- `no_cta` - Missing call-to-action when needed
- `price_error` - Wrong calculations or pricing
- `poor_upsell` - Missed obvious upsell opportunity
- `excellent_response` - Exceptionally good handling

Return ONLY valid JSON:
{{
  "quality_score": <1-5 overall>,
  "relevance": <1-5>,
  "clarity": <1-5>,
  "tone": <1-5>,
  "sales_effectiveness": <1-5>,
  "product_knowledge": <1-5>,
  "tool_usage": <1-5>,
  "order_progress": <0-100>,
  "customer_satisfaction": <1-5>,
  "upsell_opportunity": <true/false>,
  "flags": ["flag1", "flag2"],
  "reasoning": "Brief explanation",
  "recommendation": "What to improve",
  "strengths": "What was done well"
}}"""

        try:
            response = self.genai_client.models.generate_content(
                model="gemini-2.5-flash",
                contents=prompt
            )
            
            result_text = (response.text or "").strip()
            
            # Clean markdown fences
            if result_text.startswith("```"):
                lines = result_text.split("\n")
                result_text = "\n".join([l for l in lines if not l.strip().startswith("```")])
                if result_text.startswith("json"):
                    result_text = result_text[4:]
                result_text = result_text.strip()
            
            import json
            scores = json.loads(result_text)
            
            # Validate structure
            required_keys = ["quality_score", "relevance", "clarity", "tone", "sales_effectiveness"]
            if not all(k in scores for k in required_keys):
                raise ValueError("Missing required score keys")
            
            # Add excellent_response flag if quality is very high
            if scores.get("quality_score", 0) >= 5 and not scores.get("flags"):
                scores["flags"] = scores.get("flags", []) + ["excellent_response"]
                
            return scores
            
        except Exception as e:
            logger.warning("evaluation.llm_parse_failed", error=str(e))
            # Return safe defaults
            return {
                "quality_score": 3,
                "relevance": 3,
                "clarity": 3,
                "tone": 3,
                "sales_effectiveness": 3,
                "product_knowledge": 3,
                "tool_usage": 3,
                "order_progress": 50,
                "customer_satisfaction": 3,
                "upsell_opportunity": False,
                "flags": ["evaluation_failed"],
                "reasoning": f"Failed to parse: {str(e)}",
                "recommendation": "Manual review needed",
                "strengths": "Unknown"
            }
    
    def _update_conversation_stats(self, user_id: str, scores: Dict[str, Any]):
        """
        Update running conversation stats with business metrics.
        """
        try:
            conv_ref = user_root(self.db, user_id, tenant_id=self.tenant_id).collection("conversations").document("meta")
            doc = conv_ref.get()
            
            if not doc.exists:
                return
                
            data = doc.to_dict() or {}
            stats = data.get("evaluation_stats", {})
            
            # Initialize if first evaluation
            if not stats:
                stats = {
                    "total_turns": 0,
                    "avg_quality": 0,
                    "avg_relevance": 0,
                    "avg_clarity": 0,
                    "avg_tone": 0,
                    "avg_sales_effectiveness": 0,
                    "avg_product_knowledge": 0,
                    "avg_tool_usage": 0,
                    "avg_customer_satisfaction": 0,
                    "current_order_progress": 0,
                    "total_flags": 0,
                    "flag_types": {},
                    "business_events": {},
                    "excellent_responses": 0,
                    "problematic_responses": 0,
                    "total_cost_usd": 0
                }
            
            # Update running averages
            n = stats["total_turns"]
            stats["total_turns"] = n + 1
            
            metrics = [
                "quality_score", "relevance", "clarity", "tone", 
                "sales_effectiveness", "product_knowledge", "tool_usage",
                "customer_satisfaction"
            ]
            
            for metric in metrics:
                key = f"avg_{metric}" if metric != "quality_score" else "avg_quality"
                old_avg = stats.get(key, 0)
                new_value = scores.get(metric, 0)
                stats[key] = (old_avg * n + new_value) / (n + 1)
            
            # Update order progress (use latest, not average)
            stats["current_order_progress"] = scores.get("order_progress", stats.get("current_order_progress", 0))
            
            # Track flags
            flags = scores.get("flags", [])
            stats["total_flags"] += len(flags)
            
            flag_types = stats.get("flag_types", {})
            for flag in flags:
                flag_types[flag] = flag_types.get(flag, 0) + 1
            stats["flag_types"] = flag_types
            
            # Count excellent vs problematic
            if "excellent_response" in flags:
                stats["excellent_responses"] = stats.get("excellent_responses", 0) + 1
            
            problem_flags = ["agent_confused", "wrong_product_info", "price_error", "missed_tool"]
            if any(f in flags for f in problem_flags):
                stats["problematic_responses"] = stats.get("problematic_responses", 0) + 1
            
            # Track business events
            events = scores.get("business_events", [])
            event_counts = stats.get("business_events", {})
            for event in events:
                event_counts[event] = event_counts.get(event, 0) + 1
            stats["business_events"] = event_counts
            
            # Track costs
            if "costs" in scores:
                stats["total_cost_usd"] = stats.get("total_cost_usd", 0) + scores["costs"].get("gemini_cost_usd", 0)
            
            # Store latest evaluation
            stats["latest_evaluation"] = {
                "quality_score": scores.get("quality_score"),
                "order_progress": scores.get("order_progress"),
                "customer_satisfaction": scores.get("customer_satisfaction"),
                "flags": flags,
                "business_events": events,
                "timestamp": time.time()
            }
            
            # Calculate conversation health
            stats["health_score"] = self._calculate_health_score(stats)
            
            conv_ref.update({"evaluation_stats": stats})
            
        except Exception as e:
            logger.warning("evaluation.stats_update_failed", error=str(e))
    
    def _calculate_health_score(self, stats: Dict) -> str:
        """
        Calculate overall conversation health: excellent/good/needs_attention/poor
        """
        avg_quality = stats.get("avg_quality", 0)
        avg_satisfaction = stats.get("avg_customer_satisfaction", 0)
        total_turns = stats.get("total_turns", 1)
        total_flags = stats.get("total_flags", 0)
        problematic = stats.get("problematic_responses", 0)
        excellent = stats.get("excellent_responses", 0)
        
        flag_rate = total_flags / total_turns if total_turns > 0 else 0
        problem_rate = problematic / total_turns if total_turns > 0 else 0
        
        # Scoring logic
        if avg_quality >= 4.5 and avg_satisfaction >= 4.5 and flag_rate < 0.05:
            return "excellent"
        elif avg_quality >= 4.0 and avg_satisfaction >= 4.0 and flag_rate < 0.15:
            return "good"
        elif avg_quality >= 3.0 and problem_rate < 0.3:
            return "needs_attention"
        else:
            return "poor"
    
    
# Convenience function for integration
def evaluate_conversation_turn(
    user_id: str,
    conversation_id: str,
    user_message: str,
    agent_response: str,
    message_type: str = "text",
    tools_used: Optional[List[str]] = None,
    response_time_ms: Optional[int] = None,
    gemini_usage: Optional[Dict] = None,
    eleven_tts_usage: Optional[Dict] = None
) -> Optional[Dict[str, Any]]:
    """
    Quick function to evaluate a turn with full context.
    """
    evaluator = ConversationEvaluator()
    return evaluator.evaluate_turn(
        user_id=user_id,
        conversation_id=conversation_id,
        user_message=user_message,
        agent_response=agent_response,
        message_type=message_type,
        tools_used=tools_used,
        response_time_ms=response_time_ms,
        gemini_usage=gemini_usage,
        eleven_tts_usage=eleven_tts_usage
    )
