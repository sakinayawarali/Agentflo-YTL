"""
Consolidates:
- agent.py → build_vn_text (VN modular prompt)
- adk_helper.py → _prepare_vn_text, _maybe_vn_reformat_llm, _clean_system_phrase
- tts.py (TTSMixin) → _shape_text_for_tts, _shrink_text_for_tts
"""
import asyncio
import os
import re
import time
from typing import Optional

from utils.logging import logger

# Import from audio utilities
from agents.audio.utils import (
    clean_store_name_for_vn,
    int_to_urdu_words,
    number_to_urdu_words,
)

try:
    from google import genai
    from google.genai import types
except ImportError:
    genai = None
    types = None

class VoiceNoteProcessor:
    """
    Process and format text for voice note generation.
    """
    
    def __init__(self, language: str = "ur", genai_client=None, model: str = None):
        """
        Initialize processor with explicit Google AI (Gemini) API key auth.
        """
        self.language = language.lower()
        self.tts_max_chars = int(os.getenv("TTS_MAX_CHARS", "150000"))
        # VN LLM reformat is opt-in (adds latency + variability). Enable explicitly via env.
        self.enable_llm_reformat = os.getenv("VN_LLM_REFORMAT_ENABLED", "false").lower() == "true"

        # YTL: keep LLM VN reformat OFF by default (opt-in via YTL_VN_LLM_REFORMAT_ENABLED=true).
        tenant = (os.getenv("TENANT_ID") or "").strip().lower()
        if tenant == "ytl" and os.getenv("YTL_VN_LLM_REFORMAT_ENABLED", "false").lower() != "true":
            if self.enable_llm_reformat:
                logger.info("vn_processor.ytl_config_applied", tenant=tenant, llm_reformat_enabled=False)
            self.enable_llm_reformat = False

        # Always target the Google AI Studio endpoint with an API key (non-Vertex path)
        self.api_key = (
            os.getenv("GEMINI_API_KEY")
            or os.getenv("GOOGLE_API_KEY")
            or os.getenv("GENAI_API_KEY")
        )
        self.model_id = model or "gemini-2.5-flash"

        if genai_client:
            self.client = genai_client
        elif genai and self.api_key and self.enable_llm_reformat:
            http_opts_cls = getattr(types, "HttpOptions", None)
            http_opts = http_opts_cls(
                baseUrl="https://generativelanguage.googleapis.com",
                apiVersion="v1",
            ) if http_opts_cls else None
            self.client = genai.Client(api_key=self.api_key, http_options=http_opts, vertexai=False)
        else:
            self.client = None
            if self.enable_llm_reformat:
                logger.warning(
                    "vn_processor.init_warning",
                    msg="No GEMINI_API_KEY/GOOGLE_API_KEY found; LLM reformat will be disabled.",
                )

        # Backwards-compat attributes used later in this class
        self.genai_client = self.client
        self.genai_available = self.client is not None
        self.llm_model = self.model_id
  

    async def process(self, text: str, lang_code: Optional[str] = None) -> str:
        cleaned = self.clean_system_phrases(text)
        final = await self.build_vn_text(cleaned, lang_code=lang_code)
        return self.shrink_if_needed(final)
    
    # ========================================================================
    # STEP 1: CLEAN SYSTEM PHRASES
    # ========================================================================
    
    def shape_for_tts(self, text: str) -> str:
        """
        Normalize text for natural TTS reading.
        """
        if not text:
            return ""
        
        # 0) Clean store names in parentheses
        s = clean_store_name_for_vn(text)
        
        # 1) Clean system phrases (belt & suspenders)
        s = self.clean_system_phrases(s)
        
        # Normalize newlines
        s = s.replace("\r\n", "\n").replace("\r", "\n")
        
        # 2) Remove markdown/bullet formatting AND numbered list markers
        
        # 2a) Bullet characters at start of line
        s = re.sub(r'^\s*[*\-•]+\s+', '', s, flags=re.MULTILINE)
        
        # 2b) Numbered list markers
        s = re.sub(r'^\s*\d+[\.\)]\s+', '', s, flags=re.MULTILINE)
        
        # 2c) Strip markdown bold/italic
        s = re.sub(r'\*\*(.+?)\*\*', r'\1', s)   # **bold**
        s = re.sub(r'\*(.+?)\*', r'\1', s)       # *italic*
        s = re.sub(r'__(.+?)__', r'\1', s)       # __bold__
        s = re.sub(r'_(.+?)_', r'\1', s)         # _italic_
        
        # 3) Collapse line breaks → spaces
        s = re.sub(r'\s+', ' ', s).strip()
        
        # 4) Convert order lines WITH total to spoken format
        # Pattern: "Item × 6 = 2145.48 Ruppay"
        line_pattern = re.compile(
            r'(?P<item>.+?)\s*[×xX]\s*(?P<qty>\d+)\s*=\s*(?P<total>[0-9]+(?:\.[0-9]+)?)\s*(?:Ruppay|PKR|Rs\.?)',
            re.IGNORECASE
        )
        
        def _line_repl(m):
            item = m.group("item").strip(" -*•")
            qty = m.group("qty")
            total_raw = m.group("total")
            
            # quantity as integer words
            qty_words = int_to_urdu_words(int(qty))
            
            # round total to whole rupees
            try:
                total_int = int(round(float(total_raw)))
            except Exception:
                if total_raw and total_raw.replace(".", "", 1).isdigit():
                    total_int = int(float(total_raw))
                else:
                    total_int = total_raw
            
            # convert amount to Urdu words
            if isinstance(total_int, int):
                total_words = int_to_urdu_words(total_int)
            else:
                total_words = number_to_urdu_words(str(total_int))
            
            # VN style: "Item, kul X, Y روپے kay"
            return f"{item} kul {qty_words}, {total_words} ruppay kay"
        
        s = line_pattern.sub(_line_repl, s)
        
        # 5) Handle lines WITHOUT total, e.g. "Item × 10"
        qty_only_pattern = re.compile(
            r'(?P<item>.+?)\s*[×xX]\s*(?P<qty>\d+)\b',
            re.IGNORECASE
        )
        
        def _qty_only_repl(m):
            item = m.group("item").strip(" -*•")
            qty = int(m.group("qty"))
            qty_words = int_to_urdu_words(qty)
            return f"{item} kul {qty_words}"
        
        s = qty_only_pattern.sub(_qty_only_repl, s)
        
        # 6) Handle standalone currency amounts
        # Pattern: "... 7967.47 Ruppay" → "saat hazaar ... روپے"
        currency_pattern = re.compile(
            r'(?P<amount>\d+(?:\.\d+)?)\s*(?:Ruppay|PKR|Rs\.?)',
            re.IGNORECASE
        )
        
        def _currency_repl(m):
            amount = float(m.group("amount"))
            words = number_to_urdu_words(str(amount))
            return words
        
        s = currency_pattern.sub(_currency_repl, s)
        
        # ADD THIS LINE:
        return s

    def clean_system_phrases(self, text: str) -> str:
        """
        Removes internal system artifacts like 'AI:', 'User:', or strictly technical
        prefixes that sometimes leak into the generated text.
        """
        if not text:
            return ""
        
        # Remove common chat prefixes
        t = re.sub(r'^(AI|Assistant|Bot|System):\s*', '', text, flags=re.IGNORECASE)
        
        # Remove markdown bolding often used for emphasis
        t = t.replace('**', '').replace('__', '')
        
        return t.strip()
    
    # ========================================================================
    # STEP 2: BUILD VN TEXT (VN MODULAR PROMPT)
    # ========================================================================
    
    def _reformat_text_llm(self, text, customer_name, store_name, is_roman, lang_code=None):
        if not self.client:
            return None

        regional_enabled = os.getenv("VN_REGIONAL_LANG_ENABLED", "false").lower() == "true"
        effective_lang = (lang_code or self.language) if regional_enabled else self.language
        system_prompt = self._get_language_prompt(effective_lang)

        try:
            response = self.client.models.generate_content(
                model=self.model_id,
                contents=[
                    types.Content(
                        parts=[
                            types.Part.from_text(
                                text=f"{system_prompt}\n\nReformat the following text:\n\n{text}"
                            )
                        ]
                    )
                ],
                config=types.GenerateContentConfig(
                    temperature=0.7,
                    max_output_tokens=81920,  # fix: 2000000 is invalid
                )
            )
            result = (response.text or "").strip()
            return result if result else None
        except Exception as e:
            logger.error("vn_processor.llm_call_failed", error=str(e))
            raise e

    async def build_vn_text(
        self,
        raw_text: str,
        customer_name: Optional[str] = None,
        store_name: Optional[str] = None,
        msg_type: str = "general",
        is_roman: bool = False,
        lang_code: Optional[str] = None,
    ) -> str:
        """
        Main entry point: Prepare text for TTS.
        """
        if not raw_text:
            return ""

        start_ts = time.perf_counter()
        llm_used = False

        text = self.clean_system_phrases(raw_text)
        
        # Step 1: LLM reformat (keeps numbers as digits per rule 4)
        if self.enable_llm_reformat and self.client:
            try:
                reformatted = await asyncio.to_thread(
                    self._reformat_text_llm, text, customer_name, store_name, is_roman, lang_code
                )
                if reformatted:
                    text = reformatted
                    llm_used = True
            except Exception as e:
                logger.error("vn_processor.build_vn_text.llm_error", error=str(e))
        
        # Step 2: Pattern-based formatting FIRST (needs digits to match)
        text = self.shape_for_tts(text)   # ← × qty = total patterns
        
        # Step 3: Convert remaining bare digits to Urdu words LAST
        text = self._shape_text_for_tts(text)  # ← number_to_urdu_words
        
        elapsed = time.perf_counter() - start_ts
        try:
            logger.info(
                "vn_processor.build_vn_text.timing",
                latency_sec=round(elapsed, 2),
                text_len=len(raw_text or ""),
                llm_used=llm_used,
            )
        except Exception:
            # Logging should never break VN generation.
            pass

        return text
    # ========================================================================
    # STEP 3: LANGUAGE-SPECIFIC LLM REFORMAT
    # ========================================================================
    
    def _get_language_prompt(self, lang_code: Optional[str] = None) -> str:
        """Get language-specific system prompt for LLM reformat."""
        lang = (lang_code or self.language or "ur").lower()
        
        if lang == "en":
            return """
You are converting WhatsApp text into natural ENGLISH voice-note scripts for TTS.

CRITICAL RULES:
1. LANGUAGE
- Output MUST be in clear, simple English only.
- Do NOT use Urdu or Roman Urdu words (no "bhai", "yaar", "acha", etc.).
- Use only Latin characters.

2. FORMATTING
- Remove ALL:
    - Bullet points (*, -, •)
    - Numbered lists ("1.", "2)", "3 -")
    - Extra line breaks between items
- Do NOT mention "bullet", "line 1", or similar meta language.

3. FLOW
- Make the output sound like a short spoken voice note.
- Use short, natural sentences.
- Keep the same information as the input, but make it clearer and smoother.

4. NUMBERS & CURRENCY
- Keep numeric values as digits (e.g., "1020").
- For PKR, "Rs", "rupees", normalize to "rupees" in the output.
- Add commas and pauses where needed for clarity, but do not overdo it.
- Do not say decimal points. Please make sure to round it up so if its 1029.8 its 1030

5. CONTENT PRESERVATION
- Do NOT drop important details such as:
    - Items, quantities, prices
    - Totals, discounts, promotions
    - Delivery or payment information
- You may compress obvious repetition, but preserve the meaning.

OUTPUT:
- Return ONLY the rewritten English voice-note script.
- No explanations, no tags, no extra formatting.
"""
        
        elif lang == "ar":
            return """
You are converting WhatsApp text into natural Modern Standard Arabic voice-note scripts for TTS.

CRITICAL RULES:
1. LANGUAGE
- Output MUST be in clear, simple Modern Standard Arabic.
- Use Arabic script for Arabic words.
- You may keep brand names in Latin characters if needed.

2. FORMATTING
- Remove ALL:
    - Bullet points (*, -, •)
    - Numbered lists ("1.", "2)", "3 -")
    - Extra line breaks between items
- Do NOT mention "first item", "second item" explicitly unless necessary for clarity.

3. FLOW
- Make the output sound like a short spoken audio message.
- Use short, direct sentences that are easy to follow.

4. NUMBERS & CURRENCY
- Keep numeric values as digits (e.g., "1020").
- For Pakistani rupees (PKR, Rs, rupee, rupees), normalize to "روبية" in the output.
- Place the total amount in a clear phrase, usually near the end.

5. CONTENT PRESERVATION
- Do NOT drop important details such as:
    - Items, quantities, and prices
    - Total amount, discounts, and offers
    - Delivery or payment information

OUTPUT:
- Return ONLY the rewritten Arabic voice-note script.
- No explanations, no tags, no extra formatting.
"""
        elif lang == "pa":  # Punjabi
            return """
You are converting WhatsApp text into natural Punjabi (Roman script) voice-note scripts for TTS.

CRITICAL RULES:
1. LANGUAGE
- Output in Roman Punjabi (Shahmukhi romanized) with light English brand names.
- Use only Latin characters. Sound like a friendly salesperson speaking Punjabi.
- Common Punjabi connectors: "te", "naal", "ki", "aa", "han", "thodi", "mere", "tere".

2. FORMATTING
- Remove ALL bullet points, numbered lists, extra line breaks.

3. FLOW
- Conversational, like a WhatsApp voice note to a shop owner.

4. NUMBERS & CURRENCY
- Keep numbers as digits. Normalize PKR/Rs → "rupay". Round decimals.

5. SUMMARIZATION (CRITICAL)
- More than 3 SKU lines: mention ONLY the first 3, then say "...te baaki items da detail text vich mil jayega."
- ALWAYS mention total order value and total discount at the end.

DO NOT SAY SALAM EVERY TIME. YOUVE SAID IT IN GREEING ALREADY
EVERYONE IS BHAI, BHAI JAAN OR BHAIYYA. NO ONE IS BAJJI

OUTPUT: Return ONLY the Roman Punjabi voice-note script. No tags, no extra formatting.
"""

        elif lang == "sd":  # Sindhi
            return """
You are converting WhatsApp text into natural Sindhi (Roman script) voice-note scripts for TTS.

CRITICAL RULES:
1. Output in Roman Sindhi with light English brand names. Latin characters only.
2. Sound like a friendly salesperson speaking Sindhi.
3. Remove ALL bullet points, numbered lists, extra line breaks.
4. Keep numbers as digits. Normalize PKR/Rs → "rupya". Round decimals.
5. More than 3 SKUs: mention first 3, then "...baqi items jo detail text mein milندو."
6. Always mention total order value and discount at the end.
DO NOT SAY SALAM EVERY TIME. YOUVE SAID IT IN GREEING ALREADY
EVERYONE IS BHAI, BHAI JAAN OR BHAIYYA. NO ONE IS BAJJI

OUTPUT: Return ONLY the Roman Sindhi voice-note script. No tags, no extra formatting.
"""

        elif lang == "ps":  # Pashto
            return """
You are converting WhatsApp text into natural Pashto (Roman script) voice-note scripts for TTS.

CRITICAL RULES:
1. Output in Roman Pashto with light English brand names. Latin characters only.
2. Sound like a friendly salesperson speaking Pashto.
3. Remove ALL bullet points, numbered lists, extra line breaks.
4. Keep numbers as digits. Normalize PKR/Rs → "rupay". Round decimals.
5. More than 3 SKUs: mention first 3, then "...noru items detail text ke ke."
6. Always mention total and discount at end.
DO NOT SAY SALAM EVERY TIME. YOUVE SAID IT IN GREEING ALREADY
EVERYONE IS BHAI, BHAI JAAN OR BHAIYYA. NO ONE IS BAJJI

OUTPUT: Return ONLY the Roman Pashto voice-note script. No tags, no extra formatting.
"""

        elif lang == "bal":  # Balochi
            return """
You are converting WhatsApp text into natural Balochi (Roman script) voice-note scripts for TTS.
Since Balochi TTS support is limited, write in Roman Balochi leaning on Urdu where needed.

CRITICAL RULES:
1. Output in Roman Balochi/Roman Urdu mix. Latin characters only.
2. Sound like a friendly salesperson.
3. Remove ALL bullet points, numbered lists, extra line breaks.
4. Keep numbers as digits. Normalize PKR/Rs → "rupay". Round decimals.
5. More than 3 SKUs: first 3 only, then say items are in text.
6. Always mention total and discount at end.
DO NOT SAY SALAM EVERY TIME. YOUVE SAID IT IN GREEING ALREADY
EVERYONE IS BHAI, BHAI JAAN OR BHAIYYA. NO ONE IS BAJJI

OUTPUT: Return ONLY the voice-note script. No tags, no extra formatting.
"""
        
        else:  # default: Urdu / Roman Urdu
            return """
You are converting Ayesha's WhatsApp text into natural voice note scripts for Urdu TTS.

CRITICAL RULES:
1. LANGUAGE
- Output in Roman Urdu with light English.
- Use only Latin characters (no Urdu or Arabic script).
- Sound like a friendly sales agent speaking to a shop owner.
- Whenever you see text like FHC CHOCOLATE CHIPS Packet understand that FHC stands for Farmhouse cookies, so say that. For WWS that means Whole Wheat Slices. If you dont know what a short form means, skip it.

2. FORMATTING
- Remove ALL:
    - Bullet points (*, -, •)
    - Numbered lists ("1.", "2)", "3 -")
    - Extra line breaks between items
- Do NOT mention "bullet", "line 1", or similar meta language.

3. FLOW
- Make it conversational, like a short WhatsApp voice note.
- Example style:
    - "acha bhai, main bata rahi hoon, aapke order mein ye items hain..."
- Use commas and short pauses naturally, but do not overdo it.

4. NUMBERS & CURRENCY
- Keep numbers as digits (e.g., "7", "1020") so they can be converted later.
- For PKR, Rs, rupee, etc., use "rupay" in Roman Urdu.
- Add commas to separate quantity from price so TTS does not run them together.
- DO NOT SAY DECIMALS ALWAYS ROUND UP NUMBERS eg if its 1020.9 its 1021

  5. SUMMARIZATION (CRITICAL)
    - If the order has more than 3 SKU lines, mention ONLY the first 3 items with quantities.
    - Then say: "...aur baaki items ka detail aapko text mein mil jayega."
    - ALWAYS mention the total order value and total discount at the end.
    - Do NOT read every single SKU — this is a voice note, not a receipt.

DO NOT SAY SALAM EVERY TIME. YOUVE SAID IT IN GREEING ALREADY
EVERYONE IS BHAI, BHAI JAAN OR BHAIYYA. NO ONE IS BAJJI

OUTPUT:
- Return ONLY the rewritten Roman Urdu voice-note script.
- No explanations, no tags, no extra formatting.
"""
    
    # ========================================================================
    # STEP 4: SHAPE FOR TTS
    # ========================================================================
    
    def _shape_text_for_tts(self, text: str) -> str:
        """
        Regex-based cleanup to ensure TTS reads numbers and currencies naturally in Urdu.
        """
        if not text: 
            return ""
            
        s = text
        
        # 1) Currency: "Rs. 500" -> "500 rupay"
        # Handle Rs, PKR, Rs., etc.
        s = re.sub(r'(?i)\b(?:Rs\.?|PKR)\s*(\d+(?:,\d+)*)', r'\1 rupay', s)
        
        # 2) Weight/Units: "5kg" -> "5 kilo"
        s = re.sub(r'(?i)(\d+)\s*kg\b', r'\1 kilo', s)
        s = re.sub(r'(?i)(\d+)\s*gm?s?\b', r'\1 gram', s)
        s = re.sub(r'(?i)(\d+)\s*ltr?s?\b', r'\1 liter', s)
        
        # 3) Normalize decimals "1.5" -> "1 point 5" (if not handled by num converter)
        # Actually number_to_urdu_words handles this, but let's prep strict patterns if needed
        
        # 4) Convert English digits to Urdu words if strict Urdu script is enforced,
        # but for Roman Urdu TTS (ElevenLabs), digits usually work fine. 
        # However, "500" reads as "five hundred" in some voices unless spelled out.
        # We will attempt to convert bare numbers to Roman Urdu phonetics.
        
        def _convert_match(match):
            num_str = match.group(0)
            return number_to_urdu_words(num_str)

        # 5) "x" for quantity: "3x" -> "3 adad" or "3 pieces"
        s = re.sub(r'(?i)\b(\d+)\s*[xX]\b', r'\1 adad', s)
        
        # 6) Range "5-10" -> "5 say 10"
        s = re.sub(r'(\d+)\s*-\s*(\d+)', r'\1 say \2', s)
        
        # 7) Date/Time "10/12" (Skipping complex date logic for now, basic regex)
        
        # 8) Clean up any "=" signs
        s = re.sub(r'\s*=\s*', ' ', s)
        
        # 9) Final cleanup
        s = re.sub(r'\s+', ' ', s).strip()

        # 10) Convert remaining numbers to words to ensure Urdu pronunciation
        # e.g. "500" -> "paanch sau"
        # We use a negative lookbehind/ahead to skip numbers already part of words if needed,
        # but straightforward substitution is usually safest for TTS.
        
        # Note: We skip this if the text looks like pure English, but assuming Urdu/Roman context:
        s = re.sub(r'\b\d+(?:\.\d+)?\b', _convert_match, s)
        
        return s
            
    # ========================================================================
    # STEP 5: SHRINK IF NEEDED
    # ========================================================================

    def shrink_if_needed(self, text: str) -> str:
        """DISABLED: Return text unchanged to prevent truncation."""
        return text  # ✅ Just return as-is, no cutting