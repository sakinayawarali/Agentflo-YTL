import re

from agents.helpers.adk.constants import GOODBYE_RE, TRIVIAL_GREETS


class MiscMixin:
    # ---------- Misc ----------
    def _clean_system_phrase(self, text: str) -> str:
        """
        Normalize any 'system mein nahi mil raha' style phrases to
        'ye mere paas nahi hai', for both text and VN.
        """
        if not text:
            return text

        patterns = [
            r"system\s*mein\s*nahi\s*mil\s*raha",
            r"system\s*me\s*nahi\s*mil\s*raha",
            r"system\s*mein\s*nahi\s*mil\s*rahi",
            r"system\s*me\s*nahi\s*mil\s*rahi",
        ]

        for pat in patterns:
            text = re.sub(pat, "ye mere paas nahi hai", text, flags=re.IGNORECASE)

        return text

    def _is_trivial_greeting(self, msg: str) -> bool:
        if not msg:
            return False
        m = msg.strip().lower()
        m = "".join(ch for ch in m if ch.isalnum() or ch.isspace())
        return m in TRIVIAL_GREETS or (len(m) <= 8 and any(g in m for g in TRIVIAL_GREETS))

    def is_goodbye_message(self, message: str) -> bool:
        return bool(GOODBYE_RE.search((message or "").strip()))

    def _maybe_invoice_sent(self, text: str) -> bool:
        """Heuristic: detect invoice share in plain text (tool-level trigger is still preferred)."""
        if not text:
            return False
        t = text.lower()
        has_invoice = "invoice" in t
        has_link_or_id = ("http" in t) or ("invoice id" in t) or ("invoice_id" in t) or ("inv-" in t)
        return has_invoice and has_link_or_id
