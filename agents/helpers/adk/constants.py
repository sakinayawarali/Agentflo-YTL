import re

TRIVIAL_GREETS = {
    "hi", "hello", "salam", "salaam", "hey", "yo", "aoa",
    "assalamualaikum", "as-salamu alaykum"
}

GOODBYE_RE = re.compile(r'\b(bye|goodbye|see\s+you|farewell|exit|quit|end)\b', re.IGNORECASE)
