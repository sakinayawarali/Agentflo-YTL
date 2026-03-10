from __future__ import annotations

from typing import Dict, Tuple

# Prices are per cubic meter (m³), in RM.
PRICES: Dict[str, Tuple[float, float]] = {
    "G15": (250, 305),
    "G20": (270, 325),
    "G25": (290, 340),
    "G30": (300, 360),
    "G35": (320, 375),
    "G40": (350, 395),
    "G45": (380, 410),
}

