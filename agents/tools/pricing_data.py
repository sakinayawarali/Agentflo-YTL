from __future__ import annotations

from typing import Dict, Tuple

# Grade-based prices per cubic meter (m³), in RM. (min, max)
PRICES: Dict[str, Tuple[float, float]] = {
    "G15": (250, 305),
    "G20": (270, 325),
    "G25": (290, 340),
    "G30": (300, 360),
    "G35": (320, 375),
    "G40": (350, 395),
    "G45": (380, 410),
}

# ECOConcrete™ product prices per m³ (from catalog.csv).
# These are catalog list prices; min=max since they are fixed.
ECOCONCRETE_PRICES: Dict[str, Tuple[float, float]] = {
    "ECOBUILD": (299.90, 299.90),
    "AQUABUILD": (340.0, 340.0),
    "DECOBUILD": (360.0, 360.0),
    "FLOWBUILD PRO": (380.0, 380.0),
    "SUPERBUILD": (350.0, 350.0),
    "COOLBUILD": (330.0, 330.0),
    "FLOWBUILD": (320.0, 320.0),
    "FAIRBUILD": (340.0, 340.0),
    "RAPIDBUILD": (360.0, 360.0),
    "FLEXBUILD": (330.0, 330.0),
    "FIBREBUILD": (345.0, 345.0),
    "DESIGNATED CONCRETE": (299.90, 299.90),
}

# Merged lookup: grade-based + product-name-based
ALL_CONCRETE_PRICES: Dict[str, Tuple[float, float]] = {**PRICES, **ECOCONCRETE_PRICES}
