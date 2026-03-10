from __future__ import annotations

import math
from typing import Any, Dict


def calculate_concrete_volume(length: float, width: float, thickness: float) -> Dict[str, float]:
    """
    Calculate required concrete volume in cubic meters.
    """
    volume = float(length) * float(width) * float(thickness)
    return {"volume_m3": round(volume, 2)}


def calculate_trucks_needed(volume: float) -> Dict[str, Any]:
    """
    Calculate number of mixer trucks required.
    """
    truck_capacity = 8
    vol = float(volume)
    trucks = math.ceil(vol / truck_capacity) if vol > 0 else 0

    return {
        "volume_m3": round(vol, 2),
        "truck_capacity": truck_capacity,
        "trucks_required": int(trucks),
    }

