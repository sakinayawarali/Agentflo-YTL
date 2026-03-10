from __future__ import annotations

from typing import Any, Dict


def get_concrete_technical_properties() -> Dict[str, Any]:
    """
    Return standard technical properties for YTL ready-mix concrete (from knowledge/concrete_products.md).
    """
    return {
        "success": True,
        "typical_slump_range_mm": [75, 120],
        "pump_slump_range_mm": [120, 180],
        "maximum_aggregate_size_mm": 20,
        "initial_setting_time_hours": [1.5, 2.0],
        "final_setting_time_hours": [6.0, 8.0],
        "curing_period_min_days": 7,
        "maximum_delivery_time_minutes": 90,
        "source": "knowledge/concrete_products.md",
    }

