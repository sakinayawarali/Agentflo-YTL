from __future__ import annotations

from typing import Any, Dict, List

from agents.tools.pricing_data import PRICES


def _normalize_grade(grade: str) -> str:
    return (grade or "").strip().upper()


def _parse_positive_volume(volume: float) -> float | None:
    try:
        vol = float(volume)
    except (TypeError, ValueError):
        return None
    return vol if vol > 0 else None


def estimate_concrete_price(grade: str, volume: float) -> Dict[str, Any]:
    """
    Estimate concrete cost.
    """
    grade_key = _normalize_grade(grade)
    if not grade_key:
        return {
            "success": False,
            "error": "Missing grade (expected e.g. G25, G30).",
            "supported_grades": sorted(PRICES.keys()),
        }

    if grade_key not in PRICES:
        return {
            "success": False,
            "error": f"Unsupported grade: {grade_key}",
            "supported_grades": sorted(PRICES.keys()),
        }

    vol = _parse_positive_volume(volume)
    if vol is None:
        return {"success": False, "error": "Volume must be a number greater than 0."}

    min_price, max_price = PRICES[grade_key]
    est_range: List[float] = [round(min_price * vol, 2), round(max_price * vol, 2)]

    return {
        "success": True,
        "grade": grade_key,
        "volume_m3": round(vol, 2),
        "estimated_price_range_rm": est_range,
    }


def generate_quote(grade: str, volume: float, location: str) -> Dict[str, Any]:
    grade_key = _normalize_grade(grade)
    if not grade_key:
        return {
            "success": False,
            "error": "Missing grade (expected e.g. G25, G30).",
            "supported_grades": sorted(PRICES.keys()),
        }
    if grade_key not in PRICES:
        return {
            "success": False,
            "error": f"Unsupported grade: {grade_key}",
            "supported_grades": sorted(PRICES.keys()),
        }

    vol = _parse_positive_volume(volume)
    if vol is None:
        return {"success": False, "error": "Volume must be a number greater than 0."}

    loc = (location or "").strip()
    if not loc:
        return {"success": False, "error": "Missing project location."}

    min_price, max_price = PRICES[grade_key]
    return {
        "success": True,
        "project_location": loc,
        "grade": grade_key,
        "volume_m3": round(vol, 2),
        "estimated_cost_rm": {
            "min": round(vol * min_price, 2),
            "max": round(vol * max_price, 2),
        },
    }

