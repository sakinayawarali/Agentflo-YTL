from __future__ import annotations

from typing import Any, Dict, List

from agents.tools.pricing_data import PRICES, ALL_CONCRETE_PRICES


MIN_ORDER_M3 = 5.0


def _normalize_grade(grade: str) -> str:
    return (grade or "").strip().upper()


def _parse_positive_volume(volume: float) -> float | None:
    try:
        vol = float(volume)
    except (TypeError, ValueError):
        return None
    return vol if vol > 0 else None


def _volume_discount_pct(volume_m3: float) -> float:
    """
    Volume discount tiers (from knowledge/concrete_pricing.md):
    - Orders above 50 m³: 5%
    - Orders above 100 m³: 8%
    """
    v = float(volume_m3)
    if v > 100:
        return 8.0
    if v > 50:
        return 5.0
    return 0.0


def _apply_discount(value: float, pct: float) -> float:
    return value * (1.0 - (pct / 100.0))


def estimate_concrete_price(grade: str, volume: float) -> Dict[str, Any]:
    """
    Estimate concrete cost. Accepts a concrete grade (G25, G30) or
    an ECOConcrete product name (EcoBuild, FibreBuild, etc.).
    """
    grade_key = _normalize_grade(grade)
    if not grade_key:
        return {
            "success": False,
            "error": "Missing grade or product name (expected e.g. G25, G30, EcoBuild).",
            "supported": sorted(ALL_CONCRETE_PRICES.keys()),
        }

    if grade_key not in ALL_CONCRETE_PRICES:
        return {
            "success": False,
            "error": f"Unsupported grade/product: {grade_key}",
            "supported": sorted(ALL_CONCRETE_PRICES.keys()),
        }

    vol = _parse_positive_volume(volume)
    if vol is None:
        return {"success": False, "error": "Volume must be a number greater than 0."}

    min_price, max_price = ALL_CONCRETE_PRICES[grade_key]
    est_range: List[float] = [round(min_price * vol, 2), round(max_price * vol, 2)]

    discount_pct = _volume_discount_pct(vol)
    discounted_range: List[float] = [
        round(_apply_discount(est_range[0], discount_pct), 2),
        round(_apply_discount(est_range[1], discount_pct), 2),
    ]

    warnings: List[str] = []
    if vol < MIN_ORDER_M3:
        warnings.append(f"Minimum order is {int(MIN_ORDER_M3)} m³.")

    return {
        "success": True,
        "grade": grade_key,
        "volume_m3": round(vol, 2),
        "estimated_price_range_rm": est_range,
        "volume_discount_pct": discount_pct,
        "discounted_estimated_price_range_rm": discounted_range if discount_pct > 0 else est_range,
        "warnings": warnings,
    }


def generate_quote(grade: str, volume: float, location: str) -> Dict[str, Any]:
    """Generate a structured quote. Accepts grade (G25) or product name (EcoBuild)."""
    grade_key = _normalize_grade(grade)
    if not grade_key:
        return {
            "success": False,
            "error": "Missing grade or product name (expected e.g. G25, G30, EcoBuild).",
            "supported": sorted(ALL_CONCRETE_PRICES.keys()),
        }
    if grade_key not in ALL_CONCRETE_PRICES:
        return {
            "success": False,
            "error": f"Unsupported grade/product: {grade_key}",
            "supported": sorted(ALL_CONCRETE_PRICES.keys()),
        }

    vol = _parse_positive_volume(volume)
    if vol is None:
        return {"success": False, "error": "Volume must be a number greater than 0."}

    loc = (location or "").strip()
    if not loc:
        return {"success": False, "error": "Missing project location."}

    min_price, max_price = ALL_CONCRETE_PRICES[grade_key]
    discount_pct = _volume_discount_pct(vol)
    min_cost = round(vol * min_price, 2)
    max_cost = round(vol * max_price, 2)
    discounted_min = round(_apply_discount(min_cost, discount_pct), 2)
    discounted_max = round(_apply_discount(max_cost, discount_pct), 2)

    warnings: List[str] = []
    if vol < MIN_ORDER_M3:
        warnings.append(f"Minimum order is {int(MIN_ORDER_M3)} m³.")
    return {
        "success": True,
        "project_location": loc,
        "grade": grade_key,
        "volume_m3": round(vol, 2),
        "estimated_cost_rm": {"min": min_cost, "max": max_cost},
        "volume_discount_pct": discount_pct,
        "discounted_estimated_cost_rm": (
            {"min": discounted_min, "max": discounted_max} if discount_pct > 0 else {"min": min_cost, "max": max_cost}
        ),
        "warnings": warnings,
    }

