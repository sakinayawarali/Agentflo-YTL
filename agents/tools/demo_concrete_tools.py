from __future__ import annotations

import json
import math
import os
import re
from datetime import datetime
from typing import Any, Dict, Optional


def _knowledge_dir() -> str:
    # agents/tools/ -> agents/ -> Agentflo-YTL/ -> YTL-Agent/
    base = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # base == Agentflo-YTL
    return os.path.join(os.path.dirname(base), "knowledge")


def _load_ops() -> dict:
    path = os.path.join(_knowledge_dir(), "operations_demo.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            obj = json.load(f)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Great-circle distance between two points on Earth (km).
    """
    r = 6371.0
    p1 = math.radians(lat1)
    p2 = math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2) + math.cos(p1) * math.cos(p2) * (math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return r * c


def _coerce_float(val: Any) -> Optional[float]:
    try:
        if val is None:
            return None
        return float(val)
    except Exception:
        return None



def recommend_concrete_grade(project_type: str) -> Dict[str, Any]:
    """
    Recommend concrete grade(s) for ANY construction project.
    ALWAYS call this tool when a user mentions what they are building.
    Accepts any project description including:
    - Building types: house, unit house, bungalow, terrace, townhouse, semi-d, apartment, condo, shop, warehouse, factory
    - Structural elements: foundation, slab, beam, column, driveway, wall, pool, bridge
    - Any free-text description of the project
    Returns recommended grades for each structural part if it is a whole-building project.
    """
    text = (project_type or "").strip().lower()
    if not text:
        return {"success": False, "error": "Missing project_type (e.g., house, foundation, slabs, commercial building)."}

    rules = [
        ("house foundation", "G25 or G30"),
        ("residential slabs", "G25"),
        ("residential slab", "G25"),
        ("commercial slabs", "G30 – G35"),
        ("commercial slab", "G30 – G35"),
        ("industrial floors", "G35 – G40"),
        ("industrial floor", "G35 – G40"),
        ("high-rise structures", "G40 – G45"),
        ("high-rise", "G40 – G45"),
        ("highrise", "G40 – G45"),
    ]

    for key, grade in rules:
        if key in text:
            return {"success": True, "project_type": key, "recommended_grade": grade}

    _whole_house_keywords = [
        "house", "unit house", "bungalow", "banglo", "semi-d", "semi d",
        "terrace", "terraced", "link house", "townhouse", "town house",
        "residential", "rumah", "home", "apartment", "condo", "condominium",
        "flat", "duplex", "villa", "cottage", "cabin",
    ]
    if any(k in text for k in _whole_house_keywords):
        return {
            "success": True,
            "project_type": project_type,
            "is_whole_building": True,
            "recommended_grades": {
                "foundation": "G25 or G30",
                "ground_slab": "G25 (or FibreBuild for crack resistance)",
                "beams_columns": "G30",
                "upper_floor_slabs": "G25 – G30",
            },
            "eco_option": "EcoBuild (SKU16) – eco-friendly with lower CO₂",
            "note": "Ask customer which part they are pouring first, or provide a combined quote.",
        }

    _commercial_keywords = [
        "commercial", "office", "shop", "shoplot", "retail", "mall",
        "warehouse", "factory", "parking", "car park",
    ]
    if any(k in text for k in _commercial_keywords):
        return {
            "success": True,
            "project_type": project_type,
            "is_whole_building": True,
            "recommended_grades": {
                "foundation": "G30 – G35",
                "slabs": "G30 – G35",
                "beams_columns": "G35",
            },
            "eco_option": "SuperBuild (SKU20) – high compressive strength",
        }

    if "foundation" in text or "footing" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G25 or G30"}
    if "slab" in text and "commercial" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G30 – G35"}
    if "slab" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G25"}
    if "industrial" in text or "floor" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G35 – G40"}
    if "high" in text or "rise" in text or "tower" in text or "skyscraper" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G40 – G45"}
    if "column" in text or "beam" in text or "structural" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G30 – G35"}
    if "driveway" in text or "pavement" in text or "road" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G25 – G30"}
    if "wall" in text or "retaining" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G30"}
    if "bridge" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G35 – G40"}
    if "pool" in text or "swimming" in text or "tank" in text or "water" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "G30 (with waterproofing admixture)"}
    if "drain" in text or "stormwater" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "AquaBuild (SKU17) pervious concrete"}
    if "decorat" in text or "aesthetic" in text or "exposed" in text:
        return {"success": True, "project_type": project_type, "recommended_grade": "DecoBuild (SKU18) or FairBuild (SKU23)"}

    return {
        "success": True,
        "project_type": project_type,
        "recommended_grade": "G25 – G30 (general residential/structural)",
        "note": "For more precise recommendation, specify the structural element (foundation, slab, beam, column).",
    }


def recommend_pump(floor_height: int) -> Dict[str, Any]:
    """
    Simple pump recommender for demo purposes.
    """
    try:
        h = int(floor_height)
    except Exception:
        return {"success": False, "error": "Invalid floor_height (expected an integer)."}

    if h <= 1:
        return {"success": True, "floor_height": h, "pump_recommendation": "Pump not required"}
    if h <= 10:
        return {"success": True, "floor_height": h, "pump_recommendation": "36m pump recommended"}
    return {"success": True, "floor_height": h, "pump_recommendation": "52m pump recommended"}


def estimate_pump_needed(floor_height: int) -> Dict[str, Any]:
    """
    Alias for recommend_pump (kept to match requested tool name).
    """
    return recommend_pump(floor_height=floor_height)


def nearest_batching_plant(
    project_location: str = "",
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Nearest-plant selector.
    - Preferred: use latitude/longitude and plant coordinates in operations_demo.json.
    - Fallback: keyword matching on project_location (PJ / Shah Alam / Klang).
    Enforces the delivery radius (km) from operations_demo.json.
    """
    ops = _load_ops()
    radius = float(ops.get("delivery_radius_km", 40) or 40)
    plants = ops.get("plants") or []

    lat = _coerce_float(latitude)
    lon = _coerce_float(longitude)

    if lat is not None and lon is not None and isinstance(plants, list) and plants:
        best = None
        for p in plants:
            if not isinstance(p, dict):
                continue
            plat = _coerce_float(p.get("latitude"))
            plon = _coerce_float(p.get("longitude"))
            if plat is None or plon is None:
                continue
            d = _haversine_km(lat, lon, plat, plon)
            if best is None or d < best["distance_km"]:
                best = {
                    "name": p.get("name") or "Unknown Plant",
                    "capacity_m3_day": p.get("capacity_m3_day"),
                    "distance_km": d,
                }

        if best is None:
            return {"success": False, "error": "No plant coordinates available in operations_demo.json."}

        within = best["distance_km"] <= radius
        return {
            "success": bool(within),
            "project_location": (project_location or "").strip(),
            "site_coordinates": {"latitude": lat, "longitude": lon},
            "nearest_plant": best["name"],
            "distance_km": round(best["distance_km"], 2),
            "delivery_radius_km": radius,
            "serviceable": bool(within),
            "error": None if within else f"Site is outside delivery radius ({radius} km).",
            "rule": "Deliveries are served only from the nearest plant.",
        }

    # ---- fallback: keyword-based selection ----
    loc = (project_location or "").strip()
    if not loc:
        return {"success": False, "error": "Missing latitude/longitude or project_location."}

    norm = re.sub(r"\s+", " ", loc).strip().lower()
    if "petaling" in norm or "pj" in norm:
        plant = "Petaling Jaya Plant"
    elif "shah alam" in norm or "shah" in norm:
        plant = "Shah Alam Plant"
    elif "klang" in norm:
        plant = "Klang Plant"
    else:
        # default for demo: pick highest capacity plant
        plant = "Shah Alam Plant"
        try:
            if isinstance(plants, list) and plants:
                best = max(plants, key=lambda p: (p or {}).get("capacity_m3_day", 0))
                plant = (best or {}).get("name") or plant
        except Exception:
            pass

    return {
        "success": True,
        "project_location": loc,
        "nearest_plant": plant,
        "delivery_radius_km": radius,
        "serviceable": True,
        "note": "Fallback selection used (no coordinates provided). For accuracy, share a location pin.",
    }


def delivery_eta(
    project_location: str = "",
    delivery_date: Optional[str] = None,
    latitude: Optional[float] = None,
    longitude: Optional[float] = None,
) -> Dict[str, Any]:
    """
    Demo delivery ETA helper.
    Uses known operational constraint: max 90 minutes from batching plant.
    """
    plant_info = nearest_batching_plant(project_location=project_location, latitude=latitude, longitude=longitude)
    if not plant_info.get("success"):
        return plant_info

    date_note = None
    if delivery_date:
        try:
            # accepts YYYY-MM-DD or ISO strings; keep as informational only
            datetime.fromisoformat(str(delivery_date).replace("Z", "+00:00"))
            date_note = str(delivery_date)
        except Exception:
            date_note = str(delivery_date)

    return {
        "success": True,
        "project_location": project_location,
        "nearest_plant": plant_info.get("nearest_plant"),
        "distance_km": plant_info.get("distance_km"),
        "eta_guidance": "Delivery must be completed within 90 minutes from batching plant (operational limit).",
        "delivery_date": date_note,
        "note": "For exact ETA: require site address and scheduled dispatch time.",
    }

