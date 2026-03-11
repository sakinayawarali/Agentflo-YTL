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
        ("house foundation", "G25 or G30", "EcoBuild", "SKU16", "eco-friendly, 20-55% lower CO₂"),
        ("residential slabs", "G25", "FibreBuild", "SKU26", "fibre reinforced, crack resistant"),
        ("residential slab", "G25", "FibreBuild", "SKU26", "fibre reinforced, crack resistant"),
        ("commercial slabs", "G30-G35", "FibreBuild", "SKU26", "fibre reinforced for commercial loads"),
        ("commercial slab", "G30-G35", "FibreBuild", "SKU26", "fibre reinforced for commercial loads"),
        ("industrial floors", "G35-G40", "FibreBuild", "SKU26", "enhanced tensile strength"),
        ("industrial floor", "G35-G40", "FibreBuild", "SKU26", "enhanced tensile strength"),
        ("high-rise structures", "G40-G45", "SuperBuild", "SKU20", "high compressive strength"),
        ("high-rise", "G40-G45", "SuperBuild", "SKU20", "high compressive strength"),
        ("highrise", "G40-G45", "SuperBuild", "SKU20", "high compressive strength"),
    ]

    for key, grade, product, sku, benefit in rules:
        if key in text:
            return {"success": True, "project_type": key, "product": product, "grade": grade, "sku": sku, "benefit": benefit}

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
            "recommendations": [
                {"item": "Foundation", "product": "EcoBuild", "grade": "G30", "sku": "SKU16", "benefit": "eco-friendly, 20-55% lower CO₂"},
                {"item": "Columns & Beams", "product": "EcoBuild", "grade": "G30-G35", "sku": "SKU16", "benefit": "structural strength with lower carbon"},
                {"item": "Ground Floor Slab", "product": "FibreBuild", "grade": "G25", "sku": "SKU26", "benefit": "fibre reinforced, crack resistant"},
                {"item": "Upper Floor Slabs", "product": "EcoBuild", "grade": "G25-G30", "sku": "SKU16", "benefit": "eco-friendly structural concrete"},
                {"item": "Bricklaying Mortar", "product": "Castle", "grade": "cement", "sku": "SKU01", "benefit": "Green Label certified, versatile"},
                {"item": "Plastering", "product": "Walcrete", "grade": "plastering cement", "sku": "SKU03", "benefit": "air-entraining agent, excellent adhesion"},
                {"item": "Skim Coat (Base)", "product": "Base Grey", "grade": "base coat", "sku": "SKU35", "benefit": "base coat before finish"},
                {"item": "Skim Coat (Finish)", "product": "QuickSkim", "grade": "finish coat", "sku": "SKU37", "benefit": "smooth final finish"},
                {"item": "Floor Screed", "product": "Floor Screed", "grade": "levelling", "sku": "SKU51", "benefit": "professional floor levelling"},
                {"item": "Tiling", "product": "Tile Adhesive", "grade": "standard tiles", "sku": "SKU54", "benefit": "professional grade adhesive"},
                {"item": "Driveway", "product": "DecoBuild", "grade": "decorative", "sku": "SKU18", "benefit": "stamped/exposed aggregate finishes"},
            ],
            "instruction": "Present each item with product name first, then send product cards for each SKU.",
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
            "recommendations": [
                {"item": "Foundation", "product": "SuperBuild", "grade": "G35", "sku": "SKU20", "benefit": "high compressive strength"},
                {"item": "Slabs", "product": "FibreBuild", "grade": "G30-G35", "sku": "SKU26", "benefit": "fibre reinforced, crack resistant"},
                {"item": "Columns & Beams", "product": "SuperBuild", "grade": "G35", "sku": "SKU20", "benefit": "heavy structural loads"},
                {"item": "Industrial Floors", "product": "FibreBuild", "grade": "G35-G40", "sku": "SKU26", "benefit": "enhanced tensile strength"},
                {"item": "Bricklaying Mortar", "product": "Castle", "grade": "cement", "sku": "SKU01", "benefit": "Green Label certified"},
                {"item": "Plastering", "product": "Walcrete", "grade": "plastering cement", "sku": "SKU03", "benefit": "air-entraining agent"},
                {"item": "Floor Screed", "product": "Floor Screed", "grade": "levelling", "sku": "SKU51", "benefit": "professional floor levelling"},
                {"item": "Tiling", "product": "SuperBond", "grade": "large format", "sku": "SKU52", "benefit": "heavy-duty for commercial spaces"},
            ],
            "instruction": "Present each item with product name first, then send product cards for each SKU.",
        }

    if "foundation" in text or "footing" in text:
        return {"success": True, "project_type": project_type, "product": "EcoBuild", "grade": "G25 or G30", "sku": "SKU16", "benefit": "eco-friendly, 20-55% lower CO₂"}
    if "slab" in text and "commercial" in text:
        return {"success": True, "project_type": project_type, "product": "FibreBuild", "grade": "G30-G35", "sku": "SKU26", "benefit": "fibre reinforced, crack resistant"}
    if "slab" in text:
        return {"success": True, "project_type": project_type, "product": "FibreBuild", "grade": "G25", "sku": "SKU26", "benefit": "fibre reinforced, crack resistant"}
    if "industrial" in text or "floor" in text:
        return {"success": True, "project_type": project_type, "product": "FibreBuild", "grade": "G35-G40", "sku": "SKU26", "benefit": "enhanced tensile strength for heavy loads"}
    if "high" in text or "rise" in text or "tower" in text or "skyscraper" in text:
        return {"success": True, "project_type": project_type, "product": "SuperBuild", "grade": "G40-G45", "sku": "SKU20", "benefit": "high compressive strength"}
    if "column" in text or "beam" in text or "structural" in text:
        return {"success": True, "project_type": project_type, "product": "EcoBuild", "grade": "G30-G35", "sku": "SKU16", "benefit": "eco-friendly structural concrete"}
    if "driveway" in text or "pavement" in text or "road" in text:
        return {"success": True, "project_type": project_type, "product": "DecoBuild", "grade": "G25-G30", "sku": "SKU18", "benefit": "decorative concrete finishes"}
    if "wall" in text or "retaining" in text:
        return {"success": True, "project_type": project_type, "product": "EcoBuild", "grade": "G30", "sku": "SKU16", "benefit": "eco-friendly retaining wall concrete"}
    if "bridge" in text:
        return {"success": True, "project_type": project_type, "product": "SuperBuild", "grade": "G35-G40", "sku": "SKU20", "benefit": "high-performance structural concrete"}
    if "pool" in text or "swimming" in text or "tank" in text or "water" in text:
        return {"success": True, "project_type": project_type, "product": "AquaBuild", "grade": "G30", "sku": "SKU17", "benefit": "waterproof pervious concrete"}
    if "drain" in text or "stormwater" in text:
        return {"success": True, "project_type": project_type, "product": "AquaBuild", "grade": "pervious", "sku": "SKU17", "benefit": "allows stormwater to percolate, meets SUDS requirements"}
    if "decorat" in text or "aesthetic" in text or "exposed" in text:
        return {"success": True, "project_type": project_type, "product": "DecoBuild or FairBuild", "grade": "decorative", "sku": "SKU18", "alt_sku": "SKU23", "benefit": "stamped/exposed aggregate or refined off-form finish"}

    return {
        "success": True,
        "project_type": project_type,
        "product": "EcoBuild",
        "grade": "G25-G30",
        "sku": "SKU16",
        "benefit": "eco-friendly general purpose concrete",
        "note": "For a more precise recommendation, specify the structural element (foundation, slab, beam, column).",
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

