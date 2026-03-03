from __future__ import annotations

import csv
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple
from difflib import SequenceMatcher, get_close_matches

from dotenv import load_dotenv
from utils.logging import logger, debug_enabled

load_dotenv()

TOKEN_RE = re.compile(r"[a-zA-Z0-9]{2,}")

COLUMN_ALIASES = {
    "businessunit": "bussinessunitname",
    "businessunitname": "bussinessunitname",
    "unit": "bussinessunitname",
    "brand": "brandname",
    "category": "categoryname",
    "sku": "skucode",
    "skucode": "skucode",
    "skuvariant": "skuvariantname",
    "skuvariantname": "skuvariantname",
    "variant": "skuvariantname",
    "packtype": "skupacktypename",
    "packtypename": "skupacktypename",
    "pack": "skupacktypename",
    "piecesincarton": "piecesincarton",
    "piecespercarton": "piecesincarton",
    "piecesinbox": "piecesinbox",
    "piecesperbox": "piecesinbox",
    "packsperbox": "piecesinbox",
}

# Product name normalization rules
PRODUCT_SPELLING_VARIANTS = {
    # Sooper variants
    "ticky": "tikki",
    "tikky": "tikki",
    "tiki": "tikki",
    "tikey": "tikki",
    "tikee": "tikki",
    
    # Peak Freans variants
    "france": "freans",
    "friends": "freans",
    "freens": "freans",
    "frians": "freans",
    "frens": "freans",
    
    # Rio variants
    "real": "rio",
    "riyo": "rio",
    "rioo": "rio",
    "reo": "rio",
    
    # Cake Up variants
    "app": "up",
    "cakeup": "cake up",
    
    # Dairy variants
    "day": "dairy",
    "daily": "dairy",
    "dary": "dairy",
    
    # Common misspellings
    "sooper": "sooper",  # correct spelling
    "super": "sooper",
    "soper": "sooper",
    "supar": "sooper",
}

_CACHE: Dict[str, Any] = {
    "path": None,
    "mtime": None,
    "headers": [],
    "rows": [],
    "column_map": {},
}


def _tool_source(system_name: str) -> Dict[str, Any]:
    return {"system": system_name, "timestamp": int(time.time())}


def _tool_success(data: Any, system_name: str) -> Dict[str, Any]:
    return {"success": True, "data": data, "error": None, "source": _tool_source(system_name)}


def _tool_error(code: str, message: str, retryable: bool, system_name: str) -> Dict[str, Any]:
    return {
        "success": False,
        "data": None,
        "error": {"code": code, "message": message, "retryable": retryable},
        "source": _tool_source(system_name),
    }


def _normalize_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _normalize_value(value: Any, case_sensitive: bool) -> str:
    text = "" if value is None else str(value)
    text = re.sub(r"\s+", " ", text.strip())
    return text if case_sensitive else text.lower()


def _normalize_product_text(text: str) -> str:
    """
    Normalize product names by fixing common spelling variants.
    This is applied to both search queries and filter values.
    """
    if not text:
        return text
    
    normalized = text.lower()
    
    # Apply spelling corrections
    for wrong, right in PRODUCT_SPELLING_VARIANTS.items():
        # Word boundary matching to avoid partial replacements
        pattern = re.compile(rf'\b{re.escape(wrong)}\b', re.IGNORECASE)
        normalized = pattern.sub(right, normalized)
    
    return normalized


def _fuzzy_match_value(needle: str, haystack: str, threshold: float = 0.85) -> bool:
    """
    Check if needle approximately matches haystack using fuzzy string matching.
    
    Args:
        needle: The search term
        haystack: The value to match against
        threshold: Similarity threshold (0-1), default 0.85 means 85% similar
    
    Returns:
        True if match found, False otherwise
    """
    if not needle or not haystack:
        return False
    
    # Normalize both values
    needle_norm = _normalize_value(needle, False)
    haystack_norm = _normalize_value(haystack, False)
    
    # Apply product name normalization
    needle_norm = _normalize_product_text(needle_norm)
    haystack_norm = _normalize_product_text(haystack_norm)
    
    # Exact match after normalization
    if needle_norm == haystack_norm:
        return True
    
    # Fuzzy match using SequenceMatcher
    ratio = SequenceMatcher(None, needle_norm, haystack_norm).ratio()
    if ratio >= threshold:
        return True
    
    # Additional check: if one is contained in the other (for partial matches)
    # e.g., "tikki" matches "tikki pack"
    if len(needle_norm) >= 4 and (needle_norm in haystack_norm or haystack_norm in needle_norm):
        return True
    
    return False


def _match_filters_fuzzy(
    row: Dict[str, str], 
    filters: Dict[str, Any], 
    case_sensitive: bool,
    fuzzy: bool = True
) -> bool:
    """
    Enhanced filter matching with optional fuzzy support.
    
    Args:
        row: Row data from CSV
        filters: Filter conditions to check
        case_sensitive: Whether to match case-sensitively
        fuzzy: Whether to enable fuzzy matching
    
    Returns:
        True if row matches all filters, False otherwise
    """
    for key, expected in filters.items():
        actual = row.get(key)
        if actual is None:
            return False
        
        actual_cmp = _normalize_value(actual, case_sensitive)
        
        if isinstance(expected, list):
            # Match against any value in the list
            matched = False
            for item in expected:
                item_cmp = _normalize_value(item, case_sensitive)
                if fuzzy and _fuzzy_match_value(item_cmp, actual_cmp):
                    matched = True
                    break
                elif actual_cmp == item_cmp:
                    matched = True
                    break
            if not matched:
                return False
        else:
            # Single value match
            expected_cmp = _normalize_value(expected, case_sensitive)
            if fuzzy and _fuzzy_match_value(expected_cmp, actual_cmp):
                continue
            elif actual_cmp != expected_cmp:
                return False
    
    return True


def _singularize(word: str) -> str:
    if len(word) > 3 and word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def _parse_csv_list(raw: str) -> List[str]:
    return [item.strip() for item in (raw or "").split(",") if item.strip()]


def _resolve_csv_path() -> str:
    default_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "prompt", "product_info.csv")
    )
    raw_path = (os.getenv("PRODUCT_INFO_CSV_PATH") or "").strip()
    if raw_path:
        path = os.path.expanduser(os.path.expandvars(raw_path))
        if not os.path.isabs(path):
            cwd_path = os.path.abspath(os.path.join(os.getcwd(), path))
            if os.path.exists(cwd_path):
                return cwd_path
            repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
            path = os.path.abspath(os.path.join(repo_root, path))
        if os.path.exists(path):
            return path
    return default_path


def _build_column_map(headers: List[str]) -> Dict[str, str]:
    return {_normalize_key(h): h for h in headers if h}


def _resolve_column(name: str, column_map: Dict[str, str]) -> Optional[str]:
    key = _normalize_key(name)
    if key in column_map:
        return column_map[key]
    alias = COLUMN_ALIASES.get(key)
    if alias:
        alias_key = _normalize_key(alias)
        return column_map.get(alias_key)
    return None


def _resolve_columns(
    names: Optional[List[str]],
    column_map: Dict[str, str],
) -> Tuple[List[str], List[str]]:
    if not names:
        return [], []
    resolved: List[str] = []
    missing: List[str] = []
    for name in names:
        if not isinstance(name, str) or not name.strip():
            continue
        actual = _resolve_column(name, column_map)
        if actual:
            resolved.append(actual)
        else:
            missing.append(name)
    return resolved, missing


def _resolve_filters(
    filters: Dict[str, Any],
    column_map: Dict[str, str],
) -> Tuple[Dict[str, Any], List[str]]:
    resolved: Dict[str, Any] = {}
    missing: List[str] = []
    for key, value in (filters or {}).items():
        if not isinstance(key, str) or not key.strip():
            continue
        actual = _resolve_column(key, column_map)
        if actual:
            resolved[actual] = value
        else:
            missing.append(key)
    return resolved, missing


def _load_csv(path: str, refresh: bool) -> Dict[str, Any]:
    mtime = os.path.getmtime(path)
    if (
        _CACHE.get("path") == path
        and _CACHE.get("mtime") == mtime
        and not refresh
    ):
        return _CACHE

    rows: List[Dict[str, str]] = []
    headers: List[str] = []

    with open(path, newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle, skipinitialspace=True)
        headers = [h for h in (reader.fieldnames or []) if h]
        for row in reader:
            if not row:
                continue
            clean_row: Dict[str, str] = {}
            has_value = False
            for header in headers:
                value = row.get(header, "")
                text = "" if value is None else str(value).strip()
                if text:
                    has_value = True
                clean_row[header] = text
            if not has_value:
                continue
            rows.append(clean_row)
    column_map = _build_column_map(headers)
    _CACHE.update(
        {
            "path": path,
            "mtime": mtime,
            "headers": headers,
            "rows": rows,
            "column_map": column_map,
        }
    )
    return _CACHE


def _collect_value_map(rows: List[Dict[str, str]], column: Optional[str]) -> Dict[str, str]:
    values: Dict[str, str] = {}
    if not column:
        return values
    for row in rows:
        raw = row.get(column)
        if raw is None:
            continue
        norm = _normalize_value(raw, False)
        if norm and norm not in values:
            values[norm] = raw
    return values


def _adjust_unit_filters(
    filters: Dict[str, Any],
    *,
    unit_col: Optional[str],
    category_col: Optional[str],
    unit_map: Dict[str, str],
    category_map: Dict[str, str],
) -> Tuple[Dict[str, Any], Optional[Dict[str, str]]]:
    if not filters:
        return filters, None
    if not unit_col:
        return filters, None

    adjusted = dict(filters)
    adjustment: Optional[Dict[str, str]] = None

    if category_col and category_col in adjusted and unit_col not in adjusted:
        raw_val = adjusted.get(category_col)
        norm = _normalize_value(raw_val, False)
        singular = _singularize(norm)
        if norm and (norm in unit_map or singular in unit_map):
            if norm not in category_map and singular not in category_map:
                canonical = unit_map.get(norm) or unit_map.get(singular) or raw_val
                adjusted.pop(category_col, None)
                adjusted[unit_col] = canonical
                adjustment = {
                    "from": category_col,
                    "to": unit_col,
                    "value": str(canonical),
                }

    if unit_col in adjusted:
        raw_val = adjusted.get(unit_col)
        norm = _normalize_value(raw_val, False)
        singular = _singularize(norm)
        if norm not in unit_map and singular in unit_map:
            canonical = unit_map.get(singular) or raw_val
            adjusted[unit_col] = canonical
            adjustment = {
                "from": unit_col,
                "to": unit_col,
                "value": str(canonical),
            }

    return adjusted, adjustment


def _sort_values(values: List[str]) -> List[str]:
    if not values:
        return values
    if all(v.isdigit() for v in values):
        return [str(v) for v in sorted({int(v) for v in values})]
    return sorted({str(v) for v in values}, key=lambda v: v.lower())


def product_info_csv_tool(
    action: str,
    *,
    query: Optional[str] = None,
    filters: Optional[Dict[str, Any]] = None,
    column: Optional[str] = None,
    columns: Optional[List[str]] = None,
    search_columns: Optional[List[str]] = None,
    limit: int = 0,
    case_sensitive: bool = False,
    refresh: bool = False,
    fuzzy: bool = True,
) -> Dict[str, Any]:
    """
    CSV-backed product info lookup with fuzzy matching support.

    Actions:
    - schema: return column names and row count.
    - list: return rows that match filters (with fuzzy support).
    - search: full-text search across columns with fuzzy matching.
    - distinct: distinct values for a column (optional filters).
    - count: count rows matching filters.
    - count_distinct: count distinct values for a column (optional filters).
    - group_count: counts per value for a column (optional filters).

    Args:
        fuzzy: Enable fuzzy matching for filters and search (default: True)
               This helps match "tikki" vs "ticky", "freans" vs "france", etc.

    Use this tool for static catalog metadata (brands, categories, pack types,
    unit counts per box/carton, and totals). Do NOT use it for pricing or
    availability; use semantic_product_search or search_products_by_sku for that.
    """
    system_name = "product_info_csv_tool"
    action = (action or "").strip().lower()
    query = (query or "").strip()
    filters = filters or {}

    valid_actions = {
        "schema",
        "list",
        "search",
        "distinct",
        "count",
        "count_distinct",
        "group_count",
    }
    if action not in valid_actions:
        return _tool_error(
            "INVALID_ACTION",
            f"Unsupported action '{action}'. Valid actions: {', '.join(sorted(valid_actions))}.",
            False,
            system_name,
        )

    path = _resolve_csv_path()
    if not os.path.exists(path):
        return _tool_error(
            "FILE_NOT_FOUND",
            f"CSV file not found at {path}.",
            False,
            system_name,
        )

    if debug_enabled():
        logger.info(
            "tool.call",
            tool=system_name,
            action=action,
            query_preview=query[:120] if query else None,
            filters=list(filters.keys())[:8] if filters else None,
            fuzzy=fuzzy,
        )

    try:
        cache = _load_csv(path, refresh)
    except (OSError, csv.Error) as exc:
        return _tool_error(
            "CSV_READ_ERROR",
            f"Failed to read CSV: {exc}",
            True,
            system_name,
        )

    headers = cache.get("headers") or []
    rows = cache.get("rows") or []
    column_map = cache.get("column_map") or {}
    unit_col = _resolve_column("bussinessunitname", column_map)
    category_col = _resolve_column("categoryname", column_map)
    unit_map = _collect_value_map(rows, unit_col)
    category_map = _collect_value_map(rows, category_col)

    if action == "schema":
        return _tool_success(
            {"columns": headers, "row_count": len(rows), "path": path},
            system_name,
        )

    resolved_filters, missing_filters = _resolve_filters(filters, column_map)
    resolved_filters, filter_adjustment = _adjust_unit_filters(
        resolved_filters,
        unit_col=unit_col,
        category_col=category_col,
        unit_map=unit_map,
        category_map=category_map,
    )
    if missing_filters and not resolved_filters:
        return _tool_error(
            "INVALID_FILTERS",
            f"No matching filter columns. Available: {', '.join(headers)}",
            False,
            system_name,
        )

    resolved_columns: List[str] = []
    missing_columns: List[str] = []
    if columns:
        resolved_columns, missing_columns = _resolve_columns(columns, column_map)
        if columns and not resolved_columns:
            return _tool_error(
                "INVALID_COLUMNS",
                f"No matching columns. Available: {', '.join(headers)}",
                False,
                system_name,
            )

    resolved_search_columns: List[str] = []
    missing_search_columns: List[str] = []
    if search_columns:
        resolved_search_columns, missing_search_columns = _resolve_columns(search_columns, column_map)
        if search_columns and not resolved_search_columns:
            return _tool_error(
                "INVALID_SEARCH_COLUMNS",
                f"No matching search_columns. Available: {', '.join(headers)}",
                False,
                system_name,
            )

    max_results = int(os.getenv("PRODUCT_INFO_MAX_RESULTS", "50") or 50)
    if max_results <= 0:
        max_results = 50
    if limit <= 0 or limit > max_results:
        limit = max_results

    default_columns = _parse_csv_list(os.getenv("PRODUCT_INFO_DEFAULT_COLUMNS", ""))
    if not resolved_columns:
        resolved_columns, _ = _resolve_columns(default_columns, column_map)
        if not resolved_columns:
            resolved_columns = headers

    # Apply filters with fuzzy matching support
    matched_rows: List[Dict[str, str]] = []
    for row in rows:
        if resolved_filters and not _match_filters_fuzzy(row, resolved_filters, case_sensitive, fuzzy):
            continue
        matched_rows.append(row)

    if action == "count":
        data = {
            "action": action,
            "filters": resolved_filters,
            "missing_filters": missing_filters,
            "filter_adjustment": filter_adjustment,
            "row_count": len(matched_rows),
            "fuzzy_enabled": fuzzy,
        }
        return _tool_success(data, system_name)

    if action in ("distinct", "count_distinct", "group_count"):
        if not column:
            return _tool_error(
                "MISSING_COLUMN",
                "Column is required for this action.",
                False,
                system_name,
            )
        actual_col = _resolve_column(column, column_map)
        if not actual_col:
            return _tool_error(
                "INVALID_COLUMN",
                f"Unknown column '{column}'. Available: {', '.join(headers)}",
                False,
                system_name,
            )

        values = [row.get(actual_col, "") for row in matched_rows if row.get(actual_col) is not None]
        if action == "distinct":
            data = {
                "action": action,
                "column": actual_col,
                "filters": resolved_filters,
                "missing_filters": missing_filters,
                "filter_adjustment": filter_adjustment,
                "count": len(set(values)),
                "values": _sort_values(list(set(values))),
                "fuzzy_enabled": fuzzy,
            }
            return _tool_success(data, system_name)

        if action == "count_distinct":
            data = {
                "action": action,
                "column": actual_col,
                "filters": resolved_filters,
                "missing_filters": missing_filters,
                "filter_adjustment": filter_adjustment,
                "count": len(set(values)),
                "fuzzy_enabled": fuzzy,
            }
            return _tool_success(data, system_name)

        counts: Dict[str, int] = {}
        for value in values:
            key = value or ""
            counts[key] = counts.get(key, 0) + 1
        data = {
            "action": action,
            "column": actual_col,
            "filters": resolved_filters,
            "missing_filters": missing_filters,
            "filter_adjustment": filter_adjustment,
            "counts": dict(sorted(counts.items(), key=lambda item: (-item[1], item[0]))),
            "fuzzy_enabled": fuzzy,
        }
        return _tool_success(data, system_name)

    if action == "search":
        if not query:
            return _tool_error(
                "MISSING_QUERY",
                "Query is required for search.",
                False,
                system_name,
            )

        # Normalize query with product spelling corrections
        normalized_query = _normalize_product_text(query) if fuzzy else query
        query_norm = normalized_query if case_sensitive else normalized_query.lower()
        tokens = TOKEN_RE.findall(query_norm)
        
        results: List[Tuple[int, int, Dict[str, str]]] = []
        for idx, row in enumerate(matched_rows):
            if resolved_search_columns:
                haystack = " ".join(row.get(col, "") for col in resolved_search_columns)
            else:
                haystack = " ".join(row.values())
            
            # Normalize haystack for comparison
            if fuzzy:
                haystack = _normalize_product_text(haystack)
            haystack = haystack if case_sensitive else haystack.lower()
            
            # Calculate match score
            score = 0
            
            # Token matching
            for token in tokens:
                if token in haystack:
                    score += 1
            
            # Exact phrase match bonus
            if query_norm in haystack:
                score += 2
            
            # Fuzzy match bonus
            if fuzzy and score == 0:
                # Try fuzzy matching the entire query against each field
                for col_value in row.values():
                    if _fuzzy_match_value(query, col_value):
                        score += 1
                        break
            
            if score <= 0:
                continue
            
            results.append((score, idx, row))

        results.sort(key=lambda item: (-item[0], item[1]))
        output_rows: List[Dict[str, Any]] = []
        for score, _, row in results[:limit]:
            payload = {col: row.get(col, "") for col in resolved_columns}
            payload["__score__"] = score
            output_rows.append(payload)

        data = {
            "action": action,
            "query": query,
            "normalized_query": normalized_query if fuzzy and normalized_query != query else None,
            "filters": resolved_filters,
            "missing_filters": missing_filters,
            "filter_adjustment": filter_adjustment,
            "columns": resolved_columns,
            "missing_search_columns": missing_search_columns,
            "matched_rows": len(results),
            "returned_rows": len(output_rows),
            "rows": output_rows,
            "fuzzy_enabled": fuzzy,
        }
        return _tool_success(data, system_name)

    if action == "list":
        output_rows: List[Dict[str, Any]] = []
        for row in matched_rows[:limit]:
            output_rows.append({col: row.get(col, "") for col in resolved_columns})

        data = {
            "action": action,
            "filters": resolved_filters,
            "missing_filters": missing_filters,
            "filter_adjustment": filter_adjustment,
            "columns": resolved_columns,
            "missing_columns": missing_columns,
            "row_count": len(matched_rows),
            "returned_rows": len(output_rows),
            "rows": output_rows,
            "fuzzy_enabled": fuzzy,
        }
        return _tool_success(data, system_name)

    return _tool_error(
        "UNHANDLED_ACTION",
        f"Action '{action}' is not implemented.",
        False,
        system_name,
    )