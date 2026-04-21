from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Optional

import requests


OPENEI_UTILITY_RATES_URL = "https://api.openei.org/utility_rates"
EIA_RETAIL_SALES_URL = "https://api.eia.gov/v2/electricity/retail-sales/data/"

STATE_IDS = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
}


def _normalize_state_id(value: Optional[str]):
    normalized = str(value or "").strip()
    if not normalized:
        return None

    if len(normalized) == 2 and normalized.isalpha():
        return normalized.upper()

    return STATE_IDS.get(normalized.lower())


def _fetch_json(url: str, params: dict[str, Any], timeout: int = 15):
    response = requests.get(url, params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def _to_iso_date_from_timestamp(value: Any):
    try:
        timestamp = int(value)
    except (TypeError, ValueError):
        return None

    return datetime.fromtimestamp(timestamp, tz=timezone.utc).date().isoformat()


def _fetch_openei_utility_match(latitude: float, longitude: float):
    api_key = os.getenv("OPENEI_API_KEY")
    if not api_key:
        return None

    payload = _fetch_json(
        OPENEI_UTILITY_RATES_URL,
        params={
            "api_key": api_key,
            "version": "latest",
            "format": "json",
            "sector": "Residential",
            "lat": round(latitude, 6),
            "lon": round(longitude, 6),
            "country": "USA",
            "approved": "true",
            "is_default": "true",
            "detail": "minimal",
            "limit": 1,
            "radius": 25,
        },
    )
    items = payload.get("items") or []
    if not items:
        return None

    item = items[0]
    dgrules = item.get("dgrules")
    return {
        "utility_name": item.get("utility"),
        "rate_name": item.get("name"),
        "openei_label": item.get("label"),
        "openei_uri": item.get("uri"),
        "rate_effective_date": _to_iso_date_from_timestamp(item.get("startdate")),
        "export_compensation_type": dgrules,
        "net_metering_status": (
            "available"
            if isinstance(dgrules, str) and "net" in dgrules.lower()
            else "unknown"
        ),
        "tou_supported": None,
    }


def _fetch_eia_state_rate(state_id: Optional[str]):
    api_key = os.getenv("EIA_API_KEY")
    normalized_state_id = _normalize_state_id(state_id)
    if not api_key or not normalized_state_id:
        return None

    payload = _fetch_json(
        EIA_RETAIL_SALES_URL,
        params={
            "api_key": api_key,
            "data[]": "price",
            "facets[sectorid][]": "RES",
            "facets[stateid][]": normalized_state_id,
            "frequency": "monthly",
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "length": 1,
        },
    )
    rows = payload.get("response", {}).get("data") or payload.get("data") or []
    if not rows:
        return None

    row = rows[0]
    try:
        cents_per_kwh = float(row.get("price"))
    except (TypeError, ValueError):
        return None

    return {
        "state_id": normalized_state_id,
        "state_description": row.get("stateDescription"),
        "period": row.get("period"),
        "blended_kwh_rate": round(cents_per_kwh / 100, 4),
    }


def resolve_utility_context(address: Optional[dict[str, Any]], latitude: float, longitude: float):
    state_id = _normalize_state_id((address or {}).get("state"))
    openei_match = _fetch_openei_utility_match(latitude, longitude)
    eia_rate = _fetch_eia_state_rate(state_id)

    if not openei_match and not eia_rate:
        return None

    if openei_match and eia_rate:
        rate_source = "OpenEI utility match plus EIA residential retail price"
        confidence = "medium"
    elif eia_rate:
        rate_source = "EIA residential retail price"
        confidence = "medium"
    else:
        rate_source = "OpenEI utility match"
        confidence = "low"

    return {
        "utility_name": (openei_match or {}).get("utility_name") or (eia_rate or {}).get("state_description"),
        "rate_name": (openei_match or {}).get("rate_name") or "Residential average retail price",
        "rate_source": rate_source,
        "rate_effective_date": (eia_rate or {}).get("period") or (openei_match or {}).get("rate_effective_date"),
        "blended_kwh_rate": (eia_rate or {}).get("blended_kwh_rate"),
        "tou_supported": (openei_match or {}).get("tou_supported"),
        "export_compensation_type": (openei_match or {}).get("export_compensation_type"),
        "net_metering_status": (openei_match or {}).get("net_metering_status") or "unknown",
        "confidence": confidence,
        "state_id": (eia_rate or {}).get("state_id") or state_id,
        "source_details": {
            "openei_label": (openei_match or {}).get("openei_label"),
            "openei_uri": (openei_match or {}).get("openei_uri"),
            "eia_period": (eia_rate or {}).get("period"),
        },
    }
