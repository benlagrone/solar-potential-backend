from fastapi import FastAPI, HTTPException, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import logging
import math
import os
from data_persistence import (
    store_personal_info, store_browser_data, store_solar_data,
    check_existing_address_data, check_existing_solar_data, check_existing_zip_data,
    find_property_record_by_address, find_solar_quote, get_property_record, list_property_records,
    upsert_property_record, get_garden_crop_catalog,
    list_solar_quote_leads, store_solar_quote_lead,
    get_cached_property_climate, store_cached_property_climate,
    build_address_lookup_key, build_coordinate_lookup_key, get_geocode_cache, store_geocode_cache,
)
from property_context import get_property_context_snapshot
from live_conditions import (
    get_property_climate_snapshot,
    get_space_weather_snapshot,
    get_surface_irradiance_snapshot,
)
import uuid
from geopy.geocoders import Nominatim
from datetime import datetime, timedelta
import requests
from geopy.exc import GeocoderTimedOut
import ssl
import certifi
from timezonefinder import TimezoneFinder
import re
from types import SimpleNamespace
from typing import Optional

load_dotenv()

app = FastAPI(docs_url=None, redoc_url=None)  # Disable default docs

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Models
class Address(BaseModel):
    street: str
    city: str
    state: str
    zip: str
    country: str

class BrowserData(BaseModel):
    userAgent: str
    screenResolution: str
    languagePreference: str
    timeZone: str
    referrerUrl: str
    deviceType: str

class UserData(BaseModel):
    address: Address
    browserData: BrowserData

class RoofSelection(BaseModel):
    geometry: dict
    centroid: Optional[dict] = None
    areaSquareMeters: float
    areaSquareFeet: float
    recommendedKw: float


class GardenZone(BaseModel):
    id: str
    name: str
    geometry: dict
    centroid: Optional[dict] = None
    areaSquareMeters: float
    areaSquareFeet: float


class PreviewBounds(BaseModel):
    south: float
    north: float
    west: float
    east: float


class PropertyRecordRequest(BaseModel):
    guid: Optional[str] = None
    address: Address
    property_preview: Optional[dict] = None
    property_context: Optional[dict] = None
    property_climate: Optional[dict] = None
    roof_selection: Optional[RoofSelection] = None
    garden_zones: Optional[list[GardenZone]] = None


class PropertyRecordRecentRequest(BaseModel):
    max_items: int = 8
    require_garden_zones: bool = False


class SolarPotentialRequest(BaseModel):
    guid: str
    system_size: Optional[float] = 7.0  # retained for backward compatibility
    panel_efficiency: float = 0.20  # default 20%
    electricity_rate: float  # in $/kWh
    installation_cost_per_watt: float = 3.0  # in $/W, default $3/W
    roof_selection: Optional[RoofSelection] = None


class SolarReportRequest(BaseModel):
    guid: str
    panel_efficiency: float = 0.20
    electricity_rate: float
    installation_cost_per_watt: float = 3.0
    roof_selection: Optional[RoofSelection] = None
    report_name: Optional[str] = None


class SolarQuoteRequest(BaseModel):
    guid: str
    report_id: str


class SolarQuoteLeadRequest(BaseModel):
    full_name: str
    email: str
    phone: str
    preferred_contact: str = "phone"
    monthly_bill_range: Optional[str] = None
    install_timeline: Optional[str] = None
    notes: Optional[str] = None
    consent_to_contact: bool = False


class Coordinates(BaseModel):
    latitude: float
    longitude: float
    force_refresh: bool = False


class PropertyContextRequest(BaseModel):
    latitude: float
    longitude: float
    bounds: Optional[PreviewBounds] = None
    match_quality: Optional[str] = None

# Create a custom SSL context
ctx = ssl.create_default_context(cafile=certifi.where())
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


def build_nominatim_geolocator():
    def get_setting(name, default):
        value = os.getenv(name)
        if value is None:
            return default
        value = value.strip()
        return value or default

    return Nominatim(
        user_agent=get_setting("GEOCODER_USER_AGENT", "solar_potential_app"),
        domain=get_setting("GEOCODER_NOMINATIM_DOMAIN", "nominatim.openstreetmap.org"),
        scheme=get_setting("GEOCODER_NOMINATIM_SCHEME", "https"),
        ssl_context=ctx,
    )


geolocator = build_nominatim_geolocator()

logger = logging.getLogger(__name__)
ARCGIS_GEOCODE_URL = (
    "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates"
)
ARCGIS_REVERSE_GEOCODE_URL = (
    "https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/reverseGeocode"
)
MONTH_DAY_COUNTS = {
    "01": 31,
    "02": 28,
    "03": 31,
    "04": 30,
    "05": 31,
    "06": 30,
    "07": 31,
    "08": 31,
    "09": 30,
    "10": 31,
    "11": 30,
    "12": 31,
}
ROOF_COVERAGE_FACTOR = 0.565
EARTH_RADIUS_METERS = 6371000
NREL_PVWATTS_URL = os.getenv(
    "NREL_PVWATTS_URL",
    "https://developer.nlr.gov/api/pvwatts/v8.json",
)
NREL_PVWATTS_REFERENCE_SYSTEM_KW = 1.0
NREL_PVWATTS_DEFAULT_LOSSES = 14.0
NREL_PVWATTS_DEFAULT_ARRAY_TYPE = 1
NREL_PVWATTS_DEFAULT_MODULE_TYPE = 0
NREL_PVWATTS_DEFAULT_AZIMUTH = 180.0
NREL_PVWATTS_DEFAULT_INV_EFFICIENCY = 96.0
NREL_PVWATTS_DEFAULT_DATASET = "nsrdb"
NREL_PVWATTS_DEFAULT_RADIUS = 0
FORWARD_PROPERTY_PREVIEW_CACHE = "forward-property-preview"
REVERSE_PROPERTY_PREVIEW_CACHE = "reverse-property-preview"


def normalize_quality_percent(value):
    if value is None:
        return 0
    return round(value if value <= 100 else 100, 2)


def get_nrel_api_key():
    return os.getenv("NREL_API_KEY")


def month_key(index):
    return str(index).zfill(2)


def month_dict_from_sequence(values, digits=2):
    return {
        month_key(index + 1): round(float(value or 0), digits)
        for index, value in enumerate(values[:12])
    }


def estimate_pvwatts_tilt(latitude):
    return round(clamp(abs(float(latitude)), 10.0, 40.0), 1)


def resolve_solar_provider(solar_data):
    return (solar_data or {}).get("provider") or "nasa"


def get_geocoder_provider():
    provider = normalize_lookup_text(os.getenv("GEOCODER_PROVIDER", "hybrid"))
    return provider or "hybrid"


def format_address(address):
    region = " ".join(part for part in [address.get("state", ""), address.get("zip", "")] if part)
    parts = [
        address.get("street", ""),
        address.get("city", ""),
        region,
        address.get("country", ""),
    ]
    return ", ".join(part for part in parts if part)


def normalize_lookup_text(value):
    return re.sub(r"[^a-z0-9]+", " ", str(value).lower()).strip()


STREET_ABBREVIATIONS = {
    "aly": "alley",
    "ave": "avenue",
    "blvd": "boulevard",
    "cir": "circle",
    "ct": "court",
    "dr": "drive",
    "hwy": "highway",
    "ln": "lane",
    "pkwy": "parkway",
    "pl": "place",
    "rd": "road",
    "sq": "square",
    "st": "street",
    "ter": "terrace",
    "trl": "trail",
    "way": "way",
}


def normalize_lookup_tokens(value):
    return [
        STREET_ABBREVIATIONS.get(token, token)
        for token in normalize_lookup_text(value).split()
        if token
    ]


def normalize_street_text(value):
    return " ".join(normalize_lookup_tokens(value))


def extract_house_number(value):
    tokens = normalize_lookup_tokens(value)

    if tokens and tokens[0].isdigit():
        return tokens[0]

    return ""


def extract_street_name(value):
    tokens = normalize_lookup_tokens(value)

    if tokens and tokens[0].isdigit():
        tokens = tokens[1:]

    return " ".join(tokens)


def get_country_code(country_name):
    normalized = normalize_lookup_text(country_name)

    if normalized in {"united states", "usa", "us"}:
        return "us"

    return None


def build_structured_query(address):
    return {
        "street": address.get("street", ""),
        "city": address.get("city", ""),
        "state": address.get("state", ""),
        "postalcode": address.get("zip", ""),
        "country": address.get("country", ""),
    }


def score_geocode_candidate(address, location):
    raw_address = getattr(location, "raw", {}).get("address", {}) or {}
    candidate_address = extract_address_parts(raw_address)
    return score_address_match(address, candidate_address, location.address or "")


def assess_match_quality(score):
    if score >= 26:
        return "high"
    if score >= 16:
        return "medium"
    return "low"


def parse_bounding_box(location):
    raw_location = getattr(location, "raw", {}) or {}
    bounding_box = raw_location.get("boundingbox")

    if not bounding_box or len(bounding_box) != 4:
        return None

    south, north, west, east = [round(float(value), 6) for value in bounding_box]

    return {
        "south": south,
        "north": north,
        "west": west,
        "east": east,
    }


def get_state_value(raw_address):
    iso_subdivision = raw_address.get("ISO3166-2-lvl4", "")

    if iso_subdivision.startswith("US-"):
        return iso_subdivision.split("-", 1)[1]

    return raw_address.get("state", "")


def extract_address_parts(raw_address):
    house_number = raw_address.get("house_number", "").strip()
    road = (
        raw_address.get("road")
        or raw_address.get("pedestrian")
        or raw_address.get("footway")
        or raw_address.get("residential")
        or ""
    ).strip()
    street = " ".join(part for part in [house_number, road] if part).strip()
    city = (
        raw_address.get("city")
        or raw_address.get("town")
        or raw_address.get("village")
        or raw_address.get("hamlet")
        or raw_address.get("municipality")
        or raw_address.get("county")
        or ""
    )

    return {
        "street": street,
        "city": city,
        "state": get_state_value(raw_address),
        "zip": raw_address.get("postcode", ""),
        "country": raw_address.get("country", ""),
    }


def extract_arcgis_address_parts(raw_address):
    return {
        "street": raw_address.get("Address") or raw_address.get("ShortLabel") or "",
        "city": raw_address.get("City", ""),
        "state": raw_address.get("RegionAbbr", "") or raw_address.get("Region", ""),
        "zip": raw_address.get("Postal", ""),
        "country": raw_address.get("CntryName", "") or raw_address.get("CountryCode", ""),
    }


def score_address_match(requested_address, candidate_address, display_text=""):
    normalized_requested_street = normalize_street_text(requested_address.get("street", ""))
    normalized_candidate_street = normalize_street_text(candidate_address.get("street", ""))
    normalized_requested_street_name = extract_street_name(requested_address.get("street", ""))
    normalized_candidate_street_name = extract_street_name(candidate_address.get("street", ""))
    normalized_requested_city = normalize_lookup_text(requested_address.get("city", ""))
    normalized_candidate_city = normalize_lookup_text(candidate_address.get("city", ""))
    normalized_requested_state = normalize_lookup_text(requested_address.get("state", ""))
    normalized_candidate_state = normalize_lookup_text(candidate_address.get("state", ""))
    normalized_requested_zip = normalize_lookup_text(requested_address.get("zip", ""))
    normalized_candidate_zip = normalize_lookup_text(candidate_address.get("zip", ""))
    normalized_requested_country = normalize_lookup_text(requested_address.get("country", ""))
    normalized_candidate_country = normalize_lookup_text(candidate_address.get("country", ""))
    normalized_display = normalize_lookup_text(display_text)
    requested_house_number = extract_house_number(requested_address.get("street", ""))
    candidate_house_number = extract_house_number(candidate_address.get("street", ""))

    score = 0

    if requested_house_number and candidate_house_number:
        if requested_house_number == candidate_house_number:
            score += 8
        else:
            score -= 6

    if normalized_requested_zip and normalized_candidate_zip:
        if normalized_requested_zip == normalized_candidate_zip:
            score += 4
        else:
            score -= 4

    if normalized_requested_city and normalized_candidate_city:
        if normalized_requested_city in normalized_candidate_city:
            score += 3
        else:
            score -= 2

    if normalized_requested_state and normalized_candidate_state:
        if normalized_requested_state in normalized_candidate_state:
            score += 2
        else:
            score -= 2

    if normalized_requested_country and normalized_candidate_country:
        if normalized_requested_country in normalized_candidate_country:
            score += 1
        else:
            score -= 1

    if normalized_requested_street_name and normalized_candidate_street_name:
        if normalized_requested_street_name == normalized_candidate_street_name:
            score += 6
        else:
            requested_tokens = set(normalized_requested_street_name.split())
            candidate_tokens = set(normalized_candidate_street_name.split())
            overlap_count = len(requested_tokens & candidate_tokens)
            if overlap_count:
                score += min(overlap_count, 3)
            else:
                score -= 3

    if normalized_requested_street and normalized_requested_street in normalized_display:
        score += 2

    return score


def score_location_precision(location):
    raw_location = getattr(location, "raw", {}) or {}
    raw_address = raw_location.get("address", {}) or {}
    addresstype = normalize_lookup_text(raw_location.get("addresstype", ""))
    location_type = normalize_lookup_text(raw_location.get("type", ""))
    location_class = normalize_lookup_text(raw_location.get("class", ""))
    bounding_box = parse_bounding_box(location)
    score = 0

    if raw_address.get("house_number") and (
        raw_address.get("road")
        or raw_address.get("pedestrian")
        or raw_address.get("footway")
        or raw_address.get("residential")
    ):
        score += 4

    if addresstype in {"house", "building", "residential", "commercial", "amenity"}:
        score += 6
    elif addresstype in {"road", "pedestrian", "footway", "path"}:
        score -= 6

    if location_type in {"house", "building"}:
        score += 3
    elif location_type in {"road", "pedestrian", "footway", "path"}:
        score -= 4

    if location_class == "building":
        score += 2

    if get_location_source(location) == "arcgis-pointaddress":
        score += 6

    if is_road_centerline_house_match(location):
        score -= 6

    if bounding_box:
        max_span = max(
            abs(bounding_box["north"] - bounding_box["south"]),
            abs(bounding_box["east"] - bounding_box["west"]),
        )
        if max_span <= 0.0006:
            score += 5
        elif max_span <= 0.0015:
            score += 2
        elif max_span >= 0.004:
            score -= 4

    return score


def score_reverse_geocode_candidate(address, location):
    try:
        if is_arcgis_location(location):
            reverse_payload = reverse_geocode_arcgis_location(location.latitude, location.longitude)
            reverse_address = reverse_payload.get("address", {}) or {}
            candidate_address = extract_arcgis_address_parts(reverse_address)
            display_text = (
                reverse_address.get("LongLabel")
                or reverse_address.get("Match_addr")
                or reverse_address.get("Address")
                or ""
            )
            return score_address_match(address, candidate_address, display_text)

        reverse_location = reverse_geocode_location(location.latitude, location.longitude)
    except HTTPException as exc:
        logger.warning("Skipping reverse geocode validation for candidate: %s", exc.detail)
        return 0
    except Exception as exc:
        logger.warning("Skipping reverse geocode validation for candidate: %s", str(exc))
        return 0

    raw_address = getattr(reverse_location, "raw", {}).get("address", {}) or {}
    candidate_address = extract_address_parts(raw_address)
    return score_address_match(address, candidate_address, reverse_location.address or "")


def unique_geocode_key(location):
    raw_location = getattr(location, "raw", {}) or {}
    osm_type = raw_location.get("osm_type")
    osm_id = raw_location.get("osm_id")

    if osm_type and osm_id:
        return f"{osm_type}:{osm_id}"

    return (
        round(float(location.latitude), 6),
        round(float(location.longitude), 6),
        normalize_lookup_text(location.address or ""),
    )


def dedupe_geocode_candidates(candidates):
    seen = set()
    unique_candidates = []

    for location in candidates:
        key = unique_geocode_key(location)
        if key in seen:
            continue
        seen.add(key)
        unique_candidates.append(location)

    return unique_candidates


def build_bounding_box_from_extent(extent):
    if not extent:
        return None

    xmin = extent.get("xmin")
    xmax = extent.get("xmax")
    ymin = extent.get("ymin")
    ymax = extent.get("ymax")

    if None in {xmin, xmax, ymin, ymax}:
        return None

    return [str(ymin), str(ymax), str(xmin), str(xmax)]


def build_location(address, latitude, longitude, raw):
    return SimpleNamespace(
        address=address,
        latitude=float(latitude),
        longitude=float(longitude),
        raw=raw,
    )


def get_location_source(location):
    raw_location = getattr(location, "raw", {}) or {}
    return raw_location.get("source") or raw_location.get("provider") or "nominatim"


def is_arcgis_location(location):
    return "arcgis" in normalize_lookup_text(get_location_source(location))


def is_road_centerline_house_match(location):
    raw_location = getattr(location, "raw", {}) or {}

    return (
        raw_location.get("osm_type") == "way"
        and raw_location.get("class") == "place"
        and raw_location.get("type") == "house"
        and raw_location.get("addresstype") == "place"
    )


def fetch_arcgis_point_address(address):
    try:
        response = requests.get(
            ARCGIS_GEOCODE_URL,
            params={
                "SingleLine": format_address(address),
                "f": "pjson",
                "maxLocations": 1,
                "outFields": "*",
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        logger.warning("ArcGIS point-address fallback failed: %s", str(exc))
        return None
    except ValueError as exc:
        logger.warning("ArcGIS point-address fallback returned invalid JSON: %s", str(exc))
        return None

    candidate = ((data or {}).get("candidates") or [None])[0]
    if not candidate:
        return None

    attributes = candidate.get("attributes", {}) or {}
    addr_type = attributes.get("Addr_type")
    candidate_score = float(candidate.get("score") or attributes.get("Score") or 0)

    if addr_type != "PointAddress" or candidate_score < 99:
        return None

    latitude = attributes.get("Y") or candidate.get("location", {}).get("y")
    longitude = attributes.get("X") or candidate.get("location", {}).get("x")
    if latitude is None or longitude is None:
        return None

    return build_location(
        candidate.get("address") or format_address(address),
        latitude,
        longitude,
        {
            "provider": "arcgis",
            "source": "arcgis-pointaddress",
            "match_type": addr_type,
            "score": candidate_score,
            "boundingbox": build_bounding_box_from_extent(candidate.get("extent")),
            "address": {
                "house_number": attributes.get("AddNum", ""),
                "road": " ".join(
                    part
                    for part in [
                        attributes.get("StName", ""),
                        attributes.get("StType", ""),
                    ]
                    if part
                ).strip(),
                "city": attributes.get("City", ""),
                "state": attributes.get("RegionAbbr", "") or attributes.get("Region", ""),
                "postcode": attributes.get("Postal", ""),
                "country": attributes.get("CntryName", "") or attributes.get("Country", ""),
            },
            "raw_candidate": candidate,
        },
    )


def fetch_arcgis_forward_candidates(address):
    try:
        response = requests.get(
            ARCGIS_GEOCODE_URL,
            params={
                "SingleLine": format_address(address),
                "f": "pjson",
                "maxLocations": 5,
                "outFields": "*",
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        logger.warning("ArcGIS forward geocode failed: %s", str(exc))
        return []
    except ValueError as exc:
        logger.warning("ArcGIS forward geocode returned invalid JSON: %s", str(exc))
        return []

    candidates = []
    for candidate in (data.get("candidates") or []):
        attributes = candidate.get("attributes", {}) or {}
        latitude = attributes.get("Y") or candidate.get("location", {}).get("y")
        longitude = attributes.get("X") or candidate.get("location", {}).get("x")
        if latitude is None or longitude is None:
            continue

        candidates.append(
            build_location(
                candidate.get("address") or format_address(address),
                latitude,
                longitude,
                {
                    "provider": "arcgis",
                    "source": "arcgis-forward",
                    "match_type": attributes.get("Addr_type"),
                    "score": float(candidate.get("score") or attributes.get("Score") or 0),
                    "boundingbox": build_bounding_box_from_extent(candidate.get("extent")),
                    "address": {
                        "house_number": attributes.get("AddNum", ""),
                        "road": " ".join(
                            part
                            for part in [
                                attributes.get("StName", ""),
                                attributes.get("StType", ""),
                            ]
                            if part
                        ).strip(),
                        "city": attributes.get("City", ""),
                        "state": attributes.get("RegionAbbr", "") or attributes.get("Region", ""),
                        "postcode": attributes.get("Postal", ""),
                        "country": attributes.get("CntryName", "") or attributes.get("Country", ""),
                    },
                    "raw_candidate": candidate,
                },
            )
        )

    return candidates


def fetch_nominatim_candidates(address):
    structured_query = build_structured_query(address)
    formatted_address = format_address(address)
    country_code = get_country_code(address.get("country", ""))
    queries = [structured_query, formatted_address]
    candidates = []

    for query in queries:
        results = geolocator.geocode(
            query,
            timeout=10,
            exactly_one=False,
            limit=5,
            addressdetails=True,
            country_codes=country_code,
        )

        if not results:
            continue

        if not isinstance(results, list):
            results = [results]

        candidates.extend(results)

    return dedupe_geocode_candidates(candidates)


def reverse_geocode_arcgis_location(latitude, longitude):
    try:
        response = requests.get(
            ARCGIS_REVERSE_GEOCODE_URL,
            params={
                "location": f"{longitude},{latitude}",
                "f": "pjson",
            },
            timeout=10,
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        raise HTTPException(
            status_code=500,
            detail=f"ArcGIS reverse geocode failed: {str(exc)}",
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"ArcGIS reverse geocode returned invalid JSON: {str(exc)}",
        ) from exc

    if data.get("error"):
        message = (
            data["error"].get("message")
            or "ArcGIS reverse geocode returned an error"
        )
        raise HTTPException(status_code=500, detail=message)

    if not data.get("address"):
        raise HTTPException(
            status_code=404,
            detail="ArcGIS reverse geocode did not return an address",
        )

    return data

def get_nrel_pvwatts_data(
    lat: float,
    lon: float,
    tilt: Optional[float] = None,
    azimuth: Optional[float] = None,
    losses: Optional[float] = None,
):
    api_key = get_nrel_api_key()
    if not api_key:
        raise ValueError("NREL_API_KEY is not configured")

    params = {
        "format": "json",
        "api_key": api_key,
        "system_capacity": NREL_PVWATTS_REFERENCE_SYSTEM_KW,
        "module_type": NREL_PVWATTS_DEFAULT_MODULE_TYPE,
        "losses": losses if losses is not None else NREL_PVWATTS_DEFAULT_LOSSES,
        "array_type": NREL_PVWATTS_DEFAULT_ARRAY_TYPE,
        "tilt": tilt if tilt is not None else estimate_pvwatts_tilt(lat),
        "azimuth": azimuth if azimuth is not None else NREL_PVWATTS_DEFAULT_AZIMUTH,
        "lat": lat,
        "lon": lon,
        "dataset": NREL_PVWATTS_DEFAULT_DATASET,
        "radius": NREL_PVWATTS_DEFAULT_RADIUS,
        "inv_eff": NREL_PVWATTS_DEFAULT_INV_EFFICIENCY,
    }

    try:
        response = requests.get(NREL_PVWATTS_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as exc:
        logger.error("Error fetching NREL PVWatts data: %s", str(exc))
        raise HTTPException(status_code=502, detail=f"Error fetching NREL PVWatts data: {str(exc)}") from exc
    except ValueError as exc:
        logger.error("Error parsing NREL PVWatts response: %s", str(exc))
        raise HTTPException(status_code=500, detail=f"Error parsing NREL PVWatts response: {str(exc)}") from exc

    errors = data.get("errors") or []
    if errors:
        detail = "; ".join(str(error) for error in errors)
        raise HTTPException(status_code=502, detail=f"NREL PVWatts error: {detail}")

    outputs = data.get("outputs") or {}
    ac_monthly = outputs.get("ac_monthly") or []
    solrad_monthly = outputs.get("solrad_monthly") or []
    if len(ac_monthly) < 12 or len(solrad_monthly) < 12:
        raise HTTPException(
            status_code=500,
            detail="NREL PVWatts response did not include 12 months of production and solar data",
        )

    monthly_ac_per_kw = month_dict_from_sequence(ac_monthly, digits=1)
    monthly_all_sky = month_dict_from_sequence(solrad_monthly, digits=2)
    annual_production_per_kw = round(float(outputs.get("ac_annual") or sum(ac_monthly)), 2)
    annual_solrad = round(float(outputs.get("solrad_annual") or 0), 2)
    capacity_factor = round(float(outputs.get("capacity_factor") or 0) / 100, 4)
    station_info = data.get("station_info") or {}

    return {
        "provider": "nrel-pvwatts",
        "avg_all_sky_radiation": annual_solrad,
        "avg_clear_sky_radiation": annual_solrad,
        "monthly_all_sky": monthly_all_sky,
        "monthly_clear_sky": dict(monthly_all_sky),
        "all_sky_data_quality": 100.0,
        "clear_sky_data_quality": 100.0,
        "latitude": lat,
        "longitude": lon,
        "period": "daily average",
        "pvwatts": {
            "version": data.get("version"),
            "inputs": {
                "system_capacity": NREL_PVWATTS_REFERENCE_SYSTEM_KW,
                "module_type": NREL_PVWATTS_DEFAULT_MODULE_TYPE,
                "losses": params["losses"],
                "array_type": NREL_PVWATTS_DEFAULT_ARRAY_TYPE,
                "tilt": params["tilt"],
                "azimuth": params["azimuth"],
                "dataset": params["dataset"],
                "radius": params["radius"],
                "inv_eff": params["inv_eff"],
            },
            "station_info": {
                "lat": station_info.get("lat"),
                "lon": station_info.get("lon"),
                "city": station_info.get("city"),
                "state": station_info.get("state"),
                "distance": station_info.get("distance"),
                "weather_data_source": station_info.get("weather_data_source"),
            },
            "warnings": data.get("warnings") or [],
            "outputs": {
                "ac_monthly_per_kw": monthly_ac_per_kw,
                "ac_annual_per_kw": annual_production_per_kw,
                "capacity_factor": capacity_factor,
                "solrad_monthly": monthly_all_sky,
                "solrad_annual": annual_solrad,
            },
        },
    }


def get_nasa_power_data(lat: float, lon: float):
    """
    Fetch solar radiation data from NASA POWER API for a given latitude and longitude.
    
    Args:
    lat (float): Latitude of the location
    lon (float): Longitude of the location
    
    Returns:
    dict: A dictionary containing solar radiation data
    """
    base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    
    # Calculate date range for the past year
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    params = {
        "parameters": "ALLSKY_SFC_SW_DWN,CLRSKY_SFC_SW_DWN",
        "community": "RE",
        "longitude": lon,
        "latitude": lat,
        "start": start_date.strftime("%Y%m%d"),
        "end": end_date.strftime("%Y%m%d"),
        "format": "JSON"
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        all_sky_data = data['properties']['parameter']['ALLSKY_SFC_SW_DWN']
        clear_sky_data = data['properties']['parameter']['CLRSKY_SFC_SW_DWN']

        # Calculate average all sky radiation
        all_sky_values = [float(value) for value in all_sky_data.values() if value != -999]
        avg_all_sky_radiation = sum(all_sky_values) / len(all_sky_values) if all_sky_values else 0

        # Calculate average clear sky radiation
        clear_sky_values = [float(value) for value in clear_sky_data.values() if value != -999]
        avg_clear_sky_radiation = sum(clear_sky_values) / len(clear_sky_values) if clear_sky_values else 0

        # Prepare monthly data
        monthly_all_sky = {str(i).zfill(2): [] for i in range(1, 13)}
        monthly_clear_sky = {str(i).zfill(2): [] for i in range(1, 13)}

        for date, value in all_sky_data.items():
            month = date[4:6]
            if float(value) != -999:
                monthly_all_sky[month].append(float(value))

        for date, value in clear_sky_data.items():
            month = date[4:6]
            if float(value) != -999:
                monthly_clear_sky[month].append(float(value))

        monthly_all_sky = {k: round(sum(v) / len(v), 2) if v else 0 for k, v in monthly_all_sky.items()}
        monthly_clear_sky = {k: round(sum(v) / len(v), 2) if v else 0 for k, v in monthly_clear_sky.items()}

        # Calculate data quality (percentage of valid data points)
        all_sky_data_quality = len(all_sky_values) / len(all_sky_data)
        clear_sky_data_quality = len(clear_sky_values) / len(clear_sky_data)

        # Find best and worst months
        valid_all_sky = {k: v for k, v in monthly_all_sky.items() if v != 0}
        valid_clear_sky = {k: v for k, v in monthly_clear_sky.items() if v != 0}
        
        best_all_sky = max(valid_all_sky.items(), key=lambda x: x[1]) if valid_all_sky else (None, None)
        worst_all_sky = min(valid_all_sky.items(), key=lambda x: x[1]) if valid_all_sky else (None, None)
        best_clear_sky = max(valid_clear_sky.items(), key=lambda x: x[1]) if valid_clear_sky else (None, None)
        worst_clear_sky = min(valid_clear_sky.items(), key=lambda x: x[1]) if valid_clear_sky else (None, None)

        return {
            'avg_all_sky_radiation': round(avg_all_sky_radiation, 2),
            'avg_clear_sky_radiation': round(avg_clear_sky_radiation, 2),
            'monthly_all_sky': monthly_all_sky,
            'monthly_clear_sky': monthly_clear_sky,
            'all_sky_data_quality': round(all_sky_data_quality * 100, 2),
            'clear_sky_data_quality': round(clear_sky_data_quality * 100, 2),
            'best_all_sky': {'month': best_all_sky[0], 'value': round(best_all_sky[1], 2) if best_all_sky[1] is not None else None},
            'worst_all_sky': {'month': worst_all_sky[0], 'value': round(worst_all_sky[1], 2) if worst_all_sky[1] is not None else None},
            'best_clear_sky': {'month': best_clear_sky[0], 'value': round(best_clear_sky[1], 2) if best_clear_sky[1] is not None else None},
            'worst_clear_sky': {'month': worst_clear_sky[0], 'value': round(worst_clear_sky[1], 2) if worst_clear_sky[1] is not None else None},
            'latitude': lat,
            'longitude': lon,
            'period': "daily average",
            'start_date': start_date.strftime("%Y-%m-%d"),
            'end_date': end_date.strftime("%Y-%m-%d")
        }

    except requests.RequestException as e:
        logger.error(f"Error fetching NASA POWER data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching NASA POWER data: {str(e)}")
    except (KeyError, ValueError) as e:
        logger.error(f"Error processing NASA POWER data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing NASA POWER data: {str(e)}")


def get_fresh_solar_data(lat: float, lon: float):
    if get_nrel_api_key():
        try:
            return get_nrel_pvwatts_data(lat, lon), "nrel-pvwatts"
        except HTTPException as exc:
            logger.warning("NREL PVWatts unavailable, falling back to NASA POWER: %s", exc.detail)
        except Exception as exc:
            logger.warning("NREL PVWatts unavailable, falling back to NASA POWER: %s", str(exc))

    solar_data = get_nasa_power_data(lat, lon)
    solar_data["provider"] = "nasa"
    return solar_data, "nasa"

def geocode_location(address):
    try:
        provider = get_geocoder_provider()
        country_code = get_country_code(address.get("country", ""))
        unique_candidates = []

        if provider in {"nominatim", "hybrid"}:
            unique_candidates.extend(fetch_nominatim_candidates(address))

        if provider == "arcgis":
            unique_candidates = dedupe_geocode_candidates(
                [*unique_candidates, *fetch_arcgis_forward_candidates(address)]
            )

        if country_code == "us" and extract_house_number(address.get("street", "")):
            arcgis_candidate = fetch_arcgis_point_address(address)
            if arcgis_candidate and provider in {"arcgis", "hybrid"}:
                unique_candidates = dedupe_geocode_candidates(
                    [*unique_candidates, arcgis_candidate]
                )

        if not unique_candidates:
            raise HTTPException(status_code=404, detail="Address not found")

        evaluated_candidates = []
        for location in unique_candidates:
            forward_score = score_geocode_candidate(address, location)
            precision_score = score_location_precision(location)
            evaluated_candidates.append(
                {
                    "location": location,
                    "match_score": forward_score + precision_score,
                    "forward_score": forward_score,
                    "precision_score": precision_score,
                    "reverse_score": 0,
                }
            )

        evaluated_candidates.sort(key=lambda item: item["match_score"], reverse=True)

        for candidate in evaluated_candidates[:3]:
            reverse_score = score_reverse_geocode_candidate(address, candidate["location"])
            candidate["reverse_score"] = reverse_score
            candidate["match_score"] += reverse_score

        best_candidate = max(evaluated_candidates, key=lambda item: item["match_score"])
        best_location = best_candidate["location"]
        match_score = best_candidate["match_score"]
        source = get_location_source(best_location)

        return {
            "location": best_location,
            "match_score": match_score,
            "match_quality": assess_match_quality(match_score),
            "source": source,
        }
    except HTTPException:
        raise
    except GeocoderTimedOut:
        raise HTTPException(status_code=408, detail="Geocoding service timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


def geocode_address(address):
    geocode_result = geocode_location(address)
    location = geocode_result["location"]
    return location.latitude, location.longitude


def reverse_geocode_location(latitude, longitude):
    try:
        if get_geocoder_provider() == "arcgis":
            payload = reverse_geocode_arcgis_location(latitude, longitude)
            reverse_address = payload.get("address", {}) or {}
            house_number = reverse_address.get("AddNum", "")
            road = (reverse_address.get("Address", "") or "").strip()
            if house_number and road.lower().startswith(f"{house_number.lower()} "):
                road = road[len(house_number):].strip()
            formatted_address = (
                reverse_address.get("LongLabel")
                or reverse_address.get("Match_addr")
                or reverse_address.get("Address")
                or f"{round(latitude, 6)}, {round(longitude, 6)}"
            )
            return build_location(
                formatted_address,
                latitude,
                longitude,
                {
                    "provider": "arcgis",
                    "source": "arcgis-reverse",
                    "boundingbox": None,
                    "address": {
                        "house_number": house_number,
                        "road": road,
                        "city": reverse_address.get("City", ""),
                        "state": reverse_address.get("RegionAbbr", "") or reverse_address.get("Region", ""),
                        "postcode": reverse_address.get("Postal", ""),
                        "country": reverse_address.get("CntryName", "") or reverse_address.get("CountryCode", ""),
                    },
                },
            )

        location = geolocator.reverse(
            f"{latitude}, {longitude}",
            timeout=10,
            exactly_one=True,
            zoom=18,
            addressdetails=True,
        )

        if not location:
            raise HTTPException(status_code=404, detail="Unable to resolve an address from browser location")

        return location
    except HTTPException:
        raise
    except GeocoderTimedOut:
        raise HTTPException(status_code=408, detail="Reverse geocoding service timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# API Endpoints
@app.post("/api/user-data", response_model=dict, summary="Store User Data", description="Stores user data including address and browser information.")
async def store_user_data(request: Request, user_data: UserData):
    try:
        ip_address = request.client.host
        guid = str(uuid.uuid4())
        store_personal_info(guid, user_data.address.model_dump())
        store_browser_data(guid, user_data.browserData.model_dump(), ip_address)
        return {"guid": guid}
    except Exception as e:
        logging.error(f"Error storing user data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_timezone(lat, lon):
    tf = TimezoneFinder()
    return tf.timezone_at(lat=lat, lng=lon)


def clamp(value, minimum, maximum):
    return max(minimum, min(maximum, value))


def normalize_azimuth(value):
    return round(float(value) % 360, 1)


def angular_distance_degrees(value_a, value_b):
    distance = abs(float(value_a) - float(value_b)) % 360
    return min(distance, 360 - distance)


def aspect_to_azimuth(aspect):
    return {
        "north-facing": 0.0,
        "east-facing": 90.0,
        "south-facing": 180.0,
        "west-facing": 270.0,
    }.get(aspect)


def azimuth_to_direction_group(azimuth):
    azimuth = normalize_azimuth(azimuth)
    if 45 <= azimuth < 135:
        return "east"
    if 135 <= azimuth < 225:
        return "south"
    if 225 <= azimuth < 315:
        return "west"
    return "north"


def get_polygon_ring_points(geometry):
    if not geometry or geometry.get("type") != "Polygon":
        return []

    ring = (geometry.get("coordinates") or [None])[0] or []
    points = []
    for coordinate in ring:
        if not isinstance(coordinate, (list, tuple)) or len(coordinate) < 2:
            continue
        try:
            lng = float(coordinate[0])
            lat = float(coordinate[1])
        except (TypeError, ValueError):
            continue
        points.append({"lat": lat, "lng": lng})

    if len(points) >= 2 and points[0] == points[-1]:
        points = points[:-1]

    return points


def project_local_point(point, origin_latitude):
    origin_latitude_radians = math.radians(origin_latitude)
    return {
        "x": EARTH_RADIUS_METERS * math.radians(point["lng"]) * math.cos(origin_latitude_radians),
        "y": EARTH_RADIUS_METERS * math.radians(point["lat"]),
    }


def segment_bearing_degrees(point_a, point_b):
    latitude_a_radians = math.radians(point_a["lat"])
    latitude_b_radians = math.radians(point_b["lat"])
    longitude_delta = math.radians(point_b["lng"] - point_a["lng"])

    x_value = math.sin(longitude_delta) * math.cos(latitude_b_radians)
    y_value = (
        math.cos(latitude_a_radians) * math.sin(latitude_b_radians)
        - math.sin(latitude_a_radians) * math.cos(latitude_b_radians) * math.cos(longitude_delta)
    )
    return normalize_azimuth(math.degrees(math.atan2(x_value, y_value)))


def resolve_dominant_roof_edge(roof_selection):
    points = get_polygon_ring_points((roof_selection or {}).get("geometry"))
    if len(points) < 2:
        return None

    origin_latitude = sum(point["lat"] for point in points) / len(points)
    projected_points = [project_local_point(point, origin_latitude) for point in points]
    longest_edge = None

    for index, point in enumerate(points):
        next_point = points[(index + 1) % len(points)]
        projected_point = projected_points[index]
        projected_next = projected_points[(index + 1) % len(projected_points)]
        edge_length_m = math.hypot(
            projected_next["x"] - projected_point["x"],
            projected_next["y"] - projected_point["y"],
        )
        if edge_length_m < 1:
            continue

        edge = {
            "bearing": segment_bearing_degrees(point, next_point),
            "length_m": round(edge_length_m, 1),
        }
        if not longest_edge or edge["length_m"] > longest_edge["length_m"]:
            longest_edge = edge

    return longest_edge


def build_solar_modeling_context(latitude, roof_selection, property_context):
    dominant_edge = resolve_dominant_roof_edge(roof_selection)
    terrain_context = (property_context or {}).get("terrain_context") or {}
    building_context = (property_context or {}).get("building_context") or {}
    shade_context = (property_context or {}).get("shade_context") or {}
    terrain_aspect = terrain_context.get("dominant_aspect")
    preferred_azimuth = aspect_to_azimuth(terrain_aspect) or 180.0

    if dominant_edge:
        candidate_a = normalize_azimuth(dominant_edge["bearing"] + 90)
        candidate_b = normalize_azimuth(dominant_edge["bearing"] - 90)
        assumed_azimuth = (
            candidate_a
            if angular_distance_degrees(candidate_a, preferred_azimuth)
            <= angular_distance_degrees(candidate_b, preferred_azimuth)
            else candidate_b
        )
        azimuth_source = (
            "roof polygon dominant edge with terrain-aware side selection"
            if property_context
            else "roof polygon dominant edge with solar-facing side selection"
        )
    else:
        assumed_azimuth = preferred_azimuth
        azimuth_source = (
            "terrain-aware fallback"
            if property_context and aspect_to_azimuth(terrain_aspect) is not None
            else "south-facing fallback"
        )

    base_tilt = estimate_pvwatts_tilt(latitude)
    terrain_slope_percent = float(terrain_context.get("slope_percent") or 0)
    terrain_azimuth = aspect_to_azimuth(terrain_aspect)
    tilt_adjustment = 0.0
    if terrain_azimuth is not None and terrain_slope_percent > 0:
        alignment_degrees = angular_distance_degrees(assumed_azimuth, terrain_azimuth)
        if alignment_degrees <= 45:
            tilt_adjustment = min(terrain_slope_percent * 0.35, 5.5)
        elif alignment_degrees >= 135:
            tilt_adjustment = -min(terrain_slope_percent * 0.2, 3.5)

    assumed_tilt = round(clamp(base_tilt + tilt_adjustment, 8, 45), 1)
    tilt_source = (
        "latitude baseline nudged by local terrain aspect"
        if property_context and terrain_azimuth is not None and terrain_slope_percent > 0
        else "latitude fallback"
    )

    directional_pressure = building_context.get("directional_pressure") or {}
    facing_group = azimuth_to_direction_group(assumed_azimuth)
    pressure_weights = {
        "south": {"south": 0.75, "east": 0.18, "west": 0.18, "north": 0.05},
        "east": {"east": 0.75, "south": 0.18, "north": 0.18, "west": 0.05},
        "west": {"west": 0.75, "south": 0.18, "north": 0.18, "east": 0.05},
        "north": {"north": 0.75, "east": 0.18, "west": 0.18, "south": 0.05},
    }[facing_group]
    weighted_pressure = sum(
        float(directional_pressure.get(direction) or 0) * weight
        for direction, weight in pressure_weights.items()
    )
    building_loss_fraction = clamp(weighted_pressure * 0.035, 0.0, 0.12)
    terrain_bias = shade_context.get("terrain_bias") or "mostly neutral"
    terrain_loss_adjustment = 0.0
    if property_context:
        if terrain_bias == "less solar-favored":
            terrain_loss_adjustment = 0.02
        elif terrain_bias == "mostly neutral":
            terrain_loss_adjustment = 0.01

    site_loss_fraction = clamp(building_loss_fraction + terrain_loss_adjustment, 0.0, 0.16)

    return {
        "dominant_edge_bearing": dominant_edge.get("bearing") if dominant_edge else None,
        "dominant_edge_length_m": dominant_edge.get("length_m") if dominant_edge else None,
        "assumed_azimuth": assumed_azimuth,
        "assumed_tilt": assumed_tilt,
        "azimuth_source": azimuth_source,
        "tilt_source": tilt_source,
        "site_context_available": bool(property_context),
        "site_context_summary": (property_context or {}).get("summary"),
        "site_context_label": ((property_context or {}).get("match_envelope") or {}).get("label"),
        "site_context_source": ((property_context or {}).get("match_envelope") or {}).get("source"),
        "obstruction_risk": shade_context.get("obstruction_risk") or building_context.get("obstruction_risk"),
        "terrain_class": terrain_context.get("terrain_class"),
        "terrain_aspect": terrain_aspect,
        "terrain_bias": terrain_bias,
        "building_pressure_score": round(weighted_pressure, 2),
        "modeled_site_losses_percent": round(site_loss_fraction * 100, 1),
        "site_loss_factor": round(1 - site_loss_fraction, 3),
        "pvwatts_losses_percent": round(NREL_PVWATTS_DEFAULT_LOSSES + (site_loss_fraction * 100), 1),
    }


def normalize_panel_efficiency(panel_efficiency):
    return clamp(float(panel_efficiency or 0.20), 0.15, 0.27)


def calculate_roof_backed_system_size(roof_selection, panel_efficiency):
    if not roof_selection:
        return None

    area_square_meters = float(roof_selection.get("areaSquareMeters") or 0)
    if area_square_meters > 0:
        normalized_efficiency = normalize_panel_efficiency(panel_efficiency)
        kw = area_square_meters * normalized_efficiency * ROOF_COVERAGE_FACTOR
        return round(clamp(kw, 1.5, 18.0), 2)

    recommended_kw = roof_selection.get("recommendedKw")
    if recommended_kw is None:
        return None

    return round(float(recommended_kw), 2)


def resolve_temperature_factor(avg_all_sky_radiation):
    if avg_all_sky_radiation >= 5.75:
        return 0.90
    if avg_all_sky_radiation >= 4.75:
        return 0.92
    if avg_all_sky_radiation >= 3.75:
        return 0.94
    return 0.96


def build_solar_production_model(
    system_size_kw,
    monthly_all_sky,
    avg_all_sky_radiation,
    panel_efficiency,
    electricity_rate,
    roof_selection,
    modeling_context,
):
    normalized_efficiency = normalize_panel_efficiency(panel_efficiency)
    loss_factors = {
        "inverter": 0.96,
        "electrical": 0.98,
        "soiling": 0.97,
        "availability": 0.99,
        "temperature": resolve_temperature_factor(avg_all_sky_radiation),
        "layout": 0.96 if roof_selection else 0.90,
        "site_context": modeling_context.get("site_loss_factor", 1.0),
    }

    performance_ratio = 1.0
    for factor in loss_factors.values():
        performance_ratio *= factor
    performance_ratio = round(performance_ratio, 3)

    monthly_production = {}
    monthly_savings = {}
    for month, radiation in monthly_all_sky.items():
        monthly_output = round(
            float(radiation or 0)
            * system_size_kw
            * performance_ratio
            * MONTH_DAY_COUNTS.get(month, 30),
            1,
        )
        monthly_production[month] = monthly_output
        monthly_savings[month] = round(monthly_output * electricity_rate, 2)

    annual_production = round(sum(monthly_production.values()), 2)
    daily_production = round(annual_production / 365, 2)
    specific_yield = round(annual_production / system_size_kw, 2) if system_size_kw else 0
    capacity_factor = round(
        annual_production / (system_size_kw * 24 * 365),
        4,
    ) if system_size_kw else 0
    peak_month = max(monthly_production.items(), key=lambda item: item[1]) if monthly_production else (None, 0)
    lowest_month = min(monthly_production.items(), key=lambda item: item[1]) if monthly_production else (None, 0)

    return {
        "id": "roof-backed-monthly-v2",
        "label": "Roof-backed monthly model",
        "description": (
            "Uses month-by-month solar resource data, roof-backed DC sizing, inferred roof-facing "
            "assumptions, and first-pass site-context losses instead of a single annualized "
            "screening multiplier."
        ),
        "roof_coverage_factor": ROOF_COVERAGE_FACTOR,
        "effective_panel_efficiency": normalized_efficiency,
        "performance_ratio": performance_ratio,
        "loss_factors": loss_factors,
        "assumed_tilt": modeling_context.get("assumed_tilt"),
        "assumed_azimuth": modeling_context.get("assumed_azimuth"),
        "tilt_source": modeling_context.get("tilt_source"),
        "azimuth_source": modeling_context.get("azimuth_source"),
        "site_context_available": modeling_context.get("site_context_available", False),
        "site_context_summary": modeling_context.get("site_context_summary"),
        "site_context_label": modeling_context.get("site_context_label"),
        "site_context_source": modeling_context.get("site_context_source"),
        "obstruction_risk": modeling_context.get("obstruction_risk"),
        "terrain_class": modeling_context.get("terrain_class"),
        "terrain_aspect": modeling_context.get("terrain_aspect"),
        "terrain_bias": modeling_context.get("terrain_bias"),
        "building_pressure_score": modeling_context.get("building_pressure_score"),
        "modeled_site_losses_percent": modeling_context.get("modeled_site_losses_percent"),
        "dominant_edge_bearing": modeling_context.get("dominant_edge_bearing"),
        "dominant_edge_length_m": modeling_context.get("dominant_edge_length_m"),
        "monthly_production": monthly_production,
        "monthly_savings": monthly_savings,
        "annual_production": annual_production,
        "daily_production": daily_production,
        "specific_yield": specific_yield,
        "capacity_factor": capacity_factor,
        "peak_month": {
            "month": peak_month[0],
            "value": round(peak_month[1], 1),
        },
        "lowest_month": {
            "month": lowest_month[0],
            "value": round(lowest_month[1], 1),
        },
    }


def build_nrel_pvwatts_production_model(
    system_size_kw,
    solar_data,
    panel_efficiency,
    electricity_rate,
    modeling_context,
):
    pvwatts = solar_data.get("pvwatts") or {}
    outputs = pvwatts.get("outputs") or {}
    inputs = pvwatts.get("inputs") or {}
    monthly_ac_per_kw = outputs.get("ac_monthly_per_kw") or {}
    monthly_production = {
        month: round(float(ac_per_kw or 0) * system_size_kw, 1)
        for month, ac_per_kw in monthly_ac_per_kw.items()
    }
    monthly_savings = {
        month: round(production * electricity_rate, 2)
        for month, production in monthly_production.items()
    }
    annual_production = round(sum(monthly_production.values()), 2)
    daily_production = round(annual_production / 365, 2)
    specific_yield = round(annual_production / system_size_kw, 2) if system_size_kw else 0
    capacity_factor = round(float(outputs.get("capacity_factor") or 0), 4)
    peak_month = max(monthly_production.items(), key=lambda item: item[1]) if monthly_production else (None, 0)
    lowest_month = min(monthly_production.items(), key=lambda item: item[1]) if monthly_production else (None, 0)
    losses = float(inputs.get("losses") or NREL_PVWATTS_DEFAULT_LOSSES)
    inverter_efficiency = float(inputs.get("inv_eff") or NREL_PVWATTS_DEFAULT_INV_EFFICIENCY)
    performance_ratio = round((1 - (losses / 100)) * (inverter_efficiency / 100), 3)

    return {
        "id": "nrel-pvwatts-v8",
        "label": "NREL PVWatts V8",
        "description": (
            "Uses NREL PVWatts V8 with NSRDB weather data, the drawn roof geometry for azimuth "
            "selection, a terrain-informed tilt fallback, and first-pass site-context losses."
        ),
        "roof_coverage_factor": ROOF_COVERAGE_FACTOR,
        "effective_panel_efficiency": normalize_panel_efficiency(panel_efficiency),
        "performance_ratio": performance_ratio,
        "loss_factors": {
            "aggregate_system_losses": round(1 - (losses / 100), 3),
            "inverter": round(inverter_efficiency / 100, 3),
        },
        "assumed_tilt": round(float(inputs.get("tilt") or 0), 1),
        "assumed_azimuth": round(float(inputs.get("azimuth") or 0), 1),
        "tilt_source": modeling_context.get("tilt_source"),
        "azimuth_source": modeling_context.get("azimuth_source"),
        "site_context_available": modeling_context.get("site_context_available", False),
        "site_context_summary": modeling_context.get("site_context_summary"),
        "site_context_label": modeling_context.get("site_context_label"),
        "site_context_source": modeling_context.get("site_context_source"),
        "obstruction_risk": modeling_context.get("obstruction_risk"),
        "terrain_class": modeling_context.get("terrain_class"),
        "terrain_aspect": modeling_context.get("terrain_aspect"),
        "terrain_bias": modeling_context.get("terrain_bias"),
        "building_pressure_score": modeling_context.get("building_pressure_score"),
        "modeled_site_losses_percent": round(float(losses or 0) - NREL_PVWATTS_DEFAULT_LOSSES, 1),
        "dominant_edge_bearing": modeling_context.get("dominant_edge_bearing"),
        "dominant_edge_length_m": modeling_context.get("dominant_edge_length_m"),
        "weather_data_source": (pvwatts.get("station_info") or {}).get("weather_data_source"),
        "warnings": pvwatts.get("warnings") or [],
        "monthly_production": monthly_production,
        "monthly_savings": monthly_savings,
        "annual_production": annual_production,
        "daily_production": daily_production,
        "specific_yield": specific_yield,
        "capacity_factor": capacity_factor,
        "peak_month": {
            "month": peak_month[0],
            "value": round(peak_month[1], 1),
        },
        "lowest_month": {
            "month": lowest_month[0],
            "value": round(lowest_month[1], 1),
        },
    }


def build_solar_assumptions(
    system_size_kw,
    sizing_source,
    roof_selection,
    panel_efficiency,
    electricity_rate,
    installation_cost_per_watt,
    data_source,
    solar_provider,
    data_quality,
    production_model,
):
    normalized_efficiency = normalize_panel_efficiency(panel_efficiency)

    if sizing_source == "roof-geometry" and roof_selection:
        system_size_assumption = (
            f"System size uses the saved roof selection: {system_size_kw:.1f} kW from "
            f"{round(float(roof_selection.get('areaSquareFeet', 0))):,} sq ft, "
            f"{normalized_efficiency * 100:.0f}% panel efficiency, and a "
            f"{ROOF_COVERAGE_FACTOR * 100:.0f}% roof coverage allowance."
        )
    else:
        system_size_assumption = f"System size uses a manual input of {system_size_kw:.1f} kW."

    if solar_provider == "nrel-pvwatts":
        production_assumption = (
            "Production uses NREL PVWatts V8 with NSRDB weather data, a roof-mounted array type, "
            f"{production_model.get('assumed_tilt', 0):.0f}° tilt from {production_model.get('tilt_source') or 'fallback inputs'}, "
            f"{production_model.get('assumed_azimuth', 180):.0f}° azimuth from {production_model.get('azimuth_source') or 'fallback inputs'}, and "
            f"{NREL_PVWATTS_DEFAULT_LOSSES + (production_model.get('modeled_site_losses_percent') or 0):.0f}% aggregate system losses."
        )
        data_source_assumption = (
            "Solar resource and production modeling use NREL PVWatts V8; cache layers only change "
            "how the profile was retrieved, not the underlying model."
        )
    else:
        production_assumption = (
            "Monthly production uses month-by-month solar irradiance with an estimated "
            f"{production_model.get('performance_ratio', 0) * 100:.0f}% performance ratio."
        )
        data_source_assumption = (
            f"Solar resource data source is {data_source or 'unknown'} with {data_quality} quality."
        )

    assumptions = [
        system_size_assumption,
        production_assumption,
        f"Panel efficiency is assumed at {normalized_efficiency * 100:.0f}%.",
        f"Electricity rate is assumed at ${electricity_rate:.2f}/kWh.",
        f"Installed cost is assumed at ${installation_cost_per_watt:.2f}/W.",
        data_source_assumption,
    ]

    if production_model.get("site_context_available"):
        assumptions.append(
            f"Nearby building, vegetation, and terrain context contribute about {production_model.get('modeled_site_losses_percent', 0):.1f}% extra modeled site losses in this planning pass."
        )
    else:
        assumptions.append(
            "Site-context losses still use a generic fallback because no saved property context is available yet."
        )

    assumptions.extend([
        "Trees, fences, utility tariff detail, and parcel-certified roof pitch are not modeled yet.",
        "This is a stronger planning model, but it is still not an installer quote or permit-ready design.",
    ])
    return assumptions


def build_solar_confidence(
    match_quality,
    data_quality,
    sizing_source,
    data_source,
    solar_provider,
    roof_selection,
    production_model,
):
    score = 38
    factors = []

    if sizing_source == "roof-geometry" and roof_selection:
        score += 22
        factors.append("System size is derived from the saved roof geometry and panel efficiency.")
    else:
        score += 6
        factors.append("System size is using a manual fallback instead of saved roof geometry.")

    if solar_provider == "nrel-pvwatts":
        score += 12
        factors.append("Production uses NREL PVWatts V8 with NSRDB weather data.")
        if production_model.get("site_context_available"):
            score += 6
            factors.append("Roof-facing inputs and site losses are refined from the drawn roof and saved property context.")
        else:
            factors.append("Roof-facing inputs still use generic fallbacks because saved site context is missing.")
    elif production_model:
        score += 8
        factors.append(
            "Production uses month-by-month solar resource data and explicit system-loss assumptions."
        )
        if production_model.get("site_context_available"):
            score += 5
            factors.append("Nearby building, vegetation, and terrain context temper the production model for this property.")

    if match_quality == "high":
        score += 18
        factors.append("Address match quality is strong for the current property.")
    elif match_quality == "medium":
        score += 10
        factors.append("Address match quality is approximate, so parcel precision is lower.")
    else:
        factors.append("Address match quality is loose, so location precision is limited.")

    if data_quality == "high":
        score += 18
        factors.append("Solar resource data quality is high.")
    elif data_quality == "medium":
        score += 10
        factors.append("Solar resource data quality is moderate.")
    else:
        factors.append("Solar resource data quality is limited.")

    if data_source == "zip-cache":
        score -= 6
        factors.append("Solar data came from ZIP-level cache rather than a property-specific fetch.")
    elif data_source == "guid-cache":
        score += 6
        factors.append("Solar data came from this property record's recent cache.")
    elif data_source == "nrel-pvwatts":
        score += 10
        factors.append("Solar data came from a location-specific NREL PVWatts fetch.")
    elif data_source == "nasa":
        score += 8
        factors.append("Solar data came from a location-specific NASA POWER fetch.")

    roof_area_square_feet = float(roof_selection.get("areaSquareFeet", 0)) if roof_selection else 0
    if roof_area_square_feet and roof_area_square_feet < 350:
        score -= 4
        factors.append("The selected roof area is small, so the estimate is more sensitive to drawing changes.")

    score = max(20, min(95, score))

    if score >= 80:
        confidence_id = "high"
        description = (
            "Useful planning estimate with roof-backed sizing, monthly production modeling, and solid source inputs."
        )
    elif score >= 60:
        confidence_id = "medium"
        description = (
            "Useful planning estimate, but one or more source or property inputs are still approximate."
        )
    else:
        confidence_id = "low"
        description = "Treat this as a rough screening estimate until the inputs are improved."

    return {
        "id": confidence_id,
        "label": confidence_id.capitalize(),
        "score": score,
        "description": description,
        "factors": factors,
    }


def build_solar_report_name(address):
    timestamp_label = datetime.now().astimezone().strftime("%b %d, %Y %I:%M %p")
    street = address.get("street") or "Property"
    return f"{street} solar report · {timestamp_label}"


def build_saved_solar_report(address, estimate, report_name=None):
    return {
        "id": str(uuid.uuid4()),
        "name": report_name or build_solar_report_name(address),
        "created_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        "address": estimate["address"],
        "system_size_kw": estimate["system_size_kw"],
        "annual_production": estimate["annual_production"],
        "annual_savings": estimate["annual_savings"],
        "system_cost": estimate["system_cost"],
        "payback_period": estimate["payback_period"],
        "confidence": estimate["confidence"],
        "data_source": estimate["data_source"],
        "data_provider": estimate.get("data_provider"),
        "data_quality": estimate["data_quality"],
        "roof_area_square_feet": estimate["roof_area_square_feet"],
        "roof_area_square_meters": estimate["roof_area_square_meters"],
        "production_model": estimate["production_model"],
        "monthly_production": estimate["monthly_production"],
        "monthly_savings": estimate["monthly_savings"],
        "specific_yield": estimate["specific_yield"],
        "capacity_factor": estimate["capacity_factor"],
        "assumptions": estimate["assumptions"],
        "summary": (
            f"{round(estimate['annual_production']):,} kWh/year, "
            f"${round(estimate['annual_savings']):,}/year savings, "
            f"{estimate['confidence']['label']} confidence."
        ),
    }


def build_homeowner_quote(address, report, existing_quote=None):
    existing_quote = existing_quote or {}
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    street = address.get("street") or "Property"
    quote_id = existing_quote.get("id") or str(uuid.uuid4())
    system_size_kw = round(float(report.get("system_size_kw") or 0), 1)
    annual_savings = round(float(report.get("annual_savings") or 0))
    annual_production = round(float(report.get("annual_production") or 0))
    confidence_label = (report.get("confidence") or {}).get("label") or "Planning"

    return {
        "id": quote_id,
        "headline": existing_quote.get("headline") or f"{street} homeowner quote",
        "created_at": existing_quote.get("created_at") or timestamp,
        "updated_at": timestamp,
        "status": "share-ready",
        "share_path": f"/quote/{quote_id}",
        "summary": (
            f"{system_size_kw:.1f} kW, "
            f"{annual_production:,} kWh/year, "
            f"${annual_savings:,}/year savings."
        ),
        "confidence_label": confidence_label,
        "disclaimer": (
            "This is a shareable planning quote based on the saved roof geometry and current model assumptions. "
            "Final pricing, layout, and installer scope still require site review."
        ),
    }


def _clean_contact_value(value):
    return " ".join(str(value or "").strip().split())


def _normalized_phone_digits(value):
    return "".join(character for character in str(value or "") if character.isdigit())


def resolve_installer_handoff_route(address):
    state = str((address or {}).get("state") or "").strip().upper()
    region_key = normalize_lookup_text(state) or "default"
    route_label = (
        f"{state} installer review queue"
        if state
        else "Default installer review queue"
    )
    partner_name = os.getenv("SOLAR_INSTALLER_HANDOFF_NAME") or "Solar Buddy installer review"
    partner_email = os.getenv("SOLAR_INSTALLER_HANDOFF_EMAIL") or ""

    return {
        "route_id": f"{region_key}-installer-review",
        "route_label": route_label,
        "region": state or "default",
        "partner_name": partner_name,
        "partner_email": partner_email or None,
        "delivery_channel": "manual-email" if partner_email else "manual-review",
    }


def qualify_solar_lead(monthly_bill_range, install_timeline):
    strong_bill_ranges = {"200-plus", "100-200"}
    strong_timelines = {"asap", "1-3-months"}

    if monthly_bill_range in strong_bill_ranges and install_timeline in strong_timelines:
        return {
            "status": "qualified",
            "label": "Qualified",
            "summary": "The lead includes enough timeline and bill context for installer follow-up.",
        }

    if install_timeline == "researching":
        return {
            "status": "early",
            "label": "Researching",
            "summary": "The lead is still early-stage and may need nurture before installer outreach.",
        }

    return {
        "status": "review",
        "label": "Needs review",
        "summary": "The lead has contact details but still needs manual qualification review.",
    }


def build_quote_lead_capture(address, latest_lead=None, lead_count=0):
    route = resolve_installer_handoff_route(address)
    latest_handoff = (latest_lead or {}).get("handoff") or {}
    latest_qualification = (latest_lead or {}).get("qualification") or {}

    return {
        "enabled": True,
        "route": route,
        "lead_count": int(lead_count or 0),
        "latest_submitted_at": (latest_lead or {}).get("created_at"),
        "latest_status": latest_handoff.get("status") or "ready",
        "latest_qualification": latest_qualification.get("label"),
        "summary": (
            latest_handoff.get("summary")
            or "Lead capture is ready on this homeowner quote for installer follow-up."
        ),
    }


def hydrate_homeowner_quote(quote, address, latest_lead=None, lead_count=0):
    if not quote:
        return None

    return {
        **quote,
        "lead_capture": build_quote_lead_capture(
            address,
            latest_lead=latest_lead,
            lead_count=lead_count,
        ),
    }


def build_solar_quote_lead(quote_id, record, report, payload: SolarQuoteLeadRequest):
    timestamp = datetime.now().astimezone().isoformat(timespec="seconds")
    address = record.get("address") or {}
    route = resolve_installer_handoff_route(address)
    qualification = qualify_solar_lead(payload.monthly_bill_range, payload.install_timeline)

    return {
        "id": str(uuid.uuid4()),
        "quote_id": quote_id,
        "property_guid": record.get("guid"),
        "report_id": report.get("id"),
        "created_at": timestamp,
        "contact": {
            "full_name": _clean_contact_value(payload.full_name),
            "email": _clean_contact_value(payload.email).lower(),
            "phone": _clean_contact_value(payload.phone),
            "preferred_contact": _clean_contact_value(payload.preferred_contact).lower() or "phone",
        },
        "address": {
            "city": address.get("city"),
            "state": address.get("state"),
            "zip": address.get("zip"),
        },
        "qualification": {
            **qualification,
            "monthly_bill_range": payload.monthly_bill_range or "unknown",
            "install_timeline": payload.install_timeline or "unknown",
        },
        "notes": _clean_contact_value(payload.notes),
        "consent_to_contact": bool(payload.consent_to_contact),
        "handoff": {
            **route,
            "status": "queued",
            "queued_at": timestamp,
            "summary": (
                f"Queued for {route['route_label'].lower()} via {route['delivery_channel']}."
            ),
        },
    }


def build_solar_estimate_response(input_data):
    guid = input_data.guid
    address = check_existing_address_data(guid)
    if not address:
        raise HTTPException(status_code=404, detail="GUID not found")

    logger.info(f"Address data for GUID {guid}: {address}")

    property_record = get_property_record(guid) or {"address": address}
    request_roof_selection = (
        input_data.roof_selection.model_dump() if input_data.roof_selection else None
    )

    if request_roof_selection is not None:
        upsert_property_record(
            guid,
            address,
            property_preview=property_record.get("property_preview"),
            roof_selection=request_roof_selection,
        )
        property_record = get_property_record(guid) or {
            "address": address,
            "roof_selection": request_roof_selection,
        }

    roof_selection = request_roof_selection or property_record.get("roof_selection")
    if roof_selection:
        system_size_kw = calculate_roof_backed_system_size(
            roof_selection,
            input_data.panel_efficiency,
        )
        sizing_source = "roof-geometry"
    elif input_data.system_size is not None:
        system_size_kw = round(float(input_data.system_size), 2)
        sizing_source = "manual"
    else:
        raise HTTPException(
            status_code=400,
            detail="Draw a roof area first or provide a system size.",
        )

    if not system_size_kw:
        raise HTTPException(status_code=400, detail="Unable to determine a usable system size.")

    solar_data, time_zone = check_existing_solar_data(guid)
    data_source = "guid-cache" if solar_data else None
    if not solar_data:
        try:
            solar_data, time_zone = check_existing_zip_data(address["zip"])
            data_source = "zip-cache" if solar_data else None
        except Exception as e:
            logger.error(f"Error checking existing ZIP data: {str(e)}")
            solar_data, time_zone = None, None
            data_source = None

    if not solar_data:
        lat, lon = geocode_address(address)
        if get_nrel_api_key():
            solar_data = get_nasa_power_data(lat, lon)
            solar_data["provider"] = "nasa"
            fresh_data_source = "nasa"
        else:
            solar_data, fresh_data_source = get_fresh_solar_data(lat, lon)
        time_zone = get_timezone(lat, lon)
        store_solar_data(guid, solar_data, time_zone, address, fresh_data_source)
        data_source = fresh_data_source
    else:
        lat, lon = solar_data.get("latitude"), solar_data.get("longitude")

    if lat is None or lon is None:
        raise HTTPException(status_code=500, detail="Unable to determine latitude and longitude")

    property_context = property_record.get("property_context")
    modeling_context = build_solar_modeling_context(
        lat,
        roof_selection,
        property_context,
    )

    estimate_solar_data = solar_data
    estimate_data_source = data_source
    if get_nrel_api_key():
        try:
            estimate_solar_data = get_nrel_pvwatts_data(
                lat,
                lon,
                tilt=modeling_context.get("assumed_tilt"),
                azimuth=modeling_context.get("assumed_azimuth"),
                losses=modeling_context.get("pvwatts_losses_percent"),
            )
            estimate_data_source = "nrel-pvwatts"
        except HTTPException as exc:
            logger.warning(
                "Refined NREL PVWatts estimate unavailable, falling back to cached solar data: %s",
                exc.detail,
            )
        except Exception as exc:
            logger.warning(
                "Refined NREL PVWatts estimate unavailable, falling back to cached solar data: %s",
                str(exc),
            )

    solar_provider = resolve_solar_provider(estimate_solar_data)
    avg_all_sky_radiation = round(estimate_solar_data.get("avg_all_sky_radiation", 0), 2)
    avg_clear_sky_radiation = round(estimate_solar_data.get("avg_clear_sky_radiation", 0), 2)
    all_sky_data_quality = normalize_quality_percent(estimate_solar_data.get("all_sky_data_quality", 0))
    clear_sky_data_quality = normalize_quality_percent(estimate_solar_data.get("clear_sky_data_quality", 0))
    monthly_all_sky = {
        str(i).zfill(2): round(estimate_solar_data.get("monthly_all_sky", {}).get(str(i).zfill(2), 0), 2)
        for i in range(1, 13)
    }
    monthly_clear_sky = {
        str(i).zfill(2): round(estimate_solar_data.get("monthly_clear_sky", {}).get(str(i).zfill(2), 0), 2)
        for i in range(1, 13)
    }
    best_all_sky = round(max(monthly_all_sky.values()), 2) if monthly_all_sky else 0
    worst_all_sky = round(min(monthly_all_sky.values()), 2) if monthly_all_sky else 0
    best_clear_sky = round(max(monthly_clear_sky.values()), 2) if monthly_clear_sky else 0
    worst_clear_sky = round(min(monthly_clear_sky.values()), 2) if monthly_clear_sky else 0

    has_pvwatts_profile = bool(
        ((estimate_solar_data.get("pvwatts") or {}).get("outputs") or {}).get("ac_monthly_per_kw")
    )
    model_provider = solar_provider if solar_provider == "nrel-pvwatts" and has_pvwatts_profile else "nasa"

    if model_provider == "nrel-pvwatts":
        production_model = build_nrel_pvwatts_production_model(
            system_size_kw,
            estimate_solar_data,
            input_data.panel_efficiency,
            input_data.electricity_rate,
            modeling_context,
        )
    else:
        production_model = build_solar_production_model(
            system_size_kw,
            monthly_all_sky,
            avg_all_sky_radiation,
            input_data.panel_efficiency,
            input_data.electricity_rate,
            roof_selection,
            modeling_context,
        )
    annual_production = production_model["annual_production"]
    daily_production = production_model["daily_production"]
    annual_savings = round(annual_production * input_data.electricity_rate, 2)
    system_cost = round(system_size_kw * 1000 * input_data.installation_cost_per_watt, 2)
    payback_period = round(system_cost / annual_savings, 2) if annual_savings > 0 else None
    total_savings = round(sum([annual_savings * (1.02 ** year) for year in range(25)]), 2)

    overall_quality = (
        "high"
        if all_sky_data_quality > 80 and clear_sky_data_quality > 80
        else "medium"
        if all_sky_data_quality > 60 and clear_sky_data_quality > 60
        else "low"
    )
    property_preview = property_record.get("property_preview") or {}
    match_quality = property_preview.get("match_quality") or "unknown"
    assumptions = build_solar_assumptions(
        system_size_kw,
        sizing_source,
        roof_selection,
        input_data.panel_efficiency,
        input_data.electricity_rate,
        input_data.installation_cost_per_watt,
        estimate_data_source or "unknown",
        model_provider,
        overall_quality,
        production_model,
    )
    confidence = build_solar_confidence(
        match_quality,
        overall_quality,
        sizing_source,
        estimate_data_source or "unknown",
        model_provider,
        roof_selection,
        production_model,
    )

    return {
        "address": f"{address['street']}, {address['city']}, {address['state']} {address['zip']}, {address['country']}",
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "avg_all_sky_radiation": avg_all_sky_radiation,
        "avg_clear_sky_radiation": avg_clear_sky_radiation,
        "all_sky_data_quality": all_sky_data_quality,
        "clear_sky_data_quality": clear_sky_data_quality,
        "monthly_all_sky": monthly_all_sky,
        "monthly_clear_sky": monthly_clear_sky,
        "best_all_sky": best_all_sky,
        "worst_all_sky": worst_all_sky,
        "best_clear_sky": best_clear_sky,
        "worst_clear_sky": worst_clear_sky,
        "monthly_production": production_model["monthly_production"],
        "monthly_savings": production_model["monthly_savings"],
        "unit": "kWh/m²/day",
        "period": "daily average",
        "match_quality": match_quality,
        "system_size_kw": system_size_kw,
        "sizing_source": sizing_source,
        "roof_area_square_feet": roof_selection.get("areaSquareFeet") if roof_selection else None,
        "roof_area_square_meters": roof_selection.get("areaSquareMeters") if roof_selection else None,
        "assumptions": assumptions,
        "confidence": confidence,
        "production_model": production_model,
        "daily_production": daily_production,
        "annual_production": annual_production,
        "annual_savings": annual_savings,
        "system_cost": system_cost,
        "payback_period": payback_period,
        "total_savings_25_years": total_savings,
        "specific_yield": production_model["specific_yield"],
        "capacity_factor": production_model["capacity_factor"],
        "peak_month": production_model["peak_month"],
        "lowest_month": production_model["lowest_month"],
        "time_zone": time_zone,
        "data_source": estimate_data_source or "unknown",
        "data_provider": solar_provider,
        "data_quality": overall_quality,
    }


@app.post(
    "/api/property-record/find",
    response_model=dict,
    summary="Find Property Record",
    description="Finds the latest saved property record for a normalized address so the frontend can reopen saved roof or garden context.",
)
def find_property_record(address: Address):
    record = find_property_record_by_address(address.model_dump())
    return {"record": record}


@app.post(
    "/api/property-record/recent",
    response_model=dict,
    summary="List Recent Property Records",
    description="Returns recent saved property records so the frontend can reopen prior roof or garden plans without retyping an address.",
)
def recent_property_records(payload: PropertyRecordRecentRequest):
    records = list_property_records(
        limit=payload.max_items,
        require_garden_zones=payload.require_garden_zones,
    )
    return {"records": records}


@app.post(
    "/api/property-record",
    response_model=dict,
    summary="Upsert Property Record",
    description="Creates or updates the current property record, including normalized address, map preview, roof geometry, and garden zones.",
)
def save_property_record(payload: PropertyRecordRequest):
    guid = payload.guid or str(uuid.uuid4())
    address = payload.address.model_dump()
    property_preview = payload.property_preview
    roof_selection = payload.roof_selection.model_dump() if payload.roof_selection else None
    garden_zones = (
        [garden_zone.model_dump() for garden_zone in payload.garden_zones]
        if payload.garden_zones is not None
        else None
    )

    upsert_kwargs = {
        "property_preview": property_preview,
        "property_context": payload.property_context,
        "property_climate": payload.property_climate,
        "roof_selection": roof_selection,
    }
    if garden_zones is not None:
        upsert_kwargs["garden_zones"] = garden_zones

    upsert_property_record(guid, address, **upsert_kwargs)
    saved_record = get_property_record(guid) or {}

    return {
        "guid": guid,
        "address": saved_record.get("address", address),
        "property_preview": saved_record.get("property_preview", property_preview),
        "property_context": saved_record.get("property_context", payload.property_context),
        "property_climate": saved_record.get("property_climate", payload.property_climate),
        "roof_selection": saved_record.get("roof_selection", roof_selection),
        "garden_zones": saved_record.get("garden_zones", garden_zones or []),
        "saved_solar_reports": saved_record.get("saved_solar_reports", []),
        "stored_at": saved_record.get("stored_at"),
    }


@app.post(
    "/api/property-preview",
    response_model=dict,
    summary="Locate Property",
    description="Geocodes an address and returns location data for map centering.",
)
def preview_property(address: Address):
    address_dict = address.model_dump()
    cache_key = build_address_lookup_key(address_dict)
    cached_preview = get_geocode_cache(FORWARD_PROPERTY_PREVIEW_CACHE, cache_key)
    if cached_preview:
        return cached_preview

    formatted_address = format_address(address_dict)
    geocode_result = geocode_location(address_dict)
    location = geocode_result["location"]
    raw_address = getattr(location, "raw", {}).get("address", {}) or {}
    normalized_address = extract_address_parts(raw_address)

    payload = {
        "query": formatted_address,
        "formatted_address": location.address or formatted_address,
        "latitude": round(location.latitude, 6),
        "longitude": round(location.longitude, 6),
        "bounds": parse_bounding_box(location),
        "source": geocode_result.get("source", "nominatim"),
        "match_quality": geocode_result["match_quality"],
        "match_score": geocode_result["match_score"],
        "address": normalized_address,
    }
    store_geocode_cache(
        FORWARD_PROPERTY_PREVIEW_CACHE,
        cache_key,
        payload,
        payload.get("source"),
    )
    return payload


@app.post(
    "/api/reverse-geocode",
    response_model=dict,
    summary="Resolve Browser Location",
    description="Reverse geocodes browser coordinates into a normalized address and map preview.",
)
def reverse_geocode_property(coordinates: Coordinates):
    cache_key = build_coordinate_lookup_key(coordinates.latitude, coordinates.longitude)
    cached_preview = get_geocode_cache(REVERSE_PROPERTY_PREVIEW_CACHE, cache_key)
    if cached_preview:
        return cached_preview

    location = reverse_geocode_location(coordinates.latitude, coordinates.longitude)
    raw_address = getattr(location, "raw", {}).get("address", {}) or {}
    normalized_address = extract_address_parts(raw_address)

    payload = {
        "query": f"{round(coordinates.latitude, 6)}, {round(coordinates.longitude, 6)}",
        "formatted_address": location.address or format_address(normalized_address),
        "latitude": round(location.latitude, 6),
        "longitude": round(location.longitude, 6),
        "bounds": parse_bounding_box(location),
        "source": "browser-location",
        "match_quality": "high",
        "match_score": None,
        "address": normalized_address,
    }
    store_geocode_cache(
        REVERSE_PROPERTY_PREVIEW_CACHE,
        cache_key,
        payload,
        payload.get("source"),
    )
    return payload


@app.post(
    "/api/property-context",
    response_model=dict,
    summary="Get Property Context",
    description="Returns first-pass building, vegetation, and terrain context around the property for Solar Buddy and Garden Buddy.",
)
def get_property_context(payload: PropertyContextRequest):
    try:
        return get_property_context_snapshot(
            payload.latitude,
            payload.longitude,
            bounds=payload.bounds.model_dump() if payload.bounds else None,
            match_quality=payload.match_quality,
        )
    except requests.HTTPError as exc:
        logger.error("Property context upstream request failed: %s", str(exc))
        raise HTTPException(status_code=502, detail="Unable to load property context data")
    except Exception as exc:
        logger.error("Property context processing failed: %s", str(exc))
        raise HTTPException(status_code=500, detail="Unable to process property context data")


@app.post(
    "/api/space-weather",
    response_model=dict,
    summary="Get Space Weather",
    description="Returns live flare, solar wind, and geomagnetic context localized to the provided coordinates.",
)
def get_space_weather(coordinates: Coordinates):
    time_zone = get_timezone(coordinates.latitude, coordinates.longitude) or "UTC"
    try:
        return get_space_weather_snapshot(
            coordinates.latitude,
            coordinates.longitude,
            time_zone,
            force_refresh=coordinates.force_refresh,
        )
    except requests.HTTPError as exc:
        logger.error("Space weather upstream request failed: %s", str(exc))
        raise HTTPException(status_code=502, detail="Unable to load space weather data")
    except Exception as exc:
        logger.error("Space weather processing failed: %s", str(exc))
        raise HTTPException(status_code=500, detail="Unable to process space weather data")


@app.post(
    "/api/surface-irradiance",
    response_model=dict,
    summary="Get Surface Irradiance",
    description="Returns live and near-term surface irradiance conditions for the provided coordinates.",
)
def get_surface_irradiance(coordinates: Coordinates):
    time_zone = get_timezone(coordinates.latitude, coordinates.longitude) or "UTC"
    try:
        return get_surface_irradiance_snapshot(
            coordinates.latitude,
            coordinates.longitude,
            time_zone,
            force_refresh=coordinates.force_refresh,
        )
    except requests.HTTPError as exc:
        logger.error("Surface irradiance upstream request failed: %s", str(exc))
        raise HTTPException(status_code=502, detail="Unable to load surface irradiance data")
    except Exception as exc:
        logger.error("Surface irradiance processing failed: %s", str(exc))
        raise HTTPException(status_code=500, detail="Unable to process surface irradiance data")


@app.post(
    "/api/garden-crop-catalog",
    response_model=dict,
    summary="Get Garden Crop Catalog",
    description="Returns the persisted zone-aware crop catalog used by Garden Buddy recommendations.",
)
def fetch_garden_crop_catalog():
    payload = get_garden_crop_catalog("default")
    if not payload:
        raise HTTPException(status_code=404, detail="Garden crop catalog unavailable")
    return payload


@app.post(
    "/api/property-climate",
    response_model=dict,
    summary="Get Property Climate",
    description="Returns historical climate context and an estimated hardiness band for the provided coordinates.",
)
def get_property_climate(coordinates: Coordinates):
    if not coordinates.force_refresh:
        cached_snapshot = get_cached_property_climate(
            coordinates.latitude,
            coordinates.longitude,
        )
        if cached_snapshot:
            return cached_snapshot

    time_zone = get_timezone(coordinates.latitude, coordinates.longitude) or "UTC"
    try:
        snapshot = get_property_climate_snapshot(
            coordinates.latitude,
            coordinates.longitude,
            time_zone,
        )
        store_cached_property_climate(
            coordinates.latitude,
            coordinates.longitude,
            snapshot,
        )
        return snapshot
    except requests.HTTPError as exc:
        logger.error("Property climate upstream request failed: %s", str(exc))
        raise HTTPException(status_code=502, detail="Unable to load property climate data")
    except Exception as exc:
        logger.error("Property climate processing failed: %s", str(exc))
        raise HTTPException(status_code=500, detail="Unable to process property climate data")


@app.post("/api/solar-potential", response_model=dict, summary="Calculate Solar Potential", description="Calculates the solar potential based on user data and system specifications.")
def calculate_solar_potential(input_data: SolarPotentialRequest):
    return build_solar_estimate_response(input_data)


@app.post(
    "/api/solar-report",
    response_model=dict,
    summary="Save Solar Report",
    description="Recomputes the current roof-backed solar estimate and saves a report snapshot to the property record.",
)
def save_solar_report(payload: SolarReportRequest):
    estimate_request = SolarPotentialRequest(
        guid=payload.guid,
        system_size=None,
        panel_efficiency=payload.panel_efficiency,
        electricity_rate=payload.electricity_rate,
        installation_cost_per_watt=payload.installation_cost_per_watt,
        roof_selection=payload.roof_selection,
    )
    estimate = build_solar_estimate_response(estimate_request)
    property_record = get_property_record(payload.guid)
    if not property_record:
        raise HTTPException(status_code=404, detail="Property record not found")

    address = property_record["address"]
    reports = property_record.get("saved_solar_reports") or []
    report = build_saved_solar_report(address, estimate, payload.report_name)
    next_reports = [report, *reports][:8]
    upsert_property_record(
        payload.guid,
        address,
        property_preview=property_record.get("property_preview"),
        roof_selection=property_record.get("roof_selection"),
        garden_zones=property_record.get("garden_zones") or [],
        saved_solar_reports=next_reports,
    )

    return {
        "report": report,
        "reports": next_reports,
        "estimate": estimate,
    }


@app.post(
    "/api/solar-quote",
    response_model=dict,
    summary="Create Shareable Solar Quote",
    description="Promotes a saved solar report into a shareable homeowner quote without changing the underlying report snapshot.",
)
def create_solar_quote(payload: SolarQuoteRequest):
    property_record = get_property_record(payload.guid)
    if not property_record:
        raise HTTPException(status_code=404, detail="Property record not found")

    reports = property_record.get("saved_solar_reports") or []
    target_report = next((report for report in reports if report.get("id") == payload.report_id), None)
    if not target_report:
        raise HTTPException(status_code=404, detail="Saved solar report not found")

    quote = build_homeowner_quote(
        property_record["address"],
        target_report,
        target_report.get("homeowner_quote"),
    )
    hydrated_quote = hydrate_homeowner_quote(quote, property_record["address"])
    next_reports = []
    updated_report = None
    for report in reports:
        if report.get("id") != payload.report_id:
            next_reports.append(report)
            continue

        updated_report = {
            **report,
            "homeowner_quote": quote,
        }
        next_reports.append(updated_report)

    upsert_property_record(
        payload.guid,
        property_record["address"],
        property_preview=property_record.get("property_preview"),
        roof_selection=property_record.get("roof_selection"),
        garden_zones=property_record.get("garden_zones") or [],
        saved_solar_reports=next_reports,
    )

    return {
        "quote": hydrated_quote,
        "report": {
            **(updated_report or {}),
            "homeowner_quote": hydrated_quote,
        }
        if updated_report
        else None,
        "reports": [
            {
                **report,
                "homeowner_quote": (
                    hydrated_quote
                    if report.get("id") == payload.report_id and report.get("homeowner_quote")
                    else report.get("homeowner_quote")
                ),
            }
            if report.get("homeowner_quote")
            else report
            for report in next_reports
        ],
    }


@app.post(
    "/api/solar-quote/{quote_id}/lead",
    response_model=dict,
    summary="Capture Solar Quote Lead",
    description="Captures a qualified homeowner lead from a shareable quote and queues it for installer follow-up.",
)
def capture_solar_quote_lead(quote_id: str, payload: SolarQuoteLeadRequest):
    match = find_solar_quote(quote_id)
    if not match:
        raise HTTPException(status_code=404, detail="Solar quote not found")

    full_name = _clean_contact_value(payload.full_name)
    email = _clean_contact_value(payload.email).lower()
    phone = _clean_contact_value(payload.phone)
    if not full_name:
        raise HTTPException(status_code=400, detail="Full name is required")
    if "@" not in email:
        raise HTTPException(status_code=400, detail="A valid email is required")
    if len(_normalized_phone_digits(phone)) < 10:
        raise HTTPException(status_code=400, detail="A valid phone number is required")
    if not payload.consent_to_contact:
        raise HTTPException(status_code=400, detail="Consent to contact is required")

    record = match["record"]
    report = match["report"]
    lead = build_solar_quote_lead(quote_id, record, report, payload)
    store_solar_quote_lead(lead)
    leads = list_solar_quote_leads(quote_id)
    hydrated_quote = hydrate_homeowner_quote(
        match["quote"],
        record.get("address"),
        latest_lead=leads[0] if leads else lead,
        lead_count=len(leads),
    )

    return {
        "lead": lead,
        "quote": hydrated_quote,
        "report": {
            **report,
            "homeowner_quote": hydrated_quote,
        },
    }


@app.get(
    "/api/solar-quote/{quote_id}",
    response_model=dict,
    summary="Get Shareable Solar Quote",
    description="Returns the saved solar report snapshot and share metadata for a homeowner-facing quote page.",
)
def get_solar_quote(quote_id: str):
    match = find_solar_quote(quote_id)
    if not match:
        raise HTTPException(status_code=404, detail="Solar quote not found")

    record = match["record"]
    leads = list_solar_quote_leads(quote_id)
    return {
        "quote": hydrate_homeowner_quote(
            match["quote"],
            record.get("address"),
            latest_lead=leads[0] if leads else None,
            lead_count=len(leads),
        ),
        "report": {
            **match["report"],
            "homeowner_quote": hydrate_homeowner_quote(
                match["quote"],
                record.get("address"),
                latest_lead=leads[0] if leads else None,
                lead_count=len(leads),
            ),
        },
        "address": record.get("address"),
        "property_preview": record.get("property_preview"),
    }


@app.get("/health", response_model=dict, include_in_schema=False)
def health_check():
    return {"status": "ok"}

@app.get("/api/privacy-policy", response_model=dict, summary="Get Privacy Policy", description="Returns the privacy policy of the application.")
def get_privacy_policy():
    policy_text = """
    Your privacy is important to us. This privacy policy explains what personal data we collect from you and how we use it.
    """
    return {"policyText": policy_text}

# Custom OpenAPI schema generation
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Solar Potential Calculator API",
        version="1.0.0",
        description="API for calculating solar potential and storing user data.",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# Serve Swagger UI
@app.get("/docs", include_in_schema=False)
async def get_swagger_ui():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Solar Potential Calculator API")


#uvicorn main:app --reload --port 8000 - to start the server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
