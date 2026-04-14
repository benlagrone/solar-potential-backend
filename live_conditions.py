from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
import logging
import math
import time
from statistics import median
from typing import Any, Optional

import requests


logger = logging.getLogger(__name__)

_CACHE: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}

NOAA_SCALES_URL = "https://services.swpc.noaa.gov/products/noaa-scales.json"
NOAA_ALERTS_URL = "https://services.swpc.noaa.gov/products/alerts.json"
NOAA_PLASMA_URL = "https://services.swpc.noaa.gov/products/solar-wind/plasma-2-hour.json"
NOAA_XRAY_URL = "https://services.swpc.noaa.gov/json/goes/primary/xrays-1-day.json"
NOAA_AURORA_OVATION_URL = "https://services.swpc.noaa.gov/json/ovation_aurora_latest.json"
NOAA_DRAP_URL = "https://services.swpc.noaa.gov/text/drap_global_frequencies.txt"
NOAA_GLOTEC_INDEX_URL = "https://services.swpc.noaa.gov/products/glotec/geojson_2d_urt.json"
NOAA_BASE_URL = "https://services.swpc.noaa.gov"
NASA_DONKI_FLR_URL = "https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/get/FLR"
NASA_DONKI_GST_URL = "https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/get/GST"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

AURORA_VIEWLINE_THRESHOLD = 1
AURORA_VIEWLINE_LONGITUDE_WINDOW_DEGREES = 12
AURORA_NEARBY_DISTANCE_KM = 1000
AURORA_DISTANT_DISTANCE_KM = 1800


def _normalize_params(params: Optional[dict[str, Any]]) -> tuple[tuple[str, str], ...]:
    if not params:
        return ()
    return tuple(sorted((str(key), str(value)) for key, value in params.items()))


def _timestamp_to_iso(timestamp: float) -> str:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).isoformat()


def _build_source_freshness(
    cache_entry: dict[str, Any],
    now_timestamp: float,
    *,
    source: str,
    cache_hit: bool,
    refresh_failed: bool = False,
):
    fetched_at = float(cache_entry.get("fetched_at") or now_timestamp)
    expires_at = float(cache_entry.get("expires_at") or now_timestamp)
    is_stale = expires_at <= now_timestamp

    if is_stale:
        status = "stale"
    elif refresh_failed:
        status = "refresh-failed"
    else:
        status = "fresh"

    return {
        "source": source,
        "status": status,
        "fetched_at": _timestamp_to_iso(fetched_at),
        "expires_at": _timestamp_to_iso(expires_at),
        "ttl_seconds": int(cache_entry.get("ttl_seconds") or 0),
        "age_seconds": round(max(now_timestamp - fetched_at, 0.0), 1),
        "seconds_until_expiry": round(max(expires_at - now_timestamp, 0.0), 1),
        "is_stale": is_stale,
        "cache_hit": cache_hit,
        "refresh_failed": refresh_failed,
    }


def _aggregate_freshness(source_freshness: dict[str, dict[str, Any]]):
    checked_at = datetime.now(timezone.utc)
    sources = [entry for entry in source_freshness.values() if entry]
    fetched_times = [_parse_time(entry.get("fetched_at")) for entry in sources]
    expiry_times = [_parse_time(entry.get("expires_at")) for entry in sources]
    valid_fetched_times = [value for value in fetched_times if value]
    valid_expiry_times = [value for value in expiry_times if value]
    oldest_fetch = min(valid_fetched_times) if valid_fetched_times else checked_at
    latest_fetch = max(valid_fetched_times) if valid_fetched_times else checked_at
    earliest_expiry = min(valid_expiry_times) if valid_expiry_times else checked_at
    is_stale = any(entry.get("is_stale") for entry in sources)
    refresh_failed = any(entry.get("refresh_failed") for entry in sources)

    if is_stale:
        status = "stale"
    elif refresh_failed:
        status = "refresh-failed"
    else:
        status = "fresh"

    return {
        "status": status,
        "checked_at": checked_at.isoformat(),
        "fetched_at": oldest_fetch.isoformat(),
        "latest_fetched_at": latest_fetch.isoformat(),
        "expires_at": earliest_expiry.isoformat(),
        "age_seconds": round(max((checked_at - oldest_fetch).total_seconds(), 0.0), 1),
        "seconds_until_expiry": round(max((earliest_expiry - checked_at).total_seconds(), 0.0), 1),
        "is_stale": is_stale,
        "refresh_failed": refresh_failed,
        "source_count": len(sources),
        "sources": source_freshness,
    }


def _build_unavailable_source_freshness(
    source: str,
    *,
    checked_at: Optional[datetime] = None,
    ttl_seconds: int = 0,
    detail: Optional[str] = None,
):
    observed_at = checked_at or datetime.now(timezone.utc)
    return {
        "source": source,
        "status": "refresh-failed",
        "fetched_at": observed_at.isoformat(),
        "expires_at": observed_at.isoformat(),
        "ttl_seconds": ttl_seconds,
        "age_seconds": 0.0,
        "seconds_until_expiry": 0.0,
        "is_stale": True,
        "cache_hit": False,
        "refresh_failed": True,
        "detail": detail,
    }


def _decode_json_payload(text: str):
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        repaired = str(text or "").strip()
        if not repaired:
            raise

        repaired = repaired.rstrip(",")
        if repaired.startswith("["):
            repaired = repaired.rstrip(",")
            if repaired.count("[") > repaired.count("]"):
                repaired += "]" * (repaired.count("[") - repaired.count("]"))
            return json.loads(repaired)

        if repaired.startswith("{"):
            if repaired.count("{") > repaired.count("}"):
                repaired += "}" * (repaired.count("{") - repaired.count("}"))
            return json.loads(repaired)

        raise


def _fetch_optional_json(
    url: str,
    params: Optional[dict[str, Any]] = None,
    ttl_seconds: int = 300,
    *,
    force_refresh: bool = False,
    source_name: Optional[str] = None,
    default_data: Any = None,
):
    normalized_source = source_name or url
    try:
        return _fetch_json(
            url,
            params=params,
            ttl_seconds=ttl_seconds,
            force_refresh=force_refresh,
            return_metadata=True,
            source_name=normalized_source,
        )
    except (requests.RequestException, json.JSONDecodeError) as exc:
        logger.warning("Optional upstream fetch failed for %s: %s", normalized_source, str(exc))
        return {
            "data": default_data,
            "freshness": _build_unavailable_source_freshness(
                normalized_source,
                ttl_seconds=ttl_seconds,
                detail=str(exc),
            ),
        }


def _fetch_optional_text(
    url: str,
    params: Optional[dict[str, Any]] = None,
    ttl_seconds: int = 300,
    *,
    force_refresh: bool = False,
    source_name: Optional[str] = None,
    default_data: str = "",
):
    normalized_source = source_name or url
    try:
        return _fetch_text(
            url,
            params=params,
            ttl_seconds=ttl_seconds,
            force_refresh=force_refresh,
            return_metadata=True,
            source_name=normalized_source,
        )
    except requests.RequestException as exc:
        logger.warning("Optional upstream text fetch failed for %s: %s", normalized_source, str(exc))
        return {
            "data": default_data,
            "freshness": _build_unavailable_source_freshness(
                normalized_source,
                ttl_seconds=ttl_seconds,
                detail=str(exc),
            ),
        }


def _fetch_json(
    url: str,
    params: Optional[dict[str, Any]] = None,
    ttl_seconds: int = 300,
    *,
    force_refresh: bool = False,
    return_metadata: bool = False,
    source_name: Optional[str] = None,
):
    key = (url, _normalize_params(params))
    now = time.time()
    cached = _CACHE.get(key)
    normalized_source = source_name or url
    if cached and cached["expires_at"] > now and not force_refresh:
        result = {
            "data": cached["data"],
            "freshness": _build_source_freshness(
                cached,
                now,
                source=normalized_source,
                cache_hit=True,
            ),
        }
        return result if return_metadata else result["data"]

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        data = _decode_json_payload(response.text)
        fetched_at = time.time()
        cache_entry = {
            "data": data,
            "fetched_at": fetched_at,
            "expires_at": fetched_at + ttl_seconds,
            "ttl_seconds": ttl_seconds,
        }
        _CACHE[key] = cache_entry
        result = {
            "data": data,
            "freshness": _build_source_freshness(
                cache_entry,
                fetched_at,
                source=normalized_source,
                cache_hit=False,
            ),
        }
        return result if return_metadata else result["data"]
    except (requests.RequestException, json.JSONDecodeError):
        if cached:
            logger.warning("Using cached fallback for %s after upstream fetch failure", normalized_source, exc_info=True)
            result = {
                "data": cached["data"],
                "freshness": _build_source_freshness(
                    cached,
                    now,
                    source=normalized_source,
                    cache_hit=True,
                    refresh_failed=True,
                ),
            }
            return result if return_metadata else result["data"]
        raise


def _fetch_text(
    url: str,
    params: Optional[dict[str, Any]] = None,
    ttl_seconds: int = 300,
    *,
    force_refresh: bool = False,
    return_metadata: bool = False,
    source_name: Optional[str] = None,
):
    key = (url, _normalize_params(params))
    now = time.time()
    cached = _CACHE.get(key)
    normalized_source = source_name or url
    if cached and cached["expires_at"] > now and not force_refresh:
        result = {
            "data": cached["data"],
            "freshness": _build_source_freshness(
                cached,
                now,
                source=normalized_source,
                cache_hit=True,
            ),
        }
        return result if return_metadata else result["data"]

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()
        fetched_at = time.time()
        cache_entry = {
            "data": response.text,
            "fetched_at": fetched_at,
            "expires_at": fetched_at + ttl_seconds,
            "ttl_seconds": ttl_seconds,
        }
        _CACHE[key] = cache_entry
        result = {
            "data": response.text,
            "freshness": _build_source_freshness(
                cache_entry,
                fetched_at,
                source=normalized_source,
                cache_hit=False,
            ),
        }
        return result if return_metadata else result["data"]
    except requests.RequestException:
        if cached:
            logger.warning("Using cached text fallback for %s after upstream fetch failure", normalized_source, exc_info=True)
            result = {
                "data": cached["data"],
                "freshness": _build_source_freshness(
                    cached,
                    now,
                    source=normalized_source,
                    cache_hit=True,
                    refresh_failed=True,
                ),
            }
            return result if return_metadata else result["data"]
        raise


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value: Any):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average(values: list[Optional[float]]):
    numeric_values = [value for value in values if value is not None]
    if not numeric_values:
        return None
    return sum(numeric_values) / len(numeric_values)


def _round_float(value: Optional[float], digits: int = 1):
    if value is None:
        return None
    return round(value, digits)


def _celsius_to_fahrenheit(value: Optional[float]):
    if value is None:
        return None
    return (value * 9 / 5) + 32


def _megajoules_to_kwh(value: Optional[float]):
    if value is None:
        return None
    return value / 3.6


def _parse_time(value: Optional[str]):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_scale(bucket: Optional[dict[str, Any]]) -> dict[str, Any]:
    bucket = bucket or {}
    scale_value = bucket.get("Scale")
    try:
        normalized_scale = int(scale_value) if scale_value not in (None, "") else 0
    except (TypeError, ValueError):
        normalized_scale = 0

    return {
        "scale": normalized_scale,
        "label": str(bucket.get("Text") or "none").lower(),
        "probability_minor": bucket.get("MinorProb"),
        "probability_major": bucket.get("MajorProb"),
        "probability": bucket.get("Prob"),
    }


def _extract_scale_entry(scales_payload: Any, key: str) -> dict[str, Any]:
    if not isinstance(scales_payload, dict):
        return {}
    return scales_payload.get(key) or {}


def _format_xray_class(flux_value: Any) -> str:
    flux = max(_safe_float(flux_value), 0.0)
    if flux >= 1e-4:
        return f"X{flux / 1e-4:.1f}"
    if flux >= 1e-5:
        return f"M{flux / 1e-5:.1f}"
    if flux >= 1e-6:
        return f"C{flux / 1e-6:.1f}"
    if flux >= 1e-7:
        return f"B{flux / 1e-7:.1f}"
    return f"A{flux / 1e-8:.1f}"


def _extract_alert_headline(message: str) -> str:
    for raw_line in (message or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if any(token in line for token in ("WARNING:", "WATCH:", "ALERT:")):
            return line
    return ""


def _categorize_alert_level(headline: str) -> str:
    upper_headline = headline.upper()
    if "WARNING:" in upper_headline:
        return "warning"
    if "WATCH:" in upper_headline:
        return "watch"
    if "ALERT:" in upper_headline:
        return "alert"
    return "info"


def _latitude_band(latitude: float) -> str:
    absolute_latitude = abs(latitude)
    if absolute_latitude >= 55:
        return "high"
    if absolute_latitude >= 40:
        return "mid"
    if absolute_latitude >= 25:
        return "low"
    return "equatorial"


def _normalize_longitude(longitude: float) -> float:
    return ((longitude + 180.0) % 360.0) - 180.0


def _normalize_longitude_360(longitude: float) -> float:
    return longitude % 360.0


def _longitude_distance_degrees(left_longitude: float, right_longitude: float) -> float:
    delta = abs(_normalize_longitude(left_longitude) - _normalize_longitude(right_longitude))
    return min(delta, 360.0 - delta)


def _haversine_km(latitude_a: float, longitude_a: float, latitude_b: float, longitude_b: float) -> float:
    radius_km = 6371.0
    delta_latitude = math.radians(latitude_b - latitude_a)
    delta_longitude = math.radians(_normalize_longitude(longitude_b - longitude_a))
    latitude_a_radians = math.radians(latitude_a)
    latitude_b_radians = math.radians(latitude_b)
    haversine = (
        math.sin(delta_latitude / 2.0) ** 2
        + math.cos(latitude_a_radians)
        * math.cos(latitude_b_radians)
        * math.sin(delta_longitude / 2.0) ** 2
    )
    return 2.0 * radius_km * math.asin(math.sqrt(max(0.0, min(1.0, haversine))))


def _build_drap_context(text_payload: str, latitude: float, longitude: float):
    default_context = {
        "source": "noaa-drap",
        "status": "unavailable",
        "observed_at": None,
        "recovery_time": None,
        "xray_message": None,
        "proton_message": None,
        "absorption_frequency_mhz": None,
        "risk": "low",
        "detail": "D-RAP absorption detail is unavailable right now.",
    }
    if not text_payload:
        return default_context

    lines = [line.rstrip() for line in str(text_payload).splitlines() if line.strip()]
    longitude_line = ""
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "|" in stripped:
            continue

        tokens = stripped.split()
        try:
            [float(token) for token in tokens]
        except ValueError:
            continue

        if len(tokens) >= 5:
            longitude_line = stripped
            break

    if not longitude_line:
        return default_context

    try:
        longitude_values = [float(token) for token in longitude_line.split()]
    except ValueError:
        return default_context

    latitude_rows = []
    for line in lines:
        if "|" not in line:
            continue
        latitude_part, values_part = line.split("|", 1)
        try:
            row_latitude = float(latitude_part.strip())
            row_values = [float(token) for token in values_part.split()]
        except ValueError:
            continue
        if len(row_values) != len(longitude_values):
            continue
        latitude_rows.append((row_latitude, row_values))

    if not latitude_rows:
        return default_context

    nearest_latitude, nearest_row = min(
        latitude_rows,
        key=lambda row: abs(row[0] - latitude),
    )
    normalized_longitude = _normalize_longitude(longitude)
    nearest_index = min(
        range(len(longitude_values)),
        key=lambda index: _longitude_distance_degrees(longitude_values[index], normalized_longitude),
    )
    nearest_longitude = longitude_values[nearest_index]
    absorption_frequency = max(float(nearest_row[nearest_index]), 0.0)

    if absorption_frequency >= 12:
        risk = "high"
    elif absorption_frequency >= 6:
        risk = "moderate"
    elif absorption_frequency > 0:
        risk = "low"
    else:
        risk = "low"

    metadata = {}
    for line in lines:
        if not line.startswith("#"):
            continue
        if ":" not in line:
            continue
        label, value = line[1:].split(":", 1)
        metadata[label.strip().lower()] = value.strip()

    detail = (
        "D-RAP does not show meaningful HF absorption at this grid point right now."
        if absorption_frequency <= 0
        else (
            f"D-RAP suggests HF absorption effects up through about {absorption_frequency:.1f} MHz "
            f"near {nearest_latitude:.0f}° latitude and {nearest_longitude:.0f}° longitude."
        )
    )

    return {
        "source": "noaa-drap",
        "status": "available",
        "observed_at": metadata.get("product valid at"),
        "recovery_time": metadata.get("estimated recovery time"),
        "xray_message": metadata.get("x-ray message"),
        "xray_warning": metadata.get("x-ray warning"),
        "proton_message": metadata.get("proton message"),
        "proton_warning": metadata.get("proton warning"),
        "absorption_frequency_mhz": round(absorption_frequency, 1),
        "risk": risk,
        "nearest_grid_point": {
            "latitude": nearest_latitude,
            "longitude": nearest_longitude,
        },
        "detail": detail,
    }


def _fetch_glotec_context(latitude: float, longitude: float, force_refresh: bool = False):
    index_response = _fetch_optional_json(
        NOAA_GLOTEC_INDEX_URL,
        ttl_seconds=300,
        force_refresh=force_refresh,
        source_name="noaa-glotec-index",
        default_data=[],
    )
    index_payload = index_response.get("data") or []
    if not isinstance(index_payload, list) or not index_payload:
        return {
            "source": "noaa-glotec",
            "status": "unavailable",
            "detail": "GloTEC grid detail is unavailable right now.",
            "freshness": _aggregate_freshness(
                {"noaa-glotec-index": index_response.get("freshness") or {}}
            ),
        }

    latest_entry = index_payload[-1] or {}
    relative_url = str(latest_entry.get("url") or "").strip()
    if not relative_url:
        return {
            "source": "noaa-glotec",
            "status": "unavailable",
            "detail": "GloTEC index did not provide a latest grid URL.",
            "freshness": _aggregate_freshness(
                {"noaa-glotec-index": index_response.get("freshness") or {}}
            ),
        }

    file_url = f"{NOAA_BASE_URL}{relative_url}" if relative_url.startswith("/") else relative_url
    grid_response = _fetch_optional_json(
        file_url,
        ttl_seconds=300,
        force_refresh=force_refresh,
        source_name="noaa-glotec-grid",
        default_data={},
    )
    grid_payload = grid_response.get("data") or {}
    features = grid_payload.get("features") or []

    if not isinstance(features, list) or not features:
        return {
            "source": "noaa-glotec",
            "status": "unavailable",
            "detail": "GloTEC grid content is unavailable right now.",
            "freshness": _aggregate_freshness(
                {
                    "noaa-glotec-index": index_response.get("freshness") or {},
                    "noaa-glotec-grid": grid_response.get("freshness") or {},
                }
            ),
        }

    nearest_feature = None
    nearest_distance = None
    for feature in features:
        coordinates = ((feature.get("geometry") or {}).get("coordinates") or [])
        if len(coordinates) < 2:
            continue
        feature_longitude = _safe_float(coordinates[0], None)
        feature_latitude = _safe_float(coordinates[1], None)
        if feature_latitude is None or feature_longitude is None:
            continue
        distance_km = _haversine_km(latitude, longitude, feature_latitude, feature_longitude)
        if nearest_distance is None or distance_km < nearest_distance:
            nearest_distance = distance_km
            nearest_feature = feature

    if not nearest_feature:
        return {
            "source": "noaa-glotec",
            "status": "unavailable",
            "detail": "GloTEC did not return a usable local grid point.",
            "freshness": _aggregate_freshness(
                {
                    "noaa-glotec-index": index_response.get("freshness") or {},
                    "noaa-glotec-grid": grid_response.get("freshness") or {},
                }
            ),
        }

    properties = nearest_feature.get("properties") or {}
    anomaly = _optional_float(properties.get("anomaly"))
    tec = _optional_float(properties.get("tec"))
    quality_flag = int(_safe_float(properties.get("quality_flag"), 0))

    anomaly_magnitude = abs(anomaly or 0.0)
    if quality_flag > 0 or anomaly_magnitude >= 10:
        risk = "high"
    elif anomaly_magnitude >= 4:
        risk = "moderate"
    else:
        risk = "low"

    return {
        "source": "noaa-glotec",
        "status": "available",
        "observed_at": latest_entry.get("time_tag"),
        "tec": _round_float(tec, 2),
        "anomaly": _round_float(anomaly, 2),
        "hmf2_km": _round_float(_optional_float(properties.get("hmF2")), 1),
        "nmf2_per_m3": _round_float(_optional_float(properties.get("NmF2")), 0),
        "quality_flag": quality_flag,
        "risk": risk,
        "distance_km": _round_float(nearest_distance, 0),
        "detail": (
            "GloTEC is quiet near this property right now."
            if anomaly_magnitude < 2 and quality_flag == 0
            else (
                f"GloTEC shows a local ionospheric anomaly of about {anomaly_magnitude:.1f} TEC units "
                f"near this property."
            )
        ),
        "freshness": _aggregate_freshness(
            {
                "noaa-glotec-index": index_response.get("freshness") or {},
                "noaa-glotec-grid": grid_response.get("freshness") or {},
            }
        ),
    }


def _build_aurora_context(
    aurora_payload: Any,
    latitude: float,
    longitude: float,
    is_daylight: bool,
):
    default_context = {
        "source": "noaa-ovation-aurora",
        "forecast_status": "unavailable",
        "basis": "ovation-footprint-inference",
        "model_note": (
            "Local aurora reach is inferred from NOAA's OVATION aurora footprint because the official "
            "Aurora Viewline product is image-based rather than structured JSON."
        ),
        "observation_time": None,
        "forecast_time": None,
        "local_aurora_value": None,
        "viewline_threshold": AURORA_VIEWLINE_THRESHOLD,
        "longitude_window_degrees": AURORA_VIEWLINE_LONGITUDE_WINDOW_DEGREES,
        "southern_edge_latitude": None,
        "northern_edge_latitude": None,
        "distance_to_viewline_km": None,
        "reach": "unavailable",
        "reach_label": "unavailable",
        "reach_tone": "neutral",
        "visibility": "low",
        "detail": "Aurora footprint data is unavailable, so local aurora reach falls back to coarse geomagnetic heuristics.",
    }
    if not isinstance(aurora_payload, dict):
        return default_context

    coordinates = aurora_payload.get("coordinates") or []
    if not isinstance(coordinates, list) or not coordinates:
        return default_context

    local_longitude = _normalize_longitude(longitude)
    northern_hemisphere = latitude >= 0
    local_aurora_value = None
    best_local_score = None
    edge_latitude = None
    nearest_positive_distance_km = None
    nearest_positive_value = None

    for row in coordinates:
        if not isinstance(row, list) or len(row) < 3:
            continue
        grid_longitude_360 = _safe_float(row[0])
        grid_latitude = _safe_float(row[1])
        aurora_value = _safe_float(row[2])
        grid_longitude = _normalize_longitude(grid_longitude_360)
        longitude_gap = _longitude_distance_degrees(grid_longitude, local_longitude)
        local_score = abs(grid_latitude - latitude) + longitude_gap

        if best_local_score is None or local_score < best_local_score:
            best_local_score = local_score
            local_aurora_value = aurora_value

        if aurora_value < AURORA_VIEWLINE_THRESHOLD:
            continue

        if northern_hemisphere and grid_latitude < 0:
            continue
        if not northern_hemisphere and grid_latitude > 0:
            continue

        if longitude_gap <= AURORA_VIEWLINE_LONGITUDE_WINDOW_DEGREES:
            if edge_latitude is None:
                edge_latitude = grid_latitude
            elif northern_hemisphere and grid_latitude < edge_latitude:
                edge_latitude = grid_latitude
            elif not northern_hemisphere and grid_latitude > edge_latitude:
                edge_latitude = grid_latitude

        distance_km = _haversine_km(latitude, longitude, grid_latitude, grid_longitude)
        if nearest_positive_distance_km is None or distance_km < nearest_positive_distance_km:
            nearest_positive_distance_km = distance_km
            nearest_positive_value = aurora_value

    local_aurora_value = round(local_aurora_value or 0.0, 1)
    distance_to_viewline_km = round(nearest_positive_distance_km, 0) if nearest_positive_distance_km is not None else None
    southern_edge_latitude = round(edge_latitude, 1) if northern_hemisphere and edge_latitude is not None else None
    northern_edge_latitude = round(edge_latitude, 1) if not northern_hemisphere and edge_latitude is not None else None

    if edge_latitude is None:
        reach = "far"
        reach_tone = "low"
    else:
        latitude_gap = max(0.0, edge_latitude - latitude) if northern_hemisphere else max(0.0, latitude - edge_latitude)
        if local_aurora_value >= 10:
            reach = "overhead"
            reach_tone = "watch"
        elif latitude_gap <= 0.0:
            reach = "within-viewline"
            reach_tone = "watch"
        elif (nearest_positive_distance_km or float("inf")) <= AURORA_NEARBY_DISTANCE_KM:
            reach = "nearby-viewline"
            reach_tone = "watch"
        elif (nearest_positive_distance_km or float("inf")) <= AURORA_DISTANT_DISTANCE_KM:
            reach = "distant-viewline"
            reach_tone = "low"
        else:
            reach = "far"
            reach_tone = "low"

    if is_daylight:
        visibility = "low"
    elif reach in {"overhead", "within-viewline"}:
        visibility = "likely"
    elif reach == "nearby-viewline":
        visibility = "possible"
    else:
        visibility = "low"

    if edge_latitude is None:
        detail = "Current NOAA aurora footprint stays well poleward of this property's longitude band."
    elif reach == "overhead":
        detail = (
            f"NOAA's aurora footprint is active directly over this latitude band right now, with a local aurora value near {local_aurora_value:.0f}."
        )
    elif reach == "within-viewline":
        detail = (
            f"The inferred aurora viewline reaches about {edge_latitude:.0f}° latitude in this longitude band, putting this property inside the current footprint."
        )
    elif reach == "nearby-viewline":
        detail = (
            f"The inferred aurora viewline stays nearby, with the closest lit footprint about {distance_to_viewline_km:.0f} km away."
        )
    elif reach == "distant-viewline":
        detail = (
            f"The nearest lit aurora footprint is still about {distance_to_viewline_km:.0f} km away, so visibility would need a stronger expansion."
        )
    else:
        detail = "The current aurora footprint remains too far poleward to matter locally right now."

    return {
        "source": "noaa-ovation-aurora",
        "forecast_status": "available",
        "basis": "ovation-footprint-inference",
        "model_note": (
            "Local aurora reach is inferred from NOAA's OVATION aurora footprint because the official "
            "Aurora Viewline product is image-based rather than structured JSON."
        ),
        "observation_time": aurora_payload.get("Observation Time"),
        "forecast_time": aurora_payload.get("Forecast Time"),
        "local_aurora_value": local_aurora_value,
        "nearest_positive_value": round(nearest_positive_value or 0.0, 1),
        "viewline_threshold": AURORA_VIEWLINE_THRESHOLD,
        "longitude_window_degrees": AURORA_VIEWLINE_LONGITUDE_WINDOW_DEGREES,
        "southern_edge_latitude": southern_edge_latitude,
        "northern_edge_latitude": northern_edge_latitude,
        "distance_to_viewline_km": distance_to_viewline_km,
        "reach": reach,
        "reach_label": str(reach).replace("-", " "),
        "reach_tone": reach_tone,
        "visibility": visibility,
        "detail": detail,
    }


def _aurora_potential(
    geomagnetic_scale: int,
    latitude: float,
    is_daylight: bool,
    aurora_context: Optional[dict[str, Any]] = None,
) -> str:
    if aurora_context and aurora_context.get("forecast_status") == "available":
        return str(aurora_context.get("visibility") or "low")

    if is_daylight or geomagnetic_scale <= 0:
        return "low"

    absolute_latitude = abs(latitude)
    if geomagnetic_scale >= 3 and absolute_latitude >= 45:
        return "likely"
    if geomagnetic_scale >= 2 and absolute_latitude >= 50:
        return "possible"
    if geomagnetic_scale >= 1 and absolute_latitude >= 55:
        return "possible"
    return "low"


def _hf_radio_risk(
    radio_blackout_scale: int,
    is_daylight: bool,
    drap_context: Optional[dict[str, Any]] = None,
) -> str:
    drap_risk = str((drap_context or {}).get("risk") or "low")
    if drap_risk == "high":
        return "high"
    if drap_risk == "moderate":
        return "moderate"
    if not is_daylight or radio_blackout_scale <= 0:
        return "low"
    if radio_blackout_scale >= 3:
        return "high"
    if radio_blackout_scale >= 1:
        return "moderate"
    return "low"


def _gnss_risk(
    geomagnetic_scale: int,
    radiation_scale: int,
    latitude: float,
    glotec_context: Optional[dict[str, Any]] = None,
) -> str:
    glotec_risk = str((glotec_context or {}).get("risk") or "low")
    if glotec_risk == "high":
        return "high"
    if glotec_risk == "moderate":
        return "moderate"
    absolute_latitude = abs(latitude)
    if geomagnetic_scale >= 3:
        return "high"
    if geomagnetic_scale >= 2 and absolute_latitude >= 40:
        return "moderate"
    if radiation_scale >= 2 and absolute_latitude >= 50:
        return "moderate"
    return "low"


def _space_weather_alert_level(
    radio_blackout_scale: int,
    radiation_scale: int,
    geomagnetic_scale: int,
    is_daylight: bool,
    latitude: float,
    warning_count: int,
    watch_count: int,
    aurora_context: Optional[dict[str, Any]] = None,
    drap_context: Optional[dict[str, Any]] = None,
    glotec_context: Optional[dict[str, Any]] = None,
) -> str:
    absolute_latitude = abs(latitude)
    aurora_reach = str((aurora_context or {}).get("reach") or "")
    drap_risk = str((drap_context or {}).get("risk") or "low")
    glotec_risk = str((glotec_context or {}).get("risk") or "low")

    if radio_blackout_scale >= 2 and is_daylight:
        return "alert"
    if drap_risk == "high" and is_daylight:
        return "alert"
    if not is_daylight and geomagnetic_scale >= 3 and aurora_reach in {"overhead", "within-viewline"}:
        return "alert"
    if geomagnetic_scale >= 3 and absolute_latitude >= 45:
        return "alert"
    if glotec_risk == "high":
        return "alert"
    if radiation_scale >= 2 and absolute_latitude >= 55:
        return "alert"
    if warning_count > 0:
        return "watch"
    if drap_risk == "moderate" and is_daylight:
        return "watch"
    if glotec_risk == "moderate":
        return "watch"
    if not is_daylight and geomagnetic_scale >= 2 and aurora_reach in {"overhead", "within-viewline", "nearby-viewline"}:
        return "watch"
    if radio_blackout_scale >= 1 or radiation_scale >= 1 or geomagnetic_scale >= 2 or watch_count > 0:
        return "watch"
    return "low"


def _build_space_weather_summary(
    alert_level: str,
    radio_blackout_scale: int,
    radiation_scale: int,
    geomagnetic_scale: int,
    is_daylight: bool,
    aurora_potential: str,
    gnss_risk: str,
    aurora_context: Optional[dict[str, Any]] = None,
    drap_context: Optional[dict[str, Any]] = None,
    glotec_context: Optional[dict[str, Any]] = None,
) -> str:
    aurora_reach = str((aurora_context or {}).get("reach") or "")
    drap_risk = str((drap_context or {}).get("risk") or "low")
    glotec_risk = str((glotec_context or {}).get("risk") or "low")
    if alert_level == "alert" and radio_blackout_scale >= 2 and is_daylight:
        return (
            "A flare-driven radio blackout signal is active for the sunlit side of Earth. "
            "Communication effects matter more here than any residential ground-level radiation concern."
        )

    if alert_level == "alert" and drap_risk == "high" and is_daylight:
        return (
            "HF absorption is elevated for this daylight-side location right now. "
            "Shortwave communication effects are more locally relevant than any residential radiation concern."
        )

    if alert_level == "alert" and aurora_reach in {"overhead", "within-viewline"} and not is_daylight:
        return (
            "Geomagnetic conditions are elevated enough that NOAA's aurora footprint reaches this property's "
            "latitude band tonight. Aurora visibility and GNSS instability are more locally relevant than usual."
        )

    if alert_level == "alert" and glotec_risk == "high":
        return (
            "Ionospheric conditions are elevated enough to matter locally. "
            "GNSS timing and positioning may be less stable than usual at this property."
        )

    if alert_level == "alert" and geomagnetic_scale >= 3:
        return (
            "Geomagnetic conditions are elevated enough to matter locally. Expect stronger aurora "
            "potential and a higher chance of GNSS instability."
        )

    if alert_level == "watch" and aurora_reach == "nearby-viewline" and not is_daylight:
        return (
            "The NOAA aurora footprint is nearby for this longitude band tonight. Visibility is not guaranteed here, "
            "but the property is close enough that a stronger expansion could matter."
        )

    if alert_level == "watch" and geomagnetic_scale >= 1:
        summary = "Geomagnetic conditions are elevated but not extreme for this property."
        if aurora_potential in {"possible", "likely"}:
            summary += " Aurora chances improve after dark."
        if gnss_risk != "low":
            summary += " Minor GNSS instability is possible."
        return summary

    if alert_level == "watch" and drap_risk == "moderate" and is_daylight:
        return (
            "D-RAP shows some daylight-side HF absorption near this property. "
            "Radio effects are more relevant than any direct residential ground-level concern."
        )

    if alert_level == "watch" and glotec_risk == "moderate":
        return (
            "Ionospheric conditions are mildly elevated near this property. "
            "Minor GNSS or timing instability is possible."
        )

    if radiation_scale >= 1:
        return (
            "Solar activity is elevated, but the strongest radiation-storm effects remain more "
            "relevant for high-latitude or high-altitude operations than for a homeowner on the ground."
        )

    return "Space-weather conditions are quiet to minor for this location right now."


def _build_space_weather_reasons(
    radio_blackout_scale: int,
    radiation_scale: int,
    geomagnetic_scale: int,
    is_daylight: bool,
    latitude_band: str,
    aurora_potential: str,
    aurora_context: Optional[dict[str, Any]],
    drap_context: Optional[dict[str, Any]],
    glotec_context: Optional[dict[str, Any]],
    hf_radio_risk: str,
    gnss_risk: str,
    ground_note: str,
):
    daylight_detail = (
        "The property is on the daylight side of Earth, so flare-driven HF radio blackout effects are more locally relevant right now."
        if is_daylight
        else "The property is on the night side of Earth, so flare-driven HF radio blackout effects are muted locally until daylight returns."
    )
    latitude_detail_by_band = {
        "high": "High-latitude locations are the first places where geomagnetic activity tends to matter for aurora reach and GNSS stability.",
        "mid": "Mid-latitude locations need stronger geomagnetic conditions before aurora reach or navigation effects become obvious.",
        "low": "Low-latitude locations are less exposed to geomagnetic spillover unless storms become unusually strong.",
        "equatorial": "Equatorial locations rarely see direct aurora relevance, so most global alerts stay low-impact here.",
    }
    aurora_detail = (
        aurora_context.get("detail")
        if aurora_context and aurora_context.get("detail")
        else (
            "Aurora visibility is plausible here after dark if geomagnetic conditions hold."
            if aurora_potential in {"possible", "likely"}
            else "Aurora visibility is not a strong local signal here right now."
        )
    )
    gnss_detail = (
        (
            f"{glotec_context.get('detail')} Navigation and timing systems are the main local risk path from current ionospheric conditions."
            if glotec_context and glotec_context.get("status") == "available" and glotec_context.get("risk") in {"moderate", "high"}
            else "Navigation and timing systems are the main local risk path from current geomagnetic conditions."
        )
        if gnss_risk in {"moderate", "high"}
        else "Current geomagnetic conditions are not signaling meaningful local GNSS disruption."
    )

    return [
        {
            "id": "daylight-side",
            "label": "Daylight side" if is_daylight else "Night side",
            "tone": hf_radio_risk if is_daylight else "neutral",
            "detail": daylight_detail,
        },
        {
            "id": "latitude-band",
            "label": f"{latitude_band.title()} latitude band",
            "tone": "watch" if latitude_band in {"high", "mid"} and geomagnetic_scale >= 2 else "neutral",
            "detail": latitude_detail_by_band.get(latitude_band, "Latitude context is unavailable."),
        },
        {
            "id": "aurora-reach",
            "label": (
                f"Aurora {aurora_context.get('reach_label')}"
                if aurora_context and aurora_context.get("forecast_status") == "available"
                else f"Aurora {aurora_potential}"
            ),
            "tone": (
                aurora_context.get("reach_tone")
                if aurora_context and aurora_context.get("forecast_status") == "available"
                else aurora_potential
            ),
            "detail": aurora_detail,
        },
        {
            "id": "hf-radio",
            "label": f"HF radio {hf_radio_risk}",
            "tone": hf_radio_risk,
            "detail": (
                (
                    drap_context.get("detail")
                    if drap_context and drap_context.get("status") == "available" and drap_context.get("absorption_frequency_mhz") not in (None, 0)
                    else "HF radio disruption risk is elevated locally because radio-blackout conditions are active on the daylight side."
                )
                if hf_radio_risk in {"moderate", "high"}
                else "HF radio disruption risk is currently low at this location."
            ),
        },
        {
            "id": "gnss",
            "label": f"GNSS {gnss_risk}",
            "tone": gnss_risk,
            "detail": gnss_detail,
        },
        {
            "id": "drap",
            "label": (
                f"D-RAP {drap_context.get('risk')}"
                if drap_context and drap_context.get("status") == "available"
                else "D-RAP unavailable"
            ),
            "tone": (
                drap_context.get("risk")
                if drap_context and drap_context.get("status") == "available"
                else "neutral"
            ),
            "detail": (
                drap_context.get("detail")
                if drap_context and drap_context.get("detail")
                else "D-RAP detail is unavailable in this response."
            ),
        },
        {
            "id": "glotec",
            "label": (
                f"GloTEC {glotec_context.get('risk')}"
                if glotec_context and glotec_context.get("status") == "available"
                else "GloTEC unavailable"
            ),
            "tone": (
                glotec_context.get("risk")
                if glotec_context and glotec_context.get("status") == "available"
                else "neutral"
            ),
            "detail": (
                glotec_context.get("detail")
                if glotec_context and glotec_context.get("detail")
                else "GloTEC detail is unavailable in this response."
            ),
        },
        {
            "id": "ground-radiation",
            "label": "Ground-level radiation",
            "tone": "low" if radiation_scale < 3 else "watch",
            "detail": ground_note,
        },
    ]


def _find_hour_index(times: list[str], current_time: Optional[str]) -> int:
    if not times:
        return 0

    if current_time:
        current_hour = current_time[:13] + ":00"
        for index, value in enumerate(times):
            if value == current_hour:
                return index

    return max(0, len(times) - 1)


def _intensity_level(ghi_value: float, is_daylight: bool) -> str:
    if not is_daylight or ghi_value < 50:
        return "low"
    if ghi_value >= 700:
        return "high"
    if ghi_value >= 350:
        return "moderate"
    return "low"


def _spike_level(max_hourly_ramp: float, peak_ghi: float, is_daylight: bool) -> str:
    if not is_daylight and peak_ghi < 250:
        return "low"
    if max_hourly_ramp >= 350 or peak_ghi >= 900:
        return "alert"
    if max_hourly_ramp >= 200 or peak_ghi >= 700:
        return "watch"
    return "low"


def _build_surface_summary(
    is_daylight: bool,
    current_ghi: float,
    peak_time: Optional[str],
    peak_ghi: float,
    spike_level: str,
    next_hour_change: Optional[float],
) -> str:
    if not is_daylight:
        if peak_time and peak_ghi > 0:
            return (
                f"It is currently dark at this property. The next daylight peak in the forecast is "
                f"around {peak_time.split('T')[-1][:5]} local time at about {round(peak_ghi)} W/m²."
            )
        return "It is currently dark at this property, so surface irradiance is minimal."

    if spike_level == "alert":
        return (
            f"Surface sunlight is very strong with sharp hour-to-hour movement. Current GHI is about "
            f"{round(current_ghi)} W/m² and the forecast peak reaches about {round(peak_ghi)} W/m²."
        )

    if spike_level == "watch":
        direction = "up" if (next_hour_change or 0) >= 0 else "down"
        magnitude = abs(round(next_hour_change or 0))
        return (
            f"Surface irradiance is active enough to watch. Current GHI is about {round(current_ghi)} W/m², "
            f"with the next hour shifting about {magnitude} W/m² {direction}."
        )

    return (
        f"Surface sunlight is stable right now at about {round(current_ghi)} W/m², with the next peak "
        f"near {peak_time.split('T')[-1][:5] if peak_time else 'the upcoming daylight window'} local time."
    )


def _build_surface_reasons(
    is_daylight: bool,
    intensity_level: str,
    spike_level: str,
    current_ghi: float,
    next_peak_time: Optional[str],
    next_peak_ghi: float,
    change_to_next_hour: Optional[float],
    max_hourly_ramp: float,
):
    next_hour_detail = (
        "The next-hour ramp is not available yet."
        if change_to_next_hour is None
        else (
            f"Forecast sunlight rises by about {abs(change_to_next_hour):.0f} W/m² over the next hour."
            if change_to_next_hour >= 0
            else f"Forecast sunlight eases by about {abs(change_to_next_hour):.0f} W/m² over the next hour."
        )
    )

    return [
        {
            "id": "daylight",
            "label": "Daylight" if is_daylight else "Night",
            "tone": "watch" if is_daylight else "neutral",
            "detail": (
                "The property is in daylight, so surface irradiance can change quickly with the next forecast hours."
                if is_daylight
                else "It is currently dark at the property, so surface irradiance is naturally suppressed until after sunrise."
            ),
        },
        {
            "id": "current-intensity",
            "label": f"Current intensity {intensity_level}",
            "tone": intensity_level,
            "detail": f"Current global horizontal irradiance is about {current_ghi:.0f} W/m².",
        },
        {
            "id": "next-hour-ramp",
            "label": f"Next-hour trend {spike_level}",
            "tone": spike_level,
            "detail": next_hour_detail,
        },
        {
            "id": "peak-window",
            "label": "Next peak window",
            "tone": spike_level,
            "detail": (
                f"The next forecast peak is around {next_peak_time or 'the next daylight window'} near {next_peak_ghi:.0f} W/m², "
                f"with a largest hourly ramp of about {max_hourly_ramp:.0f} W/m²."
            ),
        },
    ]


def _build_recent_headlines(alerts_payload: Any) -> tuple[int, int, int, list[str]]:
    warning_count = 0
    watch_count = 0
    alert_count = 0
    headlines: list[str] = []

    if not isinstance(alerts_payload, list):
        return warning_count, watch_count, alert_count, headlines

    for item in alerts_payload:
        headline = _extract_alert_headline(str(item.get("message") or ""))
        if not headline:
            continue
        level = _categorize_alert_level(headline)
        if level == "warning":
            warning_count += 1
        elif level == "watch":
            watch_count += 1
        elif level == "alert":
            alert_count += 1
        if len(headlines) < 3:
            headlines.append(headline)

    return warning_count, watch_count, alert_count, headlines


def _historical_climate_window():
    current_date = datetime.now(timezone.utc).date()
    end_year = current_date.year - 1
    start_year = end_year - 9
    return f"{start_year}-01-01", f"{end_year}-12-31", start_year, end_year


def _month_day_label(value: Optional[str]):
    if not value:
        return None
    try:
        return datetime.strptime(value, "%m-%d").strftime("%b %d")
    except ValueError:
        return value


def _month_day_to_day_of_year(value: Optional[str]):
    if not value:
        return None
    try:
        parsed = datetime.strptime(f"2001-{value}", "%Y-%m-%d").date()
    except ValueError:
        return None
    return parsed.timetuple().tm_yday


def _day_of_year_to_month_day(day_of_year: Optional[int]):
    if day_of_year is None:
        return None
    base = date(2001, 1, 1) + timedelta(days=max(int(day_of_year) - 1, 0))
    return base.strftime("%m-%d")


def _resolve_frost_boundary(
    daily_times: list[str],
    daily_temperature_min_f: list[Optional[float]],
    *,
    threshold_f: float,
    start_month: int,
    end_month: int,
    pick: str,
):
    yearly_hits: dict[str, list[date]] = {}

    for time_value, min_temperature_f in zip(daily_times, daily_temperature_min_f):
        if min_temperature_f is None or min_temperature_f > threshold_f:
            continue

        try:
            observed_date = datetime.strptime(str(time_value)[:10], "%Y-%m-%d").date()
        except ValueError:
            continue

        if observed_date.month < start_month or observed_date.month > end_month:
            continue

        yearly_hits.setdefault(str(observed_date.year), []).append(observed_date)

    selected_dates = []
    for values in yearly_hits.values():
        if not values:
            continue
        selected_dates.append(max(values) if pick == "last" else min(values))

    if not selected_dates:
        return {
            "threshold_f": threshold_f,
            "median_month_day": None,
            "median_label": None,
            "earliest_month_day": None,
            "earliest_label": None,
            "latest_month_day": None,
            "latest_label": None,
            "sample_years": 0,
        }

    day_values = sorted(
        value.timetuple().tm_yday
        for value in (
            date(2001, observed_date.month, observed_date.day)
            for observed_date in selected_dates
        )
    )
    median_day = int(round(median(day_values)))
    median_month_day = _day_of_year_to_month_day(median_day)
    earliest = min(selected_dates)
    latest = max(selected_dates)

    return {
        "threshold_f": threshold_f,
        "median_month_day": median_month_day,
        "median_label": _month_day_label(median_month_day),
        "median_day_of_year": median_day,
        "earliest_month_day": earliest.strftime("%m-%d"),
        "earliest_label": earliest.strftime("%b %d"),
        "latest_month_day": latest.strftime("%m-%d"),
        "latest_label": latest.strftime("%b %d"),
        "sample_years": len(selected_dates),
    }


def _build_frost_window(daily_times: list[str], daily_temperature_min_f: list[Optional[float]]):
    spring_frost = _resolve_frost_boundary(
        daily_times,
        daily_temperature_min_f,
        threshold_f=32.0,
        start_month=1,
        end_month=7,
        pick="last",
    )
    fall_frost = _resolve_frost_boundary(
        daily_times,
        daily_temperature_min_f,
        threshold_f=32.0,
        start_month=8,
        end_month=12,
        pick="first",
    )

    spring_day = spring_frost.get("median_day_of_year")
    fall_day = fall_frost.get("median_day_of_year")
    frost_free_days = None
    if spring_day is not None and fall_day is not None and fall_day > spring_day:
        frost_free_days = fall_day - spring_day - 1

    confidence = "moderate"
    sample_years = min(
        spring_frost.get("sample_years") or 0,
        fall_frost.get("sample_years") or 0,
    )
    if sample_years < 6:
        confidence = "early"
    if not sample_years:
        confidence = "low"

    return {
        "model_version": "frost-window-v1",
        "threshold_f": 32.0,
        "last_spring_frost": spring_frost,
        "first_fall_frost": fall_frost,
        "median_frost_free_days": frost_free_days,
        "confidence": confidence,
        "sample_years": sample_years,
        "summary": (
            f"Typical last spring frost near {spring_frost.get('median_label') or 'unknown'} and first fall frost near "
            f"{fall_frost.get('median_label') or 'unknown'}."
            if sample_years
            else "Historical frost boundaries could not be estimated from the current climate sample."
        ),
    }


def _build_hardiness_band(avg_annual_extreme_min_f: Optional[float]) -> dict[str, Any]:
    if avg_annual_extreme_min_f is None:
        return {
            "label": None,
            "average_annual_extreme_min_f": None,
            "range_f": None,
            "estimated": True,
        }

    band_index = int((avg_annual_extreme_min_f + 60) // 5)
    clamped_index = max(0, min(25, band_index))
    zone_number = (clamped_index // 2) + 1
    subzone = "a" if clamped_index % 2 == 0 else "b"
    lower_bound = -60 + (clamped_index * 5)
    upper_bound = lower_bound + 5

    if clamped_index == 0 and avg_annual_extreme_min_f < -60:
        range_f = "Below -60°F"
    elif clamped_index == 25 and avg_annual_extreme_min_f >= 65:
        range_f = "65°F and warmer"
    else:
        range_f = f"{lower_bound:.0f} to {upper_bound:.0f}°F"

    return {
        "label": f"{zone_number}{subzone}",
        "average_annual_extreme_min_f": _round_float(avg_annual_extreme_min_f, 1),
        "range_f": range_f,
        "estimated": True,
    }


def _build_climate_extreme(
    monthly_profiles: dict[str, dict[str, Any]],
    profile_key: str,
    value_key: str,
    maximize: bool = True,
):
    candidates = []
    for month_key, profile in monthly_profiles.items():
        value = profile.get(profile_key)
        if value is None:
            continue
        candidates.append((month_key, value))

    if not candidates:
        return None

    month_key, value = (
        max(candidates, key=lambda candidate: candidate[1])
        if maximize
        else min(candidates, key=lambda candidate: candidate[1])
    )
    return {
        "month": month_key,
        value_key: value,
    }


def _build_open_meteo_payload(
    latitude: float,
    longitude: float,
    time_zone_name: str,
    *,
    force_refresh: bool = False,
    return_metadata: bool = False,
):
    return _fetch_json(
        OPEN_METEO_FORECAST_URL,
        params={
            "latitude": round(latitude, 6),
            "longitude": round(longitude, 6),
            "timezone": time_zone_name or "UTC",
            "past_hours": 2,
            "forecast_hours": 24,
            "current": "is_day,shortwave_radiation,direct_normal_irradiance",
            "hourly": "shortwave_radiation,direct_normal_irradiance",
        },
        ttl_seconds=600,
        force_refresh=force_refresh,
        return_metadata=return_metadata,
        source_name="open-meteo-forecast",
    )


def _build_open_meteo_historical_payload(latitude: float, longitude: float, time_zone_name: str):
    start_date, end_date, _, _ = _historical_climate_window()
    return _fetch_json(
        OPEN_METEO_ARCHIVE_URL,
        params={
            "latitude": round(latitude, 6),
            "longitude": round(longitude, 6),
            "timezone": time_zone_name or "UTC",
            "start_date": start_date,
            "end_date": end_date,
            "daily": (
                "temperature_2m_mean,"
                "temperature_2m_min,"
                "relative_humidity_2m_mean,"
                "shortwave_radiation_sum"
            ),
        },
        ttl_seconds=86400,
    )


def get_surface_irradiance_snapshot(
    latitude: float,
    longitude: float,
    time_zone_name: str,
    force_refresh: bool = False,
):
    forecast_response = _build_open_meteo_payload(
        latitude,
        longitude,
        time_zone_name,
        force_refresh=force_refresh,
        return_metadata=True,
    )
    forecast_payload = forecast_response["data"]
    current = forecast_payload.get("current") or {}
    hourly = forecast_payload.get("hourly") or {}
    hourly_times = list(hourly.get("time") or [])
    hourly_ghi = [_safe_float(value) for value in (hourly.get("shortwave_radiation") or [])]
    hourly_dni = [_safe_float(value) for value in (hourly.get("direct_normal_irradiance") or [])]
    current_time = current.get("time")
    current_index = _find_hour_index(hourly_times, current_time)
    is_daylight = bool(current.get("is_day"))
    current_ghi = round(_safe_float(current.get("shortwave_radiation")), 1)
    current_dni = round(_safe_float(current.get("direct_normal_irradiance")), 1)
    previous_hour_ghi = round(hourly_ghi[current_index - 1], 1) if current_index > 0 else None
    next_hour_ghi = round(hourly_ghi[current_index + 1], 1) if current_index + 1 < len(hourly_ghi) else None
    change_from_previous_hour = (
        round(current_ghi - previous_hour_ghi, 1) if previous_hour_ghi is not None else None
    )
    change_to_next_hour = round(next_hour_ghi - current_ghi, 1) if next_hour_ghi is not None else None

    future_times = hourly_times[current_index:] or hourly_times
    future_ghi = hourly_ghi[current_index:] or hourly_ghi
    future_dni = hourly_dni[current_index:] or hourly_dni

    peak_index = 0
    if future_ghi:
        peak_index = max(range(len(future_ghi)), key=lambda index: future_ghi[index])
    peak_time = future_times[peak_index] if future_times else current_time
    peak_ghi = round(future_ghi[peak_index], 1) if future_ghi else current_ghi
    peak_dni = round(future_dni[peak_index], 1) if future_dni else current_dni

    max_hourly_ramp = 0.0
    ramp_start_time = None
    ramp_end_time = None
    for index in range(max(current_index - 1, 0), max(len(hourly_ghi) - 1, 0)):
        left_value = hourly_ghi[index]
        right_value = hourly_ghi[index + 1]
        ramp_value = abs(right_value - left_value)
        if ramp_value > max_hourly_ramp:
            max_hourly_ramp = ramp_value
            ramp_start_time = hourly_times[index]
            ramp_end_time = hourly_times[index + 1]

    intensity_level = _intensity_level(current_ghi, is_daylight)
    spike_level = _spike_level(max_hourly_ramp, peak_ghi, is_daylight)
    hourly_profile = []
    for index, time_value in enumerate(future_times[:8]):
        hourly_profile.append(
            {
                "time": time_value,
                "ghi_w_m2": round(future_ghi[index], 1),
                "dni_w_m2": round(future_dni[index], 1),
            }
        )

    freshness = _aggregate_freshness(
        {
            "open-meteo-forecast": forecast_response.get("freshness") or {},
        }
    )

    return {
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "time_zone": forecast_payload.get("timezone") or time_zone_name or "UTC",
        "observed_at": current_time,
        "source": "open-meteo-forecast",
        "is_daylight": is_daylight,
        "current": {
            "ghi_w_m2": current_ghi,
            "dni_w_m2": current_dni,
            "intensity_level": intensity_level,
        },
        "trend": {
            "previous_hour_ghi_w_m2": previous_hour_ghi,
            "change_from_previous_hour_w_m2": change_from_previous_hour,
            "next_hour_ghi_w_m2": next_hour_ghi,
            "change_to_next_hour_w_m2": change_to_next_hour,
            "max_hourly_ramp_w_m2": round(max_hourly_ramp, 1),
            "ramp_start_time": ramp_start_time,
            "ramp_end_time": ramp_end_time,
        },
        "next_peak": {
            "time": peak_time,
            "ghi_w_m2": peak_ghi,
            "dni_w_m2": peak_dni,
        },
        "spike_level": spike_level,
        "summary": _build_surface_summary(
            is_daylight,
            current_ghi,
            peak_time,
            peak_ghi,
            spike_level,
            change_to_next_hour,
        ),
        "freshness": freshness,
        "reasons": _build_surface_reasons(
            is_daylight,
            intensity_level,
            spike_level,
            current_ghi,
            peak_time,
            peak_ghi,
            change_to_next_hour,
            round(max_hourly_ramp, 1),
        ),
        "hourly_profile": hourly_profile,
    }


def get_property_climate_snapshot(latitude: float, longitude: float, time_zone_name: str):
    historical_payload = _build_open_meteo_historical_payload(latitude, longitude, time_zone_name)
    daily = historical_payload.get("daily") or {}
    daily_times = list(daily.get("time") or [])
    daily_temperature_mean = list(daily.get("temperature_2m_mean") or [])
    daily_temperature_min = list(daily.get("temperature_2m_min") or [])
    daily_relative_humidity = list(daily.get("relative_humidity_2m_mean") or [])
    daily_shortwave_radiation = list(daily.get("shortwave_radiation_sum") or [])

    growing_season_months = {"04", "05", "06", "07", "08", "09"}
    monthly_buckets = {
        f"{month:02d}": {
            "temperature_c": [],
            "relative_humidity": [],
            "shortwave_radiation_mj_m2": [],
        }
        for month in range(1, 13)
    }
    annual_temperature_values: list[Optional[float]] = []
    annual_humidity_values: list[Optional[float]] = []
    annual_radiation_values: list[Optional[float]] = []
    growing_temperature_values: list[Optional[float]] = []
    growing_humidity_values: list[Optional[float]] = []
    growing_radiation_values: list[Optional[float]] = []
    yearly_extreme_mins_f: dict[str, list[float]] = {}

    for time_value, temperature_mean, temperature_min, relative_humidity, shortwave_radiation in zip(
        daily_times,
        daily_temperature_mean,
        daily_temperature_min,
        daily_relative_humidity,
        daily_shortwave_radiation,
    ):
        month_key = str(time_value)[5:7]
        year_key = str(time_value)[:4]
        mean_temperature_c = _optional_float(temperature_mean)
        min_temperature_c = _optional_float(temperature_min)
        mean_relative_humidity = _optional_float(relative_humidity)
        total_shortwave_radiation_mj = _optional_float(shortwave_radiation)

        if month_key not in monthly_buckets:
            continue

        monthly_buckets[month_key]["temperature_c"].append(mean_temperature_c)
        monthly_buckets[month_key]["relative_humidity"].append(mean_relative_humidity)
        monthly_buckets[month_key]["shortwave_radiation_mj_m2"].append(total_shortwave_radiation_mj)

        annual_temperature_values.append(mean_temperature_c)
        annual_humidity_values.append(mean_relative_humidity)
        annual_radiation_values.append(total_shortwave_radiation_mj)

        if month_key in growing_season_months:
            growing_temperature_values.append(mean_temperature_c)
            growing_humidity_values.append(mean_relative_humidity)
            growing_radiation_values.append(total_shortwave_radiation_mj)

        min_temperature_f = _celsius_to_fahrenheit(min_temperature_c)
        if min_temperature_f is not None:
            yearly_extreme_mins_f.setdefault(year_key, []).append(min_temperature_f)

    monthly_profiles = {}
    for month_key, bucket in monthly_buckets.items():
        avg_temperature_c = _average(bucket["temperature_c"])
        avg_relative_humidity = _average(bucket["relative_humidity"])
        avg_shortwave_radiation_mj = _average(bucket["shortwave_radiation_mj_m2"])
        avg_temperature_f = _celsius_to_fahrenheit(avg_temperature_c)
        avg_shortwave_radiation_kwh = _megajoules_to_kwh(avg_shortwave_radiation_mj)

        monthly_profiles[month_key] = {
            "average_temperature_c": _round_float(avg_temperature_c, 1),
            "average_temperature_f": _round_float(avg_temperature_f, 1),
            "average_relative_humidity": _round_float(avg_relative_humidity, 1),
            "average_daily_shortwave_radiation_mj_m2": _round_float(avg_shortwave_radiation_mj, 2),
            "average_daily_shortwave_radiation_kwh_m2": _round_float(avg_shortwave_radiation_kwh, 2),
        }

    avg_annual_extreme_min_f = _average(
        [min(values) for values in yearly_extreme_mins_f.values() if values]
    )
    annual_avg_temperature_c = _average(annual_temperature_values)
    annual_avg_temperature_f = _celsius_to_fahrenheit(annual_avg_temperature_c)
    annual_avg_humidity = _average(annual_humidity_values)
    annual_avg_radiation_mj = _average(annual_radiation_values)
    annual_avg_radiation_kwh = _megajoules_to_kwh(annual_avg_radiation_mj)
    growing_avg_temperature_c = _average(growing_temperature_values)
    growing_avg_temperature_f = _celsius_to_fahrenheit(growing_avg_temperature_c)
    growing_avg_humidity = _average(growing_humidity_values)
    growing_avg_radiation_mj = _average(growing_radiation_values)
    growing_avg_radiation_kwh = _megajoules_to_kwh(growing_avg_radiation_mj)
    period_start, period_end, start_year, end_year = _historical_climate_window()
    hardiness_band = _build_hardiness_band(avg_annual_extreme_min_f)
    frost_window = _build_frost_window(daily_times, [
        _celsius_to_fahrenheit(_optional_float(value)) for value in daily_temperature_min
    ])

    return {
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "time_zone": historical_payload.get("timezone") or time_zone_name or "UTC",
        "source": "open-meteo-historical-weather",
        "period_start": period_start,
        "period_end": period_end,
        "years_sampled": max(end_year - start_year + 1, 0),
        "season_window": {
            "label": "April-September",
            "start_month": "04",
            "end_month": "09",
        },
        "hardiness_zone": hardiness_band,
        "frost_window": frost_window,
        "annual": {
            "average_temperature_c": _round_float(annual_avg_temperature_c, 1),
            "average_temperature_f": _round_float(annual_avg_temperature_f, 1),
            "average_relative_humidity": _round_float(annual_avg_humidity, 1),
            "average_daily_shortwave_radiation_mj_m2": _round_float(annual_avg_radiation_mj, 2),
            "average_daily_shortwave_radiation_kwh_m2": _round_float(annual_avg_radiation_kwh, 2),
        },
        "growing_season": {
            "label": "April-September",
            "average_temperature_c": _round_float(growing_avg_temperature_c, 1),
            "average_temperature_f": _round_float(growing_avg_temperature_f, 1),
            "average_relative_humidity": _round_float(growing_avg_humidity, 1),
            "average_daily_shortwave_radiation_mj_m2": _round_float(growing_avg_radiation_mj, 2),
            "average_daily_shortwave_radiation_kwh_m2": _round_float(growing_avg_radiation_kwh, 2),
        },
        "monthly_profiles": monthly_profiles,
        "monthly_temperature_f": {
            month_key: profile.get("average_temperature_f")
            for month_key, profile in monthly_profiles.items()
        },
        "monthly_relative_humidity": {
            month_key: profile.get("average_relative_humidity")
            for month_key, profile in monthly_profiles.items()
        },
        "monthly_shortwave_radiation_kwh_m2": {
            month_key: profile.get("average_daily_shortwave_radiation_kwh_m2")
            for month_key, profile in monthly_profiles.items()
        },
        "seasonal_extremes": {
            "warmest_month": _build_climate_extreme(
                monthly_profiles,
                "average_temperature_f",
                "temperature_f",
            ),
            "coolest_month": _build_climate_extreme(
                monthly_profiles,
                "average_temperature_f",
                "temperature_f",
                maximize=False,
            ),
            "most_humid_month": _build_climate_extreme(
                monthly_profiles,
                "average_relative_humidity",
                "relative_humidity",
            ),
            "sunniest_month": _build_climate_extreme(
                monthly_profiles,
                "average_daily_shortwave_radiation_kwh_m2",
                "shortwave_radiation_kwh_m2",
            ),
        },
        "summary": (
            f"Estimated hardiness band {hardiness_band.get('label') or 'unknown'} using {start_year}-{end_year} "
            f"historical weather. Average growing-season conditions run about "
            f"{_round_float(growing_avg_temperature_f, 1) or 0:.1f}°F and "
            f"{_round_float(growing_avg_humidity, 0) or 0:.0f}% relative humidity. "
            f"{frost_window.get('summary')}"
        ),
        "model_note": (
            "Property-level climate averages from historical weather. This does not model "
            "parcel-specific shade, tree canopy, soil, or irrigation conditions."
        ),
    }


def get_space_weather_snapshot(
    latitude: float,
    longitude: float,
    time_zone_name: str,
    force_refresh: bool = False,
):
    scales_response = _fetch_json(
        NOAA_SCALES_URL,
        ttl_seconds=60,
        force_refresh=force_refresh,
        return_metadata=True,
        source_name="noaa-scales",
    )
    alerts_response = _fetch_json(
        NOAA_ALERTS_URL,
        ttl_seconds=60,
        force_refresh=force_refresh,
        return_metadata=True,
        source_name="noaa-alerts",
    )
    plasma_response = _fetch_json(
        NOAA_PLASMA_URL,
        ttl_seconds=60,
        force_refresh=force_refresh,
        return_metadata=True,
        source_name="noaa-solar-wind",
    )
    xray_response = _fetch_json(
        NOAA_XRAY_URL,
        ttl_seconds=60,
        force_refresh=force_refresh,
        return_metadata=True,
        source_name="noaa-goes-xray",
    )
    aurora_response = _fetch_optional_json(
        NOAA_AURORA_OVATION_URL,
        ttl_seconds=300,
        force_refresh=force_refresh,
        source_name="noaa-ovation-aurora",
        default_data={},
    )
    drap_response = _fetch_optional_text(
        NOAA_DRAP_URL,
        ttl_seconds=60,
        force_refresh=force_refresh,
        source_name="noaa-drap",
        default_data="",
    )
    scales_payload = scales_response["data"]
    alerts_payload = alerts_response["data"]
    plasma_payload = plasma_response["data"]
    xray_payload = xray_response["data"]
    aurora_payload = aurora_response["data"]
    drap_context = _build_drap_context(drap_response.get("data") or "", latitude, longitude)
    glotec_context = _fetch_glotec_context(
        latitude,
        longitude,
        force_refresh=force_refresh,
    )

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=7)
    flares_response = _fetch_json(
        NASA_DONKI_FLR_URL,
        params={
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        },
        ttl_seconds=1800,
        force_refresh=force_refresh,
        return_metadata=True,
        source_name="nasa-donki-flares",
    )
    storms_response = _fetch_json(
        NASA_DONKI_GST_URL,
        params={
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        },
        ttl_seconds=1800,
        force_refresh=force_refresh,
        return_metadata=True,
        source_name="nasa-donki-geomagnetic-storms",
    )
    flares_payload = flares_response["data"]
    storms_payload = storms_response["data"]
    irradiance_snapshot = get_surface_irradiance_snapshot(
        latitude,
        longitude,
        time_zone_name,
        force_refresh=force_refresh,
    )

    current_scale_entry = _extract_scale_entry(scales_payload, "0")
    day_one_scale_entry = _extract_scale_entry(scales_payload, "1")
    day_two_scale_entry = _extract_scale_entry(scales_payload, "2")
    day_three_scale_entry = _extract_scale_entry(scales_payload, "3")
    radio_blackout_scale = _format_scale(current_scale_entry.get("R"))
    radiation_storm_scale = _format_scale(current_scale_entry.get("S"))
    geomagnetic_storm_scale = _format_scale(current_scale_entry.get("G"))

    warning_count, watch_count, alert_count, recent_headlines = _build_recent_headlines(alerts_payload)
    plasma_row = (plasma_payload[-1] if isinstance(plasma_payload, list) and len(plasma_payload) > 1 else [])
    solar_wind = {
        "observed_at": plasma_row[0] if len(plasma_row) > 0 else None,
        "density_p_cm3": round(_safe_float(plasma_row[1]), 2) if len(plasma_row) > 1 else 0.0,
        "speed_km_s": round(_safe_float(plasma_row[2]), 1) if len(plasma_row) > 2 else 0.0,
        "temperature_k": round(_safe_float(plasma_row[3]), 0) if len(plasma_row) > 3 else 0.0,
    }

    long_band_rows = []
    if isinstance(xray_payload, list):
        long_band_rows = [row for row in xray_payload if row.get("energy") == "0.1-0.8nm"]
    long_band_rows.sort(key=lambda row: str(row.get("time_tag") or ""))
    latest_long_band_row = long_band_rows[-1] if long_band_rows else {}
    peak_long_band_row = (
        max(long_band_rows, key=lambda row: _safe_float(row.get("flux"))) if long_band_rows else {}
    )

    latest_flare_event = None
    if isinstance(flares_payload, list) and flares_payload:
        latest_flare = max(
            flares_payload,
            key=lambda event: str(event.get("peakTime") or event.get("beginTime") or ""),
        )
        latest_flare_event = {
            "class": latest_flare.get("classType"),
            "peak_time": latest_flare.get("peakTime") or latest_flare.get("beginTime"),
            "source_location": latest_flare.get("sourceLocation"),
        }

    latest_geomagnetic_storm = None
    if isinstance(storms_payload, list) and storms_payload:
        latest_storm = max(
            storms_payload,
            key=lambda event: str(event.get("startTime") or ""),
        )
        kp_index = 0.0
        for entry in latest_storm.get("allKpIndex") or []:
            kp_index = max(kp_index, _safe_float(entry.get("kpIndex")))
        latest_geomagnetic_storm = {
            "start_time": latest_storm.get("startTime"),
            "max_kp_index": round(kp_index, 2),
        }

    is_daylight = bool(irradiance_snapshot.get("is_daylight"))
    latitude_band = _latitude_band(latitude)
    aurora_context = _build_aurora_context(aurora_payload, latitude, longitude, is_daylight)
    aurora_potential = _aurora_potential(
        geomagnetic_storm_scale["scale"],
        latitude,
        is_daylight,
        aurora_context,
    )
    hf_radio_risk = _hf_radio_risk(
        radio_blackout_scale["scale"],
        is_daylight,
        drap_context,
    )
    gnss_risk = _gnss_risk(
        geomagnetic_storm_scale["scale"],
        radiation_storm_scale["scale"],
        latitude,
        glotec_context,
    )
    ground_note = (
        "No elevated residential ground-level concern."
        if radiation_storm_scale["scale"] < 3
        else "Elevated concern stays weighted toward high-latitude or high-altitude operations, not typical residential ground level."
    )
    alert_level = _space_weather_alert_level(
        radio_blackout_scale["scale"],
        radiation_storm_scale["scale"],
        geomagnetic_storm_scale["scale"],
        is_daylight,
        latitude,
        warning_count,
        watch_count,
        aurora_context,
        drap_context,
        glotec_context,
    )

    summary = _build_space_weather_summary(
        alert_level,
        radio_blackout_scale["scale"],
        radiation_storm_scale["scale"],
        geomagnetic_storm_scale["scale"],
        is_daylight,
        aurora_potential,
        gnss_risk,
        aurora_context,
        drap_context,
        glotec_context,
    )
    freshness_sources = {
        "noaa-scales": scales_response.get("freshness") or {},
        "noaa-alerts": alerts_response.get("freshness") or {},
        "noaa-solar-wind": plasma_response.get("freshness") or {},
        "noaa-goes-xray": xray_response.get("freshness") or {},
        "noaa-ovation-aurora": aurora_response.get("freshness") or {},
        "noaa-drap": drap_response.get("freshness") or {},
        "nasa-donki-flares": flares_response.get("freshness") or {},
        "nasa-donki-geomagnetic-storms": storms_response.get("freshness") or {},
    }
    freshness_sources.update((glotec_context.get("freshness") or {}).get("sources") or {})
    freshness_sources.update((irradiance_snapshot.get("freshness") or {}).get("sources") or {})
    reasons = _build_space_weather_reasons(
        radio_blackout_scale["scale"],
        radiation_storm_scale["scale"],
        geomagnetic_storm_scale["scale"],
        is_daylight,
        latitude_band,
        aurora_potential,
        aurora_context,
        drap_context,
        glotec_context,
        hf_radio_risk,
        gnss_risk,
        ground_note,
    )

    return {
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "time_zone": irradiance_snapshot.get("time_zone") or time_zone_name or "UTC",
        "observed_at": solar_wind["observed_at"] or latest_long_band_row.get("time_tag"),
        "global": {
            "radio_blackout_scale": radio_blackout_scale,
            "radiation_storm_scale": radiation_storm_scale,
            "geomagnetic_storm_scale": geomagnetic_storm_scale,
            "solar_wind": solar_wind,
            "current_xray_class": _format_xray_class(latest_long_band_row.get("flux")),
            "peak_xray_24h_class": _format_xray_class(peak_long_band_row.get("flux")),
            "lookahead": {
                "day_1": {
                    "date": day_one_scale_entry.get("DateStamp"),
                    "radio_blackout": _format_scale(day_one_scale_entry.get("R")),
                    "radiation_storm": _format_scale(day_one_scale_entry.get("S")),
                    "geomagnetic_storm": _format_scale(day_one_scale_entry.get("G")),
                },
                "day_2": {
                    "date": day_two_scale_entry.get("DateStamp"),
                    "radio_blackout": _format_scale(day_two_scale_entry.get("R")),
                    "radiation_storm": _format_scale(day_two_scale_entry.get("S")),
                    "geomagnetic_storm": _format_scale(day_two_scale_entry.get("G")),
                },
                "day_3": {
                    "date": day_three_scale_entry.get("DateStamp"),
                    "radio_blackout": _format_scale(day_three_scale_entry.get("R")),
                    "radiation_storm": _format_scale(day_three_scale_entry.get("S")),
                    "geomagnetic_storm": _format_scale(day_three_scale_entry.get("G")),
                },
            },
        },
        "local": {
            "is_daylight": is_daylight,
            "latitude_band": latitude_band,
            "aurora_visibility_potential": aurora_potential,
            "aurora_viewline": aurora_context,
            "drap": drap_context,
            "glotec": {
                key: value
                for key, value in glotec_context.items()
                if key != "freshness"
            },
            "hf_radio_risk": hf_radio_risk,
            "gnss_risk": gnss_risk,
            "ground_radiation_note": ground_note,
        },
        "alert_level": alert_level,
        "alert_count": alert_count,
        "watch_count": watch_count,
        "warning_count": warning_count,
        "freshness": _aggregate_freshness(freshness_sources),
        "reasons": reasons,
        "recent_headlines": recent_headlines,
        "recent_activity": {
            "latest_flare_event": latest_flare_event,
            "latest_geomagnetic_storm": latest_geomagnetic_storm,
        },
        "summary": summary,
        "sources": [
            "noaa-scales",
            "noaa-alerts",
            "noaa-solar-wind",
            "noaa-goes-xray",
            "noaa-ovation-aurora",
            "noaa-drap",
            "noaa-glotec",
            "nasa-donki",
            "open-meteo-forecast",
        ],
    }
