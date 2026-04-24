from __future__ import annotations

import math
import re
import time
from typing import Any, Optional

import requests


_CACHE: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}

OVERPASS_INTERPRETER_URL = "https://overpass-api.de/api/interpreter"
OPEN_TOPO_DATA_URL = "https://api.opentopodata.org/v1/srtm90m"
EARTH_RADIUS_METERS = 6371000
SQ_METERS_TO_SQ_FEET = 10.7639
BASELINE_PANEL_EFFICIENCY = 0.20
ROOF_COVERAGE_FACTOR = 0.565


def _normalize_params(params: Optional[dict[str, Any]]) -> tuple[tuple[str, str], ...]:
    if not params:
        return ()
    return tuple(sorted((str(key), str(value)) for key, value in params.items()))


def _fetch_json(url: str, params: Optional[dict[str, Any]] = None, ttl_seconds: int = 300):
    key = (url, _normalize_params(params))
    now = time.time()
    cached = _CACHE.get(key)
    if cached and cached["expires_at"] > now:
        return cached["data"]

    response = requests.get(url, params=params, timeout=20)
    response.raise_for_status()
    data = response.json()
    _CACHE[key] = {
        "data": data,
        "expires_at": now + ttl_seconds,
    }
    return data


def _safe_float(value: Any, default: Optional[float] = None):
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _round(value: Optional[float], digits: int = 1):
    if value is None:
        return None
    return round(value, digits)


def _clamp(value: float, minimum: float, maximum: float):
    return max(minimum, min(value, maximum))


def _meters_to_lat_delta(meters: float):
    return (meters / EARTH_RADIUS_METERS) * (180 / math.pi)


def _meters_to_lon_delta(meters: float, latitude: float):
    latitude_radians = math.radians(latitude)
    cosine = max(math.cos(latitude_radians), 0.2)
    return (meters / (EARTH_RADIUS_METERS * cosine)) * (180 / math.pi)


def _offset_coordinate(latitude: float, longitude: float, north_meters: float = 0, east_meters: float = 0):
    return {
        "lat": round(latitude + _meters_to_lat_delta(north_meters), 6),
        "lng": round(longitude + _meters_to_lon_delta(east_meters, latitude), 6),
    }


def _haversine_meters(latitude_a: float, longitude_a: float, latitude_b: float, longitude_b: float):
    latitude_a_radians = math.radians(latitude_a)
    latitude_b_radians = math.radians(latitude_b)
    latitude_delta = math.radians(latitude_b - latitude_a)
    longitude_delta = math.radians(longitude_b - longitude_a)

    haversine_value = (
        math.sin(latitude_delta / 2) ** 2
        + math.cos(latitude_a_radians)
        * math.cos(latitude_b_radians)
        * math.sin(longitude_delta / 2) ** 2
    )
    return 2 * EARTH_RADIUS_METERS * math.atan2(math.sqrt(haversine_value), math.sqrt(1 - haversine_value))


def _bearing_degrees(latitude_a: float, longitude_a: float, latitude_b: float, longitude_b: float):
    latitude_a_radians = math.radians(latitude_a)
    latitude_b_radians = math.radians(latitude_b)
    longitude_delta = math.radians(longitude_b - longitude_a)

    x_value = math.sin(longitude_delta) * math.cos(latitude_b_radians)
    y_value = (
        math.cos(latitude_a_radians) * math.sin(latitude_b_radians)
        - math.sin(latitude_a_radians) * math.cos(latitude_b_radians) * math.cos(longitude_delta)
    )
    return (math.degrees(math.atan2(x_value, y_value)) + 360) % 360


def _direction_bucket(bearing_degrees: float):
    directions = [
        "north",
        "northeast",
        "east",
        "southeast",
        "south",
        "southwest",
        "west",
        "northwest",
    ]
    return directions[int(((bearing_degrees + 22.5) % 360) // 45)]


def _direction_group(direction_bucket: str):
    if direction_bucket in {"north", "northeast", "northwest"}:
        return "north"
    if direction_bucket in {"south", "southeast", "southwest"}:
        return "south"
    if direction_bucket == "east":
        return "east"
    if direction_bucket == "west":
        return "west"
    return "unknown"


def _parse_height_meters(tags: dict[str, Any]):
    height_value = str(tags.get("height") or "").strip().lower()
    if height_value:
        feet_match = re.search(r"(-?\d+(?:\.\d+)?)\s*ft", height_value)
        if feet_match:
            return max(float(feet_match.group(1)) * 0.3048, 3.0)

        number_match = re.search(r"(-?\d+(?:\.\d+)?)", height_value)
        if number_match:
            return max(float(number_match.group(1)), 3.0)

    levels_value = _safe_float(tags.get("building:levels"))
    if levels_value is not None:
        return max(levels_value * 3.1, 3.0)

    building_kind = str(tags.get("building") or "").lower()
    if building_kind in {"garage", "shed", "carport"}:
        return 3.4
    if building_kind in {"house", "residential", "apartments"}:
        return 7.5
    return 9.0


def _polygon_centroid(points: list[dict[str, float]]):
    if not points:
        return None

    return {
        "lat": round(sum(point["lat"] for point in points) / len(points), 6),
        "lng": round(sum(point["lng"] for point in points) / len(points), 6),
    }


def _build_polygon_geometry(points: list[dict[str, float]]):
    if len(points) < 3:
        return None

    ring = [[round(point["lng"], 6), round(point["lat"], 6)] for point in points]
    if ring[0] != ring[-1]:
        ring.append(ring[0])

    return {
        "type": "Polygon",
        "coordinates": [ring],
    }


def _build_point_geometry(point: Optional[dict[str, float]]):
    if not point:
        return None
    return {
        "type": "Point",
        "coordinates": [round(point["lng"], 6), round(point["lat"], 6)],
    }


def _point_within_bounds(point: Optional[dict[str, float]], bounds: Optional[dict[str, Any]]):
    if not point or not bounds:
        return False

    return (
        float(bounds.get("south") or -90) <= float(point.get("lat") or 0) <= float(bounds.get("north") or 90)
        and float(bounds.get("west") or -180) <= float(point.get("lng") or 0) <= float(bounds.get("east") or 180)
    )


def _project_point_meters(reference_lat: float, reference_lng: float, point: dict[str, float]):
    latitude_radians = math.radians(reference_lat)
    x_value = math.radians(point["lng"] - reference_lng) * EARTH_RADIUS_METERS * math.cos(latitude_radians)
    y_value = math.radians(point["lat"] - reference_lat) * EARTH_RADIUS_METERS
    return x_value, y_value


def _polygon_area_square_meters(points: list[dict[str, float]]):
    if len(points) < 3:
        return None

    reference = _polygon_centroid(points)
    if not reference:
        return None

    projected_points = [_project_point_meters(reference["lat"], reference["lng"], point) for point in points]
    if projected_points[0] != projected_points[-1]:
        projected_points.append(projected_points[0])

    area = 0.0
    for index in range(len(projected_points) - 1):
        x_a, y_a = projected_points[index]
        x_b, y_b = projected_points[index + 1]
        area += (x_a * y_b) - (x_b * y_a)

    return abs(area) / 2


def _dominant_edge(points: list[dict[str, float]]):
    if len(points) < 2:
        return None

    ring_points = points[:]
    if ring_points[0] != ring_points[-1]:
        ring_points.append(ring_points[0])

    longest_edge = None
    for index in range(len(ring_points) - 1):
        start = ring_points[index]
        end = ring_points[index + 1]
        edge_length_m = _haversine_meters(start["lat"], start["lng"], end["lat"], end["lng"])
        if edge_length_m < 2:
            continue

        edge = {
            "bearing": _round(_bearing_degrees(start["lat"], start["lng"], end["lat"], end["lng"]), 1),
            "length_m": _round(edge_length_m, 1),
        }
        if not longest_edge or edge["length_m"] > longest_edge["length_m"]:
            longest_edge = edge

    return longest_edge


def _classify_pressure(score: float):
    if score >= 0.7:
        return "high"
    if score >= 0.35:
        return "moderate"
    return "low"


def _classify_terrain(slope_percent: float):
    if slope_percent >= 14:
        return "steep"
    if slope_percent >= 7:
        return "rolling"
    if slope_percent >= 3:
        return "gentle"
    return "flat"


def _parse_canopy_height_meters(tags: dict[str, Any]):
    height_value = str(tags.get("height") or tags.get("est_height") or "").strip().lower()
    if height_value:
        feet_match = re.search(r"(-?\d+(?:\.\d+)?)\s*ft", height_value)
        if feet_match:
            return max(float(feet_match.group(1)) * 0.3048, 2.5)

        number_match = re.search(r"(-?\d+(?:\.\d+)?)", height_value)
        if number_match:
            return max(float(number_match.group(1)), 2.5)

    genus = str(tags.get("genus") or "").lower()
    natural = str(tags.get("natural") or "").lower()
    landuse = str(tags.get("landuse") or "").lower()

    if natural == "tree":
        return 8.0 if genus not in {"palm"} else 5.0
    if natural in {"wood", "tree_row"}:
        return 7.0
    if natural == "scrub":
        return 3.5
    if landuse in {"forest", "orchard"}:
        return 6.5
    if landuse == "vineyard":
        return 2.5
    return 5.5


def _build_context_envelope(latitude: float, longitude: float, bounds: Optional[dict[str, Any]]):
    if bounds:
        try:
            south = float(bounds["south"])
            north = float(bounds["north"])
            west = float(bounds["west"])
            east = float(bounds["east"])
            lat_span = abs(north - south)
            lon_span = abs(east - west)
            if 0 < lat_span <= 0.0015 and 0 < lon_span <= 0.0018:
                width_m = _haversine_meters(latitude, west, latitude, east)
                height_m = _haversine_meters(south, longitude, north, longitude)
                return {
                    "bounds": {
                        "south": round(south, 6),
                        "north": round(north, 6),
                        "west": round(west, 6),
                        "east": round(east, 6),
                    },
                    "width_m": round(width_m, 1),
                    "height_m": round(height_m, 1),
                    "source": "geocoder-match-envelope",
                    "label": "Geocoder match envelope",
                }
        except (KeyError, TypeError, ValueError):
            pass

    half_height_m = 24
    half_width_m = 32
    north = latitude + _meters_to_lat_delta(half_height_m)
    south = latitude - _meters_to_lat_delta(half_height_m)
    east = longitude + _meters_to_lon_delta(half_width_m, latitude)
    west = longitude - _meters_to_lon_delta(half_width_m, latitude)
    return {
        "bounds": {
            "south": round(south, 6),
            "north": round(north, 6),
            "west": round(west, 6),
            "east": round(east, 6),
        },
        "width_m": round(half_width_m * 2, 1),
        "height_m": round(half_height_m * 2, 1),
        "source": "synthetic-planning-envelope",
        "label": "Planning envelope",
    }


def _bounds_area_square_meters(bounds: dict[str, Any]):
    try:
        south = float(bounds["south"])
        north = float(bounds["north"])
        west = float(bounds["west"])
        east = float(bounds["east"])
    except (KeyError, TypeError, ValueError):
        return 0.0

    center_latitude = (south + north) / 2
    center_longitude = (west + east) / 2
    width_m = _haversine_meters(center_latitude, west, center_latitude, east)
    height_m = _haversine_meters(south, center_longitude, north, center_longitude)
    return max(width_m * height_m, 0.0)


def _inset_bounds(bounds: dict[str, Any], latitude: float, inset_m: float):
    try:
        south = float(bounds["south"]) + _meters_to_lat_delta(inset_m)
        north = float(bounds["north"]) - _meters_to_lat_delta(inset_m)
        west = float(bounds["west"]) + _meters_to_lon_delta(inset_m, latitude)
        east = float(bounds["east"]) - _meters_to_lon_delta(inset_m, latitude)
    except (KeyError, TypeError, ValueError):
        return None

    if south >= north or west >= east:
        return None

    return {
        "south": round(south, 6),
        "north": round(north, 6),
        "west": round(west, 6),
        "east": round(east, 6),
    }


def _classify_parcel_shape(width_m: float, height_m: float):
    if height_m <= 0:
        return "unknown"

    ratio = width_m / height_m
    if ratio >= 1.45:
        return "wide"
    if ratio <= 0.72:
        return "deep"
    return "compact"


def _classify_terrain_limit(terrain_context: dict[str, Any]):
    slope_percent = _safe_float(terrain_context.get("slope_percent"), 0) or 0
    terrain_class = terrain_context.get("terrain_class") or "flat"

    if terrain_class == "steep" or slope_percent >= 12:
        return "high"
    if terrain_class == "rolling" or slope_percent >= 7:
        return "moderate"
    return "low"


def _build_parcel_context(
    latitude: float,
    envelope: dict[str, Any],
    building_context: dict[str, Any],
    canopy_context: dict[str, Any],
    terrain_context: dict[str, Any],
):
    bounds = envelope.get("bounds") or {}
    width_m = _safe_float(envelope.get("width_m"), 0) or 0
    height_m = _safe_float(envelope.get("height_m"), 0) or 0
    gross_area_sq_m = _bounds_area_square_meters(bounds)
    gross_area_sq_ft = gross_area_sq_m * SQ_METERS_TO_SQ_FEET

    shortest_edge_m = max(min(width_m, height_m), 4.0)
    raw_edge_buffer_m = _clamp(shortest_edge_m * 0.12, 2.5, 6.0)
    max_edge_buffer_m = max(shortest_edge_m / 2 - 1.0, 1.5)
    edge_buffer_m = min(raw_edge_buffer_m, max_edge_buffer_m)

    planning_core_bounds = _inset_bounds(bounds, latitude, edge_buffer_m)
    planning_core_area_sq_m = _bounds_area_square_meters(planning_core_bounds) if planning_core_bounds else 0.0
    planning_core_area_sq_ft = planning_core_area_sq_m * SQ_METERS_TO_SQ_FEET
    planning_core_share = (
        planning_core_area_sq_m / gross_area_sq_m
        if gross_area_sq_m > 0
        else 0.0
    )

    building_pressure = building_context.get("directional_pressure") or {}
    canopy_pressure = canopy_context.get("directional_pressure") or {}
    terrain_aspect = terrain_context.get("dominant_aspect") or "flat"
    terrain_limit = _classify_terrain_limit(terrain_context)

    terrain_open_bonus = {
        "north": 0.0,
        "south": 0.0,
        "east": 0.0,
        "west": 0.0,
    }
    if terrain_aspect == "south-facing":
        terrain_open_bonus["south"] = 0.14
        terrain_open_bonus["north"] = -0.06
    elif terrain_aspect == "north-facing":
        terrain_open_bonus["north"] = 0.08
        terrain_open_bonus["south"] = -0.10
    elif terrain_aspect == "east-facing":
        terrain_open_bonus["east"] = 0.05
        terrain_open_bonus["west"] = -0.03
    elif terrain_aspect == "west-facing":
        terrain_open_bonus["west"] = 0.05
        terrain_open_bonus["east"] = -0.03

    directional_open_score = {}
    for direction in ("north", "south", "east", "west"):
        combined_pressure = (
            (_safe_float(building_pressure.get(direction), 0) or 0)
            + ((_safe_float(canopy_pressure.get(direction), 0) or 0) * 0.85)
        )
        directional_open_score[direction] = _round(
            max(0.0, 1.65 - combined_pressure + terrain_open_bonus.get(direction, 0.0)),
            2,
        )

    open_side = max(directional_open_score, key=directional_open_score.get)
    building_penalty_share = min(
        (
            (_safe_float((building_context.get("nearest_building") or {}).get("shadow_pressure"), 0) or 0)
            * 0.08
        )
        + ((len(building_context.get("nearby_buildings") or []) * 0.015)),
        0.18,
    )
    canopy_penalty_share = min(
        (
            (_safe_float((canopy_context.get("nearest_canopy") or {}).get("canopy_pressure"), 0) or 0)
            * 0.07
        )
        + ((len(canopy_context.get("nearby_canopy") or []) * 0.01)),
        0.12,
    )
    terrain_penalty_share = 0.12 if terrain_limit == "high" else 0.06 if terrain_limit == "moderate" else 0.0
    minimum_share = 0.12 if planning_core_share > 0 else 0.0
    maximum_share = planning_core_share if planning_core_share > 0 else 0.0
    estimated_plantable_share = _clamp(
        planning_core_share - building_penalty_share - canopy_penalty_share - terrain_penalty_share,
        minimum_share,
        maximum_share,
    )
    estimated_plantable_area_sq_m = gross_area_sq_m * estimated_plantable_share
    estimated_plantable_area_sq_ft = estimated_plantable_area_sq_m * SQ_METERS_TO_SQ_FEET

    return {
        "source": envelope.get("source") or "planning-envelope",
        "label": "Planning core",
        "bounds": bounds,
        "gross_area_sq_m": _round(gross_area_sq_m, 1),
        "gross_area_sq_ft": _round(gross_area_sq_ft, 0),
        "shape_class": _classify_parcel_shape(width_m, height_m),
        "edge_buffer_m": _round(edge_buffer_m, 1),
        "planning_core_bounds": planning_core_bounds,
        "planning_core_area_sq_m": _round(planning_core_area_sq_m, 1),
        "planning_core_area_sq_ft": _round(planning_core_area_sq_ft, 0),
        "planning_core_share": _round(planning_core_share, 2),
        "estimated_plantable_share": _round(estimated_plantable_share, 2),
        "estimated_plantable_area_sq_m": _round(estimated_plantable_area_sq_m, 1),
        "estimated_plantable_area_sq_ft": _round(estimated_plantable_area_sq_ft, 0),
        "terrain_limit": terrain_limit,
        "open_side": open_side,
        "directional_open_score": directional_open_score,
        "summary": (
            f"Planning envelope covers about {gross_area_sq_ft:.0f} sq ft. "
            f"An inset planning core keeps about {planning_core_area_sq_ft:.0f} sq ft away from edge effects, "
            f"with the most open side looking {open_side} and {terrain_limit} terrain constraint."
        ),
    }


def _build_overpass_buildings(latitude: float, longitude: float, radius_m: int):
    query = (
        f'[out:json][timeout:20];'
        f'way["building"](around:{radius_m},{latitude},{longitude});'
        f'out tags geom center;'
    )
    return _fetch_json(
        OVERPASS_INTERPRETER_URL,
        params={"data": query},
        ttl_seconds=86400,
    )


def _build_overpass_canopy(latitude: float, longitude: float, radius_m: int):
    query = (
        f'[out:json][timeout:20];('
        f'node["natural"="tree"](around:{radius_m},{latitude},{longitude});'
        f'way["natural"~"wood|tree_row|scrub"](around:{radius_m},{latitude},{longitude});'
        f'way["landuse"~"forest|orchard|vineyard"](around:{radius_m},{latitude},{longitude});'
        f');out tags geom center;'
    )
    return _fetch_json(
        OVERPASS_INTERPRETER_URL,
        params={"data": query},
        ttl_seconds=86400,
    )


def _build_building_context(latitude: float, longitude: float, envelope: dict[str, Any]):
    radius_m = int(_clamp(max(envelope.get("width_m") or 0, envelope.get("height_m") or 0) * 0.95, 50, 95))
    try:
        payload = _build_overpass_buildings(latitude, longitude, radius_m)
    except requests.RequestException:
        return {
            "source": "openstreetmap-overpass",
            "search_radius_m": radius_m,
            "building_count": 0,
            "nearby_buildings": [],
            "nearest_building": None,
            "directional_pressure": {
                "north": 0.0,
                "south": 0.0,
                "east": 0.0,
                "west": 0.0,
            },
            "obstruction_risk": "low",
            "summary": "Mapped building context is unavailable for this property right now.",
        }
    directional_scores = {"north": 0.0, "south": 0.0, "east": 0.0, "west": 0.0}
    nearby_buildings = []

    for element in payload.get("elements") or []:
        geometry_points = [
            {
                "lat": float(point.get("lat")),
                "lng": float(point.get("lon")),
            }
            for point in (element.get("geometry") or [])
            if point.get("lat") is not None and point.get("lon") is not None
        ]
        if len(geometry_points) < 3:
            continue

        centroid = {
            "lat": round(float((element.get("center") or {}).get("lat")), 6),
            "lng": round(float((element.get("center") or {}).get("lon")), 6),
        } if (element.get("center") or {}).get("lat") is not None and (element.get("center") or {}).get("lon") is not None else _polygon_centroid(geometry_points)
        if not centroid:
            continue

        distance_m = _haversine_meters(latitude, longitude, centroid["lat"], centroid["lng"])
        if distance_m > radius_m * 1.1:
            continue

        bearing = _bearing_degrees(latitude, longitude, centroid["lat"], centroid["lng"])
        direction_bucket = _direction_bucket(bearing)
        direction_group = _direction_group(direction_bucket)
        tags = element.get("tags") or {}
        height_m = _parse_height_meters(tags)
        geometry = _build_polygon_geometry(geometry_points)
        footprint_area_square_meters = _polygon_area_square_meters(geometry_points)
        dominant_edge = _dominant_edge(geometry_points)
        shadow_pressure = min(height_m / max(distance_m, 6), 2.5)
        directional_scores[direction_group] = directional_scores.get(direction_group, 0.0) + shadow_pressure

        nearby_buildings.append(
            {
                "id": f"osm-way-{element.get('id')}",
                "name": tags.get("name") or tags.get("addr:housenumber") or "Nearby building",
                "kind": tags.get("building") or "building",
                "levels": _safe_float(tags.get("building:levels")),
                "height_m": _round(height_m, 1),
                "distance_m": _round(distance_m, 1),
                "bearing_degrees": _round(bearing, 0),
                "direction_bucket": direction_bucket,
                "direction_group": direction_group,
                "shadow_pressure": _round(shadow_pressure, 2),
                "obstruction_risk": _classify_pressure(shadow_pressure),
                "footprint_area_square_meters": _round(footprint_area_square_meters, 1),
                "footprint_area_square_feet": (
                    _round(footprint_area_square_meters * SQ_METERS_TO_SQ_FEET, 1)
                    if footprint_area_square_meters is not None
                    else None
                ),
                "centroid_within_match_envelope": _point_within_bounds(
                    centroid,
                    envelope.get("bounds"),
                ),
                "dominant_edge_bearing": dominant_edge.get("bearing") if dominant_edge else None,
                "dominant_edge_length_m": dominant_edge.get("length_m") if dominant_edge else None,
                "centroid": centroid,
                "geometry": geometry,
            }
        )

    nearby_buildings.sort(
        key=lambda building: (
            -(building.get("shadow_pressure") or 0),
            building.get("distance_m") or 9999,
        )
    )
    nearby_buildings = nearby_buildings[:8]
    nearest_building = min(
        nearby_buildings,
        key=lambda building: building.get("distance_m") or 9999,
    ) if nearby_buildings else None
    strongest_south_pressure = directional_scores.get("south", 0.0)
    combined_pressure = (
        strongest_south_pressure * 0.8
        + (directional_scores.get("east", 0.0) + directional_scores.get("west", 0.0)) * 0.3
        + directional_scores.get("north", 0.0) * 0.1
    )

    if not nearby_buildings:
        summary = "No nearby OpenStreetMap building footprints were found in the current planning radius."
    else:
        summary = (
            f"{len(nearby_buildings)} nearby building footprints found. "
            f"Nearest structure is about {nearest_building.get('distance_m', 0):.0f} m away, "
            f"with the strongest structure pressure on the {max(directional_scores, key=directional_scores.get)} side."
        )

    return {
        "source": "openstreetmap-overpass",
        "search_radius_m": radius_m,
        "building_count": len(nearby_buildings),
        "nearby_buildings": nearby_buildings,
        "nearest_building": nearest_building,
        "directional_pressure": {
            direction: _round(score, 2)
            for direction, score in directional_scores.items()
        },
        "obstruction_risk": _classify_pressure(combined_pressure),
        "summary": summary,
    }


def _is_primary_roof_candidate_kind(building_kind: Optional[str]):
    normalized_kind = str(building_kind or "").strip().lower()
    if not normalized_kind:
        return True

    disfavored_kinds = {
        "barn",
        "carport",
        "garage",
        "garages",
        "greenhouse",
        "hut",
        "kiosk",
        "roof",
        "service",
        "shed",
        "silo",
        "storage_tank",
    }
    return normalized_kind not in disfavored_kinds


def _build_unavailable_roof_capacity_context(summary: str):
    return {
        "available": False,
        "candidate_building_id": None,
        "candidate_building_name": None,
        "candidate_building_kind": None,
        "candidate_building_distance_m": None,
        "centroid_within_match_envelope": False,
        "gross_footprint_area_square_meters": None,
        "gross_footprint_area_square_feet": None,
        "usable_roof_area_square_meters": None,
        "usable_roof_area_square_feet": None,
        "recommended_system_size_kw": None,
        "dominant_edge_bearing": None,
        "dominant_edge_length_m": None,
        "confidence": "low",
        "summary": summary,
        "model_note": (
            "Roof-capacity inference is only emitted when a nearby primary building footprint looks "
            "confident enough to stand in for the main roof."
        ),
    }


def _build_roof_capacity_context(
    envelope: dict[str, Any],
    building_context: dict[str, Any],
    match_quality: Optional[str],
):
    nearby_buildings = list(building_context.get("nearby_buildings") or [])
    candidate_buildings = [
        building
        for building in nearby_buildings
        if (building.get("footprint_area_square_meters") or 0) >= 20
    ]
    if not candidate_buildings:
        return _build_unavailable_roof_capacity_context(
            "No confident primary building footprint was available to infer parcel roof capacity."
        )

    primary_building = sorted(
        candidate_buildings,
        key=lambda building: (
            0 if building.get("centroid_within_match_envelope") else 1,
            0 if _is_primary_roof_candidate_kind(building.get("kind")) else 1,
            -(building.get("footprint_area_square_meters") or 0),
            building.get("distance_m") or 9999,
        ),
    )[0]

    gross_area_square_meters = float(primary_building.get("footprint_area_square_meters") or 0)
    centroid_within_match_envelope = bool(
        primary_building.get("centroid_within_match_envelope")
    )
    candidate_distance_m = float(primary_building.get("distance_m") or 9999)
    max_distance_m = max(float(envelope.get("width_m") or 0) * 0.55, 28.0)
    if gross_area_square_meters < 25 or (
        not centroid_within_match_envelope and candidate_distance_m > max_distance_m
    ):
        return _build_unavailable_roof_capacity_context(
            "Nearby building footprints were found, but none were confident enough to stand in for the main roof."
        )

    usable_roof_area_square_meters = gross_area_square_meters * ROOF_COVERAGE_FACTOR
    usable_roof_area_square_feet = usable_roof_area_square_meters * SQ_METERS_TO_SQ_FEET
    recommended_system_size_kw = _round(
        _clamp(
            usable_roof_area_square_meters * BASELINE_PANEL_EFFICIENCY,
            1.5,
            18.0,
        ),
        2,
    )

    confidence_score = 0
    if centroid_within_match_envelope:
        confidence_score += 2
    if _is_primary_roof_candidate_kind(primary_building.get("kind")):
        confidence_score += 1
    if match_quality == "high":
        confidence_score += 2
    elif match_quality == "medium":
        confidence_score += 1
    if gross_area_square_meters >= 110:
        confidence_score += 1

    confidence = (
        "high"
        if confidence_score >= 5
        else "medium"
        if confidence_score >= 3
        else "low"
    )

    return {
        "available": True,
        "candidate_building_id": primary_building.get("id"),
        "candidate_building_name": primary_building.get("name"),
        "candidate_building_kind": primary_building.get("kind"),
        "candidate_building_distance_m": _round(candidate_distance_m, 1),
        "centroid_within_match_envelope": centroid_within_match_envelope,
        "gross_footprint_area_square_meters": _round(gross_area_square_meters, 1),
        "gross_footprint_area_square_feet": _round(
            gross_area_square_meters * SQ_METERS_TO_SQ_FEET,
            1,
        ),
        "usable_roof_area_square_meters": _round(usable_roof_area_square_meters, 1),
        "usable_roof_area_square_feet": _round(usable_roof_area_square_feet, 1),
        "recommended_system_size_kw": recommended_system_size_kw,
        "dominant_edge_bearing": primary_building.get("dominant_edge_bearing"),
        "dominant_edge_length_m": primary_building.get("dominant_edge_length_m"),
        "confidence": confidence,
        "summary": (
            f"Matched building footprint suggests about {round(gross_area_square_meters * SQ_METERS_TO_SQ_FEET):,} sq ft "
            f"of gross roof area, roughly {round(usable_roof_area_square_feet):,} sq ft of usable solar area, "
            f"and about {recommended_system_size_kw:.1f} kW of baseline capacity."
        ),
        "model_note": (
            "Roof-capacity inference is conservative and uses the matched building footprint, a "
            f"{ROOF_COVERAGE_FACTOR * 100:.0f}% usable-roof allowance, and "
            f"{BASELINE_PANEL_EFFICIENCY * 100:.0f}% baseline panel efficiency."
        ),
    }


def _build_canopy_context(latitude: float, longitude: float, envelope: dict[str, Any]):
    radius_m = int(_clamp(max(envelope.get("width_m") or 0, envelope.get("height_m") or 0) * 1.05, 35, 85))
    try:
        payload = _build_overpass_canopy(latitude, longitude, radius_m)
    except requests.RequestException:
        return {
            "source": "openstreetmap-overpass",
            "search_radius_m": radius_m,
            "canopy_count": 0,
            "nearby_canopy": [],
            "nearest_canopy": None,
            "directional_pressure": {
                "north": 0.0,
                "south": 0.0,
                "east": 0.0,
                "west": 0.0,
            },
            "summary": "Mapped canopy context is unavailable for this property right now.",
        }

    directional_scores = {"north": 0.0, "south": 0.0, "east": 0.0, "west": 0.0}
    nearby_canopy = []

    for element in payload.get("elements") or []:
        tags = element.get("tags") or {}
        centroid = None
        geometry = None

        if element.get("type") == "node":
            if element.get("lat") is None or element.get("lon") is None:
                continue
            centroid = {
                "lat": round(float(element.get("lat")), 6),
                "lng": round(float(element.get("lon")), 6),
            }
            geometry = _build_point_geometry(centroid)
        else:
            geometry_points = [
                {
                    "lat": float(point.get("lat")),
                    "lng": float(point.get("lon")),
                }
                for point in (element.get("geometry") or [])
                if point.get("lat") is not None and point.get("lon") is not None
            ]
            centroid = (
                {
                    "lat": round(float((element.get("center") or {}).get("lat")), 6),
                    "lng": round(float((element.get("center") or {}).get("lon")), 6),
                }
                if (element.get("center") or {}).get("lat") is not None
                and (element.get("center") or {}).get("lon") is not None
                else _polygon_centroid(geometry_points)
            )
            geometry = _build_polygon_geometry(geometry_points) if len(geometry_points) >= 3 else None

        if not centroid:
            continue

        distance_m = _haversine_meters(latitude, longitude, centroid["lat"], centroid["lng"])
        if distance_m > radius_m * 1.15:
            continue

        bearing = _bearing_degrees(latitude, longitude, centroid["lat"], centroid["lng"])
        direction_bucket = _direction_bucket(bearing)
        direction_group = _direction_group(direction_bucket)
        canopy_height_m = _parse_canopy_height_meters(tags)
        canopy_pressure = min((canopy_height_m / max(distance_m, 5)) * 0.58, 1.8)
        directional_scores[direction_group] = directional_scores.get(direction_group, 0.0) + canopy_pressure

        nearby_canopy.append(
            {
                "id": f"osm-{element.get('type')}-{element.get('id')}",
                "name": tags.get("name") or tags.get("species") or "Nearby canopy",
                "kind": tags.get("natural") or tags.get("landuse") or "vegetation",
                "height_m": _round(canopy_height_m, 1),
                "distance_m": _round(distance_m, 1),
                "bearing_degrees": _round(bearing, 0),
                "direction_bucket": direction_bucket,
                "direction_group": direction_group,
                "canopy_pressure": _round(canopy_pressure, 2),
                "centroid": centroid,
                "geometry": geometry,
            }
        )

    nearby_canopy.sort(
        key=lambda feature: (
            -(feature.get("canopy_pressure") or 0),
            feature.get("distance_m") or 9999,
        )
    )
    nearby_canopy = nearby_canopy[:8]
    nearest_canopy = (
        min(nearby_canopy, key=lambda feature: feature.get("distance_m") or 9999)
        if nearby_canopy
        else None
    )
    strongest_direction = max(directional_scores, key=directional_scores.get)

    if not nearby_canopy:
        summary = "No nearby mapped canopy features were found in the current planning radius."
    else:
        summary = (
            f"{len(nearby_canopy)} nearby canopy features found. "
            f"Nearest canopy is about {nearest_canopy.get('distance_m', 0):.0f} m away, "
            f"with the strongest canopy pressure on the {strongest_direction} side."
        )

    return {
        "source": "openstreetmap-overpass",
        "search_radius_m": radius_m,
        "canopy_count": len(nearby_canopy),
        "nearby_canopy": nearby_canopy,
        "nearest_canopy": nearest_canopy,
        "directional_pressure": {
            direction: _round(score, 2)
            for direction, score in directional_scores.items()
        },
        "summary": summary,
    }


def _fetch_terrain_samples(samples: list[dict[str, Any]]):
    locations = "|".join(f"{sample['lat']:.6f},{sample['lng']:.6f}" for sample in samples)
    return _fetch_json(
        OPEN_TOPO_DATA_URL,
        params={"locations": locations},
        ttl_seconds=86400,
    )


def _build_terrain_context(latitude: float, longitude: float, envelope: dict[str, Any]):
    sample_radius_m = int(_clamp(max(envelope.get("width_m") or 0, envelope.get("height_m") or 0) * 0.55, 24, 42))
    samples = [
        {"id": "center", "lat": round(latitude, 6), "lng": round(longitude, 6)},
        {"id": "north", **_offset_coordinate(latitude, longitude, north_meters=sample_radius_m)},
        {"id": "south", **_offset_coordinate(latitude, longitude, north_meters=-sample_radius_m)},
        {"id": "east", **_offset_coordinate(latitude, longitude, east_meters=sample_radius_m)},
        {"id": "west", **_offset_coordinate(latitude, longitude, east_meters=-sample_radius_m)},
    ]
    payload = _fetch_terrain_samples(samples)
    elevations_by_id = {}
    enriched_samples = []

    for sample, result in zip(samples, payload.get("results") or []):
        elevation = _safe_float(result.get("elevation"))
        if elevation is None:
            continue
        elevations_by_id[sample["id"]] = elevation
        enriched_samples.append(
            {
                "id": sample["id"],
                "lat": sample["lat"],
                "lng": sample["lng"],
                "elevation_m": _round(elevation, 1),
            }
        )

    center_elevation = elevations_by_id.get("center")
    north_elevation = elevations_by_id.get("north")
    south_elevation = elevations_by_id.get("south")
    east_elevation = elevations_by_id.get("east")
    west_elevation = elevations_by_id.get("west")
    if not elevations_by_id:
        return {
            "source": "opentopodata-srtm90m",
            "summary": "Terrain context is unavailable for this property right now.",
        }

    relief_m = max(elevations_by_id.values()) - min(elevations_by_id.values())
    north_south_difference = (north_elevation or center_elevation or 0) - (south_elevation or center_elevation or 0)
    east_west_difference = (east_elevation or center_elevation or 0) - (west_elevation or center_elevation or 0)
    baseline_m = max(sample_radius_m * 2, 1)

    dominant_difference = north_south_difference
    dominant_aspect = "flat"
    if abs(north_south_difference) >= abs(east_west_difference) and abs(north_south_difference) >= 1.5:
        dominant_aspect = "south-facing" if north_south_difference > 0 else "north-facing"
    elif abs(east_west_difference) >= 1.5:
        dominant_aspect = "west-facing" if east_west_difference > 0 else "east-facing"
        dominant_difference = east_west_difference

    slope_percent = abs(dominant_difference) / baseline_m * 100
    terrain_class = _classify_terrain(slope_percent)

    return {
        "source": "opentopodata-srtm90m",
        "center_elevation_m": _round(center_elevation, 1),
        "local_relief_m": _round(relief_m, 1),
        "dominant_aspect": dominant_aspect,
        "slope_percent": _round(slope_percent, 1),
        "terrain_class": terrain_class,
        "sample_radius_m": sample_radius_m,
        "samples": enriched_samples,
        "summary": (
            f"Local terrain reads as {terrain_class} with about {relief_m:.0f} m of relief in the immediate "
            f"planning radius and a {dominant_aspect} bias."
        ),
    }


def _build_shade_context(
    building_context: dict[str, Any],
    terrain_context: dict[str, Any],
    canopy_context: dict[str, Any],
):
    directional_pressure = building_context.get("directional_pressure") or {}
    canopy_pressure = canopy_context.get("directional_pressure") or {}
    south_pressure = directional_pressure.get("south", 0) or 0
    east_west_pressure = (directional_pressure.get("east", 0) or 0) + (directional_pressure.get("west", 0) or 0)
    south_canopy_pressure = canopy_pressure.get("south", 0) or 0
    east_west_canopy_pressure = (canopy_pressure.get("east", 0) or 0) + (canopy_pressure.get("west", 0) or 0)
    terrain_aspect = terrain_context.get("dominant_aspect") or "flat"

    building_pressure_score = south_pressure * 0.8 + east_west_pressure * 0.28
    canopy_pressure_score = south_canopy_pressure * 0.58 + east_west_canopy_pressure * 0.22
    combined_pressure = building_pressure_score + canopy_pressure_score
    obstruction_risk = _classify_pressure(combined_pressure)
    if terrain_aspect == "north-facing":
        terrain_bias = "less solar-favored"
    elif terrain_aspect == "south-facing":
        terrain_bias = "more solar-favored"
    else:
        terrain_bias = "mostly neutral"

    return {
        "obstruction_risk": obstruction_risk,
        "terrain_bias": terrain_bias,
        "building_pressure_score": _round(building_pressure_score, 2),
        "canopy_pressure_score": _round(canopy_pressure_score, 2),
        "summary": (
            f"Combined structure and canopy shade risk reads as {obstruction_risk}, with terrain looking "
            f"{terrain_bias} for open-sky light."
        ),
    }


def get_property_context_snapshot(
    latitude: float,
    longitude: float,
    bounds: Optional[dict[str, Any]] = None,
    match_quality: Optional[str] = None,
):
    envelope = _build_context_envelope(latitude, longitude, bounds)
    building_context = _build_building_context(latitude, longitude, envelope)
    terrain_context = _build_terrain_context(latitude, longitude, envelope)
    canopy_context = _build_canopy_context(latitude, longitude, envelope)
    parcel_context = _build_parcel_context(latitude, envelope, building_context, canopy_context, terrain_context)
    shade_context = _build_shade_context(building_context, terrain_context, canopy_context)
    roof_capacity_context = _build_roof_capacity_context(
        envelope,
        building_context,
        match_quality,
    )

    summary = (
        f"{building_context.get('summary')} {canopy_context.get('summary')} {terrain_context.get('summary')} "
        f"{parcel_context.get('summary')} {shade_context.get('summary')}"
    )
    if roof_capacity_context.get("available"):
        summary = f"{summary} {roof_capacity_context.get('summary')}"

    return {
        "context_version": "property-context-v3",
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "match_quality": match_quality or "unknown",
        "match_envelope": envelope,
        "parcel_context": parcel_context,
        "building_context": building_context,
        "canopy_context": canopy_context,
        "terrain_context": terrain_context,
        "shade_context": shade_context,
        "roof_capacity_context": roof_capacity_context,
        "summary": summary,
        "model_note": (
            "This context layer uses nearby OpenStreetMap building and vegetation features plus SRTM terrain samples "
            "and an inset planning core derived from the address match envelope. It still does not include "
            "parcel-certified boundaries, fence lines, or tree-perfect canopy geometry."
        ),
    }
