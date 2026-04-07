import json
import logging
import os
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from garden_crop_catalog_seed import GARDEN_CROP_CATALOG_SEED

logger = logging.getLogger(__name__)

_personal_info_memory = {}
_browser_data_memory = {}
_solar_data_memory = {}
_property_record_memory = {}
_geocode_cache_memory = {}
_garden_crop_catalog_memory = {}
_property_climate_snapshot_memory = {}
_UNSET = object()
_RECENT_CACHE_DAYS = 30


def _stored_at_value():
    return datetime.now().strftime("%Y-%m-%d")


def _property_record_stored_at_value():
    return datetime.now().isoformat()


def _reference_data_stored_at_value():
    return datetime.now().isoformat()


def _is_recent(date_value):
    try:
        return datetime.strptime(date_value, "%Y-%m-%d") > (
            datetime.now() - timedelta(days=_RECENT_CACHE_DAYS)
        )
    except (TypeError, ValueError):
        return False


def _normalize_address_part(value):
    return " ".join(str(value or "").strip().lower().split())


def build_address_lookup_key(address):
    return "|".join(
        _normalize_address_part(address.get(field, ""))
        for field in ("street", "city", "state", "zip", "country")
    )


def build_coordinate_lookup_key(latitude, longitude):
    return f"{round(float(latitude), 6):.6f},{round(float(longitude), 6):.6f}"


def _runtime_db_path():
    raw_path = os.getenv("APP_DB_PATH") or ".runtime/solar-potential.sqlite3"
    if raw_path == ":memory:":
        return raw_path

    path = Path(raw_path)
    if not path.is_absolute():
        path = Path(__file__).resolve().parent / path

    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def _connect():
    path = _runtime_db_path()
    connection = sqlite3.connect(path)
    connection.row_factory = sqlite3.Row
    _initialize_db(connection)
    return connection


def _initialize_db(connection):
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS property_records (
            guid TEXT PRIMARY KEY,
            address_lookup_key TEXT NOT NULL,
            address_json TEXT NOT NULL,
            property_preview_json TEXT,
            property_context_json TEXT,
            property_climate_json TEXT,
            roof_selection_json TEXT,
            garden_zones_json TEXT NOT NULL,
            saved_solar_reports_json TEXT NOT NULL,
            stored_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_property_records_address_lookup_key
            ON property_records(address_lookup_key);

        CREATE TABLE IF NOT EXISTS browser_data (
            guid TEXT NOT NULL,
            browser_data_json TEXT NOT NULL,
            ip_address TEXT,
            stored_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_browser_data_guid
            ON browser_data(guid);

        CREATE TABLE IF NOT EXISTS solar_data (
            guid TEXT PRIMARY KEY,
            zip_code TEXT,
            solar_data_json TEXT NOT NULL,
            time_zone TEXT,
            data_source TEXT,
            address_json TEXT,
            stored_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_solar_data_zip_code
            ON solar_data(zip_code, stored_at);

        CREATE TABLE IF NOT EXISTS geocode_cache (
            cache_key TEXT PRIMARY KEY,
            query_type TEXT NOT NULL,
            source TEXT,
            response_json TEXT NOT NULL,
            stored_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_geocode_cache_query_type
            ON geocode_cache(query_type, stored_at);

        CREATE TABLE IF NOT EXISTS garden_crop_catalogs (
            catalog_id TEXT PRIMARY KEY,
            version TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            stored_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS property_climate_snapshots (
            coordinate_lookup_key TEXT PRIMARY KEY,
            climate_json TEXT NOT NULL,
            stored_at TEXT NOT NULL
        );
        """
    )
    columns = {
        row["name"]
        for row in connection.execute("PRAGMA table_info(property_records)").fetchall()
    }
    if "property_context_json" not in columns:
        connection.execute("ALTER TABLE property_records ADD COLUMN property_context_json TEXT")
    if "property_climate_json" not in columns:
        connection.execute("ALTER TABLE property_records ADD COLUMN property_climate_json TEXT")
    _seed_garden_crop_catalog(connection)
    connection.commit()


def _json_dump(value):
    return json.dumps(value) if value is not None else None


def _json_load(value, default=None):
    if value in (None, ""):
        return default

    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def _build_property_record_from_row(row):
    return {
        "guid": row["guid"],
        "address": _json_load(row["address_json"], default={}) or {},
        "property_preview": _json_load(row["property_preview_json"]),
        "property_context": _json_load(row["property_context_json"]),
        "property_climate": _json_load(row["property_climate_json"]),
        "roof_selection": _json_load(row["roof_selection_json"]),
        "stored_at": row["stored_at"],
        "garden_zones": _json_load(row["garden_zones_json"], default=[]) or [],
        "saved_solar_reports": _json_load(row["saved_solar_reports_json"], default=[]) or [],
    }


def _remember_property_record(record):
    guid = record.get("guid")
    if not guid:
        return

    _property_record_memory.pop(guid, None)
    _property_record_memory[guid] = record


def _garden_crop_catalog_seed_payload():
    return json.loads(json.dumps(GARDEN_CROP_CATALOG_SEED))


def _remember_garden_crop_catalog(payload):
    catalog_id = payload.get("catalog_id")
    if not catalog_id:
        return

    _garden_crop_catalog_memory.pop(catalog_id, None)
    _garden_crop_catalog_memory[catalog_id] = payload


def _write_garden_crop_catalog(connection, payload):
    stored_at = payload.get("stored_at") or _reference_data_stored_at_value()
    normalized_payload = {
        **payload,
        "stored_at": stored_at,
    }
    connection.execute(
        """
        INSERT INTO garden_crop_catalogs (
            catalog_id,
            version,
            payload_json,
            stored_at
        )
        VALUES (?, ?, ?, ?)
        ON CONFLICT(catalog_id) DO UPDATE SET
            version = excluded.version,
            payload_json = excluded.payload_json,
            stored_at = excluded.stored_at
        """,
        (
            normalized_payload["catalog_id"],
            normalized_payload["version"],
            json.dumps(normalized_payload),
            stored_at,
        ),
    )
    _remember_garden_crop_catalog(normalized_payload)


def _seed_garden_crop_catalog(connection):
    seed_payload = _garden_crop_catalog_seed_payload()
    row = connection.execute(
        """
        SELECT version
        FROM garden_crop_catalogs
        WHERE catalog_id = ?
        """,
        (seed_payload["catalog_id"],),
    ).fetchone()
    if row and row["version"] == seed_payload["version"]:
        return

    _write_garden_crop_catalog(connection, seed_payload)


def _write_property_record(
    connection,
    guid,
    address,
    property_preview,
    property_context,
    property_climate,
    roof_selection,
    garden_zones,
    saved_solar_reports,
):
    stored_at = _property_record_stored_at_value()
    connection.execute(
        """
        INSERT INTO property_records (
            guid,
            address_lookup_key,
            address_json,
            property_preview_json,
            property_context_json,
            property_climate_json,
            roof_selection_json,
            garden_zones_json,
            saved_solar_reports_json,
            stored_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(guid) DO UPDATE SET
            address_lookup_key = excluded.address_lookup_key,
            address_json = excluded.address_json,
            property_preview_json = excluded.property_preview_json,
            property_context_json = excluded.property_context_json,
            property_climate_json = excluded.property_climate_json,
            roof_selection_json = excluded.roof_selection_json,
            garden_zones_json = excluded.garden_zones_json,
            saved_solar_reports_json = excluded.saved_solar_reports_json,
            stored_at = excluded.stored_at
        """,
        (
            guid,
            build_address_lookup_key(address),
            json.dumps(address),
            _json_dump(property_preview),
            _json_dump(property_context),
            _json_dump(property_climate),
            _json_dump(roof_selection),
            json.dumps(garden_zones or []),
            json.dumps(saved_solar_reports or []),
            stored_at,
        ),
    )
    connection.commit()

    _personal_info_memory[guid] = dict(address)
    _remember_property_record({
        "guid": guid,
        "address": dict(address),
        "property_preview": property_preview,
        "property_context": property_context,
        "property_climate": property_climate,
        "roof_selection": roof_selection,
        "garden_zones": garden_zones or [],
        "saved_solar_reports": saved_solar_reports or [],
        "stored_at": stored_at,
    })


def reset_memory_storage():
    _personal_info_memory.clear()
    _browser_data_memory.clear()
    _solar_data_memory.clear()
    _property_record_memory.clear()
    _geocode_cache_memory.clear()
    _garden_crop_catalog_memory.clear()
    _property_climate_snapshot_memory.clear()

    try:
        with _connect() as connection:
            connection.execute("DELETE FROM browser_data")
            connection.execute("DELETE FROM solar_data")
            connection.execute("DELETE FROM geocode_cache")
            connection.execute("DELETE FROM property_records")
            connection.execute("DELETE FROM garden_crop_catalogs")
            connection.execute("DELETE FROM property_climate_snapshots")
            connection.commit()
    except sqlite3.Error as exc:
        logger.warning("Unable to reset SQLite persistence: %s", str(exc))


def get_property_record(guid):
    try:
        with _connect() as connection:
            row = connection.execute(
                "SELECT * FROM property_records WHERE guid = ?",
                (guid,),
            ).fetchone()
        if row:
            return _build_property_record_from_row(row)
    except sqlite3.Error as exc:
        logger.warning("Property lookup fell back to memory: %s", str(exc))

    return _property_record_memory.get(guid)


def find_property_record_by_address(address):
    lookup_key = build_address_lookup_key(address)
    if not lookup_key:
        return None

    try:
        with _connect() as connection:
            row = connection.execute(
                """
                SELECT * FROM property_records
                WHERE address_lookup_key = ?
                ORDER BY stored_at DESC
                LIMIT 1
                """,
                (lookup_key,),
            ).fetchone()
        if row:
            return _build_property_record_from_row(row)
    except sqlite3.Error as exc:
        logger.warning("Property address lookup fell back to memory: %s", str(exc))

    records = list(_property_record_memory.values())
    for record in reversed(records):
        if build_address_lookup_key(record.get("address", {})) == lookup_key:
            return record

    return None


def list_property_records(limit=8, require_garden_zones=False):
    try:
        normalized_limit = max(1, int(limit or 8))
    except (TypeError, ValueError):
        normalized_limit = 8

    try:
        with _connect() as connection:
            query = """
                SELECT * FROM property_records
            """
            parameters = []
            if require_garden_zones:
                query += """
                WHERE garden_zones_json IS NOT NULL
                  AND garden_zones_json != '[]'
                """
            query += """
                ORDER BY stored_at DESC
                LIMIT ?
            """
            parameters.append(normalized_limit)
            rows = connection.execute(query, parameters).fetchall()
        return [_build_property_record_from_row(row) for row in rows]
    except sqlite3.Error as exc:
        logger.warning("Property listing fell back to memory: %s", str(exc))

    records = list(_property_record_memory.values())
    if require_garden_zones:
        records = [record for record in records if record.get("garden_zones")]

    return list(reversed(records))[:normalized_limit]


def get_garden_crop_catalog(catalog_id="default"):
    try:
        with _connect() as connection:
            row = connection.execute(
                """
                SELECT payload_json
                FROM garden_crop_catalogs
                WHERE catalog_id = ?
                """,
                (catalog_id,),
            ).fetchone()
        if row:
            payload = _json_load(row["payload_json"], default={}) or {}
            if payload:
                _remember_garden_crop_catalog(payload)
                return payload
    except sqlite3.Error as exc:
        logger.warning("Garden crop catalog lookup fell back to memory: %s", str(exc))

    payload = _garden_crop_catalog_memory.get(catalog_id)
    if payload:
        return payload

    seed_payload = _garden_crop_catalog_seed_payload()
    if seed_payload.get("catalog_id") == catalog_id:
        _remember_garden_crop_catalog(seed_payload)
        return seed_payload

    return None


def find_solar_quote(quote_id):
    if not quote_id:
        return None

    try:
        with _connect() as connection:
            rows = connection.execute(
                "SELECT * FROM property_records ORDER BY stored_at DESC"
            ).fetchall()
        for row in rows:
            record = _build_property_record_from_row(row)
            for report in record.get("saved_solar_reports", []):
                quote = report.get("homeowner_quote")
                if quote and quote.get("id") == quote_id:
                    return {
                        "record": record,
                        "report": report,
                        "quote": quote,
                    }
    except sqlite3.Error as exc:
        logger.warning("Quote lookup fell back to memory: %s", str(exc))

    for record in reversed(list(_property_record_memory.values())):
        for report in record.get("saved_solar_reports", []):
            quote = report.get("homeowner_quote")
            if quote and quote.get("id") == quote_id:
                return {
                    "record": record,
                    "report": report,
                    "quote": quote,
                }

    return None


def store_personal_info(guid, address):
    existing_record = get_property_record(guid) or {}
    try:
        with _connect() as connection:
            _write_property_record(
                connection,
                guid,
                address,
                existing_record.get("property_preview"),
                existing_record.get("property_context"),
                existing_record.get("property_climate"),
                existing_record.get("roof_selection"),
                existing_record.get("garden_zones") or [],
                existing_record.get("saved_solar_reports") or [],
            )
        return
    except sqlite3.Error as exc:
        logger.warning("Storing address fell back to memory: %s", str(exc))

    stored_at = _property_record_stored_at_value()
    _personal_info_memory[guid] = dict(address)
    _remember_property_record({
        "guid": guid,
        "address": dict(address),
        "property_preview": existing_record.get("property_preview"),
        "property_context": existing_record.get("property_context"),
        "property_climate": existing_record.get("property_climate"),
        "roof_selection": existing_record.get("roof_selection"),
        "garden_zones": existing_record.get("garden_zones") or [],
        "saved_solar_reports": existing_record.get("saved_solar_reports") or [],
        "stored_at": stored_at,
    })


def upsert_property_record(
    guid,
    address,
    property_preview=None,
    property_context=_UNSET,
    property_climate=_UNSET,
    roof_selection=None,
    garden_zones=_UNSET,
    saved_solar_reports=_UNSET,
):
    existing_record = get_property_record(guid) or {}
    if property_context is _UNSET:
        property_context_to_store = existing_record.get("property_context")
    else:
        property_context_to_store = property_context
    if property_climate is _UNSET:
        property_climate_to_store = existing_record.get("property_climate")
    else:
        property_climate_to_store = property_climate
    if garden_zones is _UNSET:
        garden_zones_to_store = existing_record.get("garden_zones") or []
    else:
        garden_zones_to_store = garden_zones or []

    if saved_solar_reports is _UNSET:
        saved_solar_reports_to_store = existing_record.get("saved_solar_reports") or []
    else:
        saved_solar_reports_to_store = saved_solar_reports or []

    try:
        with _connect() as connection:
            _write_property_record(
                connection,
                guid,
                address,
                property_preview,
                property_context_to_store,
                property_climate_to_store,
                roof_selection,
                garden_zones_to_store,
                saved_solar_reports_to_store,
            )
        return
    except sqlite3.Error as exc:
        logger.warning("Property upsert fell back to memory: %s", str(exc))

    stored_at = _property_record_stored_at_value()
    _personal_info_memory[guid] = dict(address)
    _remember_property_record({
        "guid": guid,
        "address": dict(address),
        "property_preview": property_preview,
        "property_context": property_context_to_store,
        "property_climate": property_climate_to_store,
        "roof_selection": roof_selection,
        "garden_zones": garden_zones_to_store,
        "saved_solar_reports": saved_solar_reports_to_store,
        "stored_at": stored_at,
    })


def store_browser_data(guid, browser_data, ip_address):
    stored_at = _stored_at_value()
    try:
        with _connect() as connection:
            connection.execute(
                """
                INSERT INTO browser_data (guid, browser_data_json, ip_address, stored_at)
                VALUES (?, ?, ?, ?)
                """,
                (guid, json.dumps(browser_data), ip_address, stored_at),
            )
            connection.commit()
        return
    except sqlite3.Error as exc:
        logger.warning("Browser data persistence fell back to memory: %s", str(exc))

    _browser_data_memory[guid] = {
        **browser_data,
        "ipAddress": ip_address,
        "storedAt": stored_at,
    }


def store_solar_data(guid, solar_data, time_zone, address, data_source):
    stored_at = _stored_at_value()
    try:
        with _connect() as connection:
            connection.execute(
                """
                INSERT INTO solar_data (
                    guid,
                    zip_code,
                    solar_data_json,
                    time_zone,
                    data_source,
                    address_json,
                    stored_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(guid) DO UPDATE SET
                    zip_code = excluded.zip_code,
                    solar_data_json = excluded.solar_data_json,
                    time_zone = excluded.time_zone,
                    data_source = excluded.data_source,
                    address_json = excluded.address_json,
                    stored_at = excluded.stored_at
                """,
                (
                    guid,
                    address.get("zip", ""),
                    json.dumps(solar_data),
                    time_zone,
                    data_source,
                    json.dumps(address),
                    stored_at,
                ),
            )
            connection.commit()
        return
    except sqlite3.Error as exc:
        logger.warning("Solar data persistence fell back to memory: %s", str(exc))

    _solar_data_memory[guid] = {
        "solar_data": dict(solar_data),
        "time_zone": time_zone,
        "address": dict(address),
        "data_source": data_source,
        "stored_at": stored_at,
    }


def check_existing_address_data(guid):
    property_record = get_property_record(guid)
    if property_record:
        logger.info("Address data found for GUID %s", guid)
        return property_record["address"]

    logger.info("No address data found for GUID %s", guid)
    return _personal_info_memory.get(guid)


def check_existing_solar_data(guid):
    try:
        with _connect() as connection:
            row = connection.execute(
                "SELECT solar_data_json, time_zone, stored_at FROM solar_data WHERE guid = ?",
                (guid,),
            ).fetchone()
        if row and _is_recent(row["stored_at"]):
            return _json_load(row["solar_data_json"], default={}), row["time_zone"]
    except sqlite3.Error as exc:
        logger.warning("Solar lookup fell back to memory: %s", str(exc))

    cached = _solar_data_memory.get(guid)
    if cached and _is_recent(cached.get("stored_at")):
        return cached["solar_data"], cached["time_zone"]

    return None, None


def check_existing_zip_data(zip_code):
    if not zip_code:
        return None, None

    try:
        with _connect() as connection:
            row = connection.execute(
                """
                SELECT solar_data_json, time_zone, stored_at
                FROM solar_data
                WHERE zip_code = ?
                ORDER BY stored_at DESC
                LIMIT 1
                """,
                (zip_code,),
            ).fetchone()
        if row and _is_recent(row["stored_at"]):
            return _json_load(row["solar_data_json"], default={}), row["time_zone"]
    except sqlite3.Error as exc:
        logger.warning("ZIP solar lookup fell back to memory: %s", str(exc))

    for guid, address in _personal_info_memory.items():
        if address.get("zip") != zip_code:
            continue
        cached = _solar_data_memory.get(guid)
        if cached and _is_recent(cached.get("stored_at")):
            return cached["solar_data"], cached["time_zone"]

    return None, None


def get_cached_property_climate(latitude, longitude):
    if latitude is None or longitude is None:
        return None

    coordinate_lookup_key = build_coordinate_lookup_key(latitude, longitude)
    try:
        with _connect() as connection:
            row = connection.execute(
                """
                SELECT climate_json, stored_at
                FROM property_climate_snapshots
                WHERE coordinate_lookup_key = ?
                """,
                (coordinate_lookup_key,),
            ).fetchone()
        if row and _is_recent(row["stored_at"]):
            return _json_load(row["climate_json"], default={})
    except sqlite3.Error as exc:
        logger.warning("Property climate lookup fell back to memory: %s", str(exc))

    cached = _property_climate_snapshot_memory.get(coordinate_lookup_key)
    if cached and _is_recent(cached.get("stored_at")):
        return cached.get("climate")

    return None


def store_cached_property_climate(latitude, longitude, climate):
    if latitude is None or longitude is None or climate is None:
        return

    coordinate_lookup_key = build_coordinate_lookup_key(latitude, longitude)
    stored_at = _stored_at_value()
    try:
        with _connect() as connection:
            connection.execute(
                """
                INSERT INTO property_climate_snapshots (coordinate_lookup_key, climate_json, stored_at)
                VALUES (?, ?, ?)
                ON CONFLICT(coordinate_lookup_key) DO UPDATE SET
                    climate_json = excluded.climate_json,
                    stored_at = excluded.stored_at
                """,
                (coordinate_lookup_key, json.dumps(climate), stored_at),
            )
            connection.commit()
        return
    except sqlite3.Error as exc:
        logger.warning("Property climate persistence fell back to memory: %s", str(exc))

    _property_climate_snapshot_memory[coordinate_lookup_key] = {
        "climate": climate,
        "stored_at": stored_at,
    }


def get_geocode_cache(query_type, cache_key):
    if not query_type or not cache_key:
        return None

    composite_key = f"{query_type}:{cache_key}"
    try:
        with _connect() as connection:
            row = connection.execute(
                """
                SELECT response_json, stored_at
                FROM geocode_cache
                WHERE cache_key = ? AND query_type = ?
                """,
                (composite_key, query_type),
            ).fetchone()
        if row and _is_recent(row["stored_at"]):
            return _json_load(row["response_json"], default={})
    except sqlite3.Error as exc:
        logger.warning("Geocode cache lookup fell back to memory: %s", str(exc))

    cached = _geocode_cache_memory.get(composite_key)
    if cached and _is_recent(cached.get("stored_at")):
        return cached.get("response")

    return None


def store_geocode_cache(query_type, cache_key, response, source=None):
    if not query_type or not cache_key or response is None:
        return

    composite_key = f"{query_type}:{cache_key}"
    stored_at = _stored_at_value()
    try:
        with _connect() as connection:
            connection.execute(
                """
                INSERT INTO geocode_cache (cache_key, query_type, source, response_json, stored_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    query_type = excluded.query_type,
                    source = excluded.source,
                    response_json = excluded.response_json,
                    stored_at = excluded.stored_at
                """,
                (composite_key, query_type, source, json.dumps(response), stored_at),
            )
            connection.commit()
        return
    except sqlite3.Error as exc:
        logger.warning("Geocode cache persistence fell back to memory: %s", str(exc))

    _geocode_cache_memory[composite_key] = {
        "response": response,
        "source": source,
        "stored_at": stored_at,
    }
