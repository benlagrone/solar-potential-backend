import copy
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import data_persistence
import main


def build_address():
    return {
        "street": "123 Main St",
        "city": "Austin",
        "state": "TX",
        "zip": "78702",
        "country": "United States",
    }


def build_property_preview():
    return {
        "query": "123 Main St, Austin, TX 78702, United States",
        "formatted_address": "123 Main St, Austin, TX 78702, United States",
        "latitude": 30.2672,
        "longitude": -97.7431,
        "bounds": {
            "south": 30.2661,
            "north": 30.2683,
            "west": -97.7443,
            "east": -97.7419,
        },
        "source": "test",
        "match_quality": "high",
        "match_score": 31,
        "address": build_address(),
    }


def build_roof_selection():
    return {
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-97.7435, 30.2676],
                [-97.7430, 30.2676],
                [-97.7430, 30.2672],
                [-97.7435, 30.2672],
                [-97.7435, 30.2676],
            ]],
        },
        "centroid": {"lat": 30.2674, "lng": -97.74325},
        "areaSquareMeters": 92.9,
        "areaSquareFeet": 1000,
        "recommendedKw": 10.5,
    }


def build_west_facing_roof_selection():
    return {
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [-97.7434, 30.2678],
                [-97.7431, 30.2678],
                [-97.7431, 30.2670],
                [-97.7434, 30.2670],
                [-97.7434, 30.2678],
            ]],
        },
        "centroid": {"lat": 30.2674, "lng": -97.74325},
        "areaSquareMeters": 84.0,
        "areaSquareFeet": 904,
        "recommendedKw": 9.5,
    }


def build_garden_zone(index=1):
    west = -97.7438 + (index * 0.00045)
    east = west + 0.00032
    south = 30.2670 + (index * 0.00018)
    north = south + 0.00028

    return {
        "id": f"garden-zone-{index}",
        "name": f"Garden zone {index}",
        "geometry": {
            "type": "Polygon",
            "coordinates": [[
                [west, north],
                [east, north],
                [east, south],
                [west, south],
                [west, north],
            ]],
        },
        "centroid": {"lat": round((north + south) / 2, 6), "lng": round((west + east) / 2, 6)},
        "areaSquareMeters": 18.4 + index,
        "areaSquareFeet": 198 + (index * 12),
    }


def build_garden_zones():
    return [build_garden_zone(1), build_garden_zone(2)]


def build_property_context():
    return {
        "context_version": "property-context-v1",
        "match_envelope": {
            "bounds": {
                "south": 30.2668,
                "north": 30.2676,
                "west": -97.7437,
                "east": -97.7426,
            },
            "width_m": 84.2,
            "height_m": 64.1,
            "source": "geocoder-match-envelope",
            "label": "Geocoder match envelope",
        },
        "building_context": {
            "source": "openstreetmap-overpass",
            "building_count": 2,
            "obstruction_risk": "moderate",
            "nearby_buildings": [
                {
                    "id": "osm-way-1",
                    "name": "Nearby building",
                    "kind": "office",
                    "height_m": 21.0,
                    "distance_m": 18.4,
                    "direction_bucket": "south",
                    "direction_group": "south",
                    "shadow_pressure": 1.14,
                    "obstruction_risk": "high",
                    "centroid": {"lat": 30.2670, "lng": -97.7430},
                    "geometry": {
                        "type": "Polygon",
                        "coordinates": [[
                            [-97.7431, 30.2671],
                            [-97.7429, 30.2671],
                            [-97.7429, 30.2669],
                            [-97.7431, 30.2669],
                            [-97.7431, 30.2671],
                        ]],
                    },
                }
            ],
            "directional_pressure": {
                "north": 0.2,
                "south": 1.14,
                "east": 0.1,
                "west": 0.0,
            },
            "summary": "2 nearby building footprints found.",
        },
        "terrain_context": {
            "source": "opentopodata-srtm90m",
            "center_elevation_m": 160.0,
            "local_relief_m": 9.0,
            "dominant_aspect": "south-facing",
            "slope_percent": 7.1,
            "terrain_class": "rolling",
            "sample_radius_m": 35,
            "samples": [],
            "summary": "Local terrain reads as rolling with a south-facing bias.",
        },
        "shade_context": {
            "obstruction_risk": "moderate",
            "terrain_bias": "more solar-favored",
            "summary": "Structure-driven shade risk reads as moderate.",
        },
        "summary": "2 nearby building footprints found. Local terrain reads as rolling with a south-facing bias.",
        "model_note": "Building and terrain context only.",
    }


def build_west_facing_property_context():
    context = copy.deepcopy(build_property_context())
    context["terrain_context"]["dominant_aspect"] = "west-facing"
    context["terrain_context"]["summary"] = "Local terrain reads as rolling with a west-facing bias."
    context["shade_context"]["terrain_bias"] = "mostly neutral"
    context["summary"] = "2 nearby building footprints found. Local terrain reads as rolling with a west-facing bias."
    return context


def build_solar_data(latitude=30.2672, longitude=-97.7431):
    return {
        "avg_all_sky_radiation": 5.25,
        "avg_clear_sky_radiation": 6.33,
        "monthly_all_sky": {str(index).zfill(2): 5.25 for index in range(1, 13)},
        "monthly_clear_sky": {str(index).zfill(2): 6.33 for index in range(1, 13)},
        "all_sky_data_quality": 96.0,
        "clear_sky_data_quality": 98.0,
        "best_all_sky": {"month": "07", "value": 5.25},
        "worst_all_sky": {"month": "01", "value": 5.25},
        "best_clear_sky": {"month": "07", "value": 6.33},
        "worst_clear_sky": {"month": "01", "value": 6.33},
        "latitude": latitude,
        "longitude": longitude,
        "period": "daily average",
        "start_date": "2025-03-20",
        "end_date": "2026-03-20",
    }


def build_nrel_solar_data(latitude=30.2672, longitude=-97.7431, tilt=30.3, azimuth=180.0, losses=14.0):
    monthly_all_sky = {
        "01": 4.21,
        "02": 4.88,
        "03": 5.32,
        "04": 5.74,
        "05": 6.11,
        "06": 6.24,
        "07": 6.38,
        "08": 6.02,
        "09": 5.67,
        "10": 5.19,
        "11": 4.56,
        "12": 4.08,
    }
    monthly_ac_per_kw = {
        "01": 95.4,
        "02": 102.7,
        "03": 128.5,
        "04": 134.9,
        "05": 142.8,
        "06": 145.1,
        "07": 149.7,
        "08": 141.3,
        "09": 132.6,
        "10": 121.8,
        "11": 102.4,
        "12": 93.8,
    }

    return {
        "provider": "nrel-pvwatts",
        "avg_all_sky_radiation": 5.37,
        "avg_clear_sky_radiation": 5.37,
        "monthly_all_sky": monthly_all_sky,
        "monthly_clear_sky": dict(monthly_all_sky),
        "all_sky_data_quality": 100.0,
        "clear_sky_data_quality": 100.0,
        "latitude": latitude,
        "longitude": longitude,
        "period": "daily average",
        "pvwatts": {
            "version": "8.0.0",
            "inputs": {
                "system_capacity": 1.0,
                "module_type": 0,
                "losses": losses,
                "array_type": 1,
                "tilt": tilt,
                "azimuth": azimuth,
                "dataset": "nsrdb",
                "radius": 0,
                "inv_eff": 96.0,
            },
            "station_info": {
                "weather_data_source": "NSRDB PSM V3 GOES tmy-2020 3.2.0",
                "distance": 0,
                "city": "Austin",
                "state": "TX",
            },
            "warnings": [],
            "outputs": {
                "ac_monthly_per_kw": monthly_ac_per_kw,
                "ac_annual_per_kw": round(sum(monthly_ac_per_kw.values()), 2),
                "capacity_factor": 0.1594,
                "solrad_monthly": monthly_all_sky,
                "solrad_annual": 5.37,
            },
        },
    }


class PropertyRecordTests(unittest.TestCase):
    def setUp(self):
        data_persistence.reset_memory_storage()
        self.client = TestClient(main.app)

    def tearDown(self):
        data_persistence.reset_memory_storage()

    def test_property_record_endpoint_persists_roof_selection(self):
        response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "roof_selection": build_roof_selection(),
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("guid", payload)

        record = data_persistence.get_property_record(payload["guid"])
        self.assertIsNotNone(record)
        self.assertEqual(record["address"]["street"], "123 Main St")
        self.assertEqual(record["roof_selection"]["recommendedKw"], 10.5)
        self.assertEqual(record["property_preview"]["formatted_address"], build_property_preview()["formatted_address"])

    def test_property_record_endpoint_persists_garden_zones(self):
        response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "garden_zones": build_garden_zones(),
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["garden_zones"]), 2)
        self.assertEqual(payload["garden_zones"][0]["name"], "Garden zone 1")

        record = data_persistence.get_property_record(payload["guid"])
        self.assertIsNotNone(record)
        self.assertEqual(len(record["garden_zones"]), 2)
        self.assertEqual(record["garden_zones"][1]["id"], "garden-zone-2")

    def test_property_record_endpoint_persists_property_context(self):
        response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "property_context": build_property_context(),
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["property_context"]["context_version"], "property-context-v1")
        self.assertEqual(payload["property_context"]["building_context"]["obstruction_risk"], "moderate")

        record = data_persistence.get_property_record(payload["guid"])
        self.assertEqual(record["property_context"]["terrain_context"]["terrain_class"], "rolling")

    def test_property_record_endpoint_preserves_garden_zones_when_updating_roof(self):
        first_response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "garden_zones": build_garden_zones(),
            },
        )
        guid = first_response.json()["guid"]

        second_response = self.client.post(
            "/api/property-record",
            json={
                "guid": guid,
                "address": build_address(),
                "property_preview": build_property_preview(),
                "roof_selection": build_roof_selection(),
            },
        )

        self.assertEqual(second_response.status_code, 200)
        payload = second_response.json()
        self.assertEqual(len(payload["garden_zones"]), 2)

        record = data_persistence.get_property_record(guid)
        self.assertEqual(record["roof_selection"]["recommendedKw"], 10.5)
        self.assertEqual(len(record["garden_zones"]), 2)

    def test_find_property_record_returns_saved_garden_zones_by_address(self):
        property_response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "garden_zones": build_garden_zones(),
            },
        )
        guid = property_response.json()["guid"]

        find_response = self.client.post(
            "/api/property-record/find",
            json=build_address(),
        )

        self.assertEqual(find_response.status_code, 200)
        payload = find_response.json()
        self.assertEqual(payload["record"]["guid"], guid)
        self.assertEqual(len(payload["record"]["garden_zones"]), 2)
        self.assertEqual(payload["record"]["address"]["street"], "123 Main St")

    def test_recent_property_records_returns_latest_garden_plan_first(self):
        address_one = build_address()
        address_two = {
            **build_address(),
            "street": "456 Oak Ave",
            "zip": "78703",
        }
        preview_one = build_property_preview()
        preview_two = {
            **build_property_preview(),
            "query": "456 Oak Ave, Austin, TX 78703, United States",
            "formatted_address": "456 Oak Ave, Austin, TX 78703, United States",
            "latitude": 30.2711,
            "longitude": -97.7418,
            "address": address_two,
        }
        updated_garden_zones = [build_garden_zone(1), build_garden_zone(3)]

        with patch.object(
            data_persistence,
            "_property_record_stored_at_value",
            side_effect=[
                "2026-03-31T10:00:00",
                "2026-03-31T10:05:00",
                "2026-03-31T10:10:00",
            ],
        ):
            first_response = self.client.post(
                "/api/property-record",
                json={
                    "address": address_one,
                    "property_preview": preview_one,
                    "garden_zones": [build_garden_zone(1)],
                },
            )
            second_response = self.client.post(
                "/api/property-record",
                json={
                    "address": address_two,
                    "property_preview": preview_two,
                    "garden_zones": [build_garden_zone(2)],
                },
            )
            first_guid = first_response.json()["guid"]
            second_guid = second_response.json()["guid"]
            self.client.post(
                "/api/property-record",
                json={
                    "guid": first_guid,
                    "address": address_one,
                    "property_preview": preview_one,
                    "garden_zones": updated_garden_zones,
                },
            )

        response = self.client.post(
            "/api/property-record/recent",
            json={
                "max_items": 8,
                "require_garden_zones": True,
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        records = payload["records"]
        self.assertEqual([record["guid"] for record in records], [first_guid, second_guid])
        self.assertEqual(records[0]["stored_at"], "2026-03-31T10:10:00")
        self.assertEqual(len(records[0]["garden_zones"]), 2)
        self.assertEqual(records[0]["garden_zones"][1]["id"], "garden-zone-3")

    def test_recent_property_records_can_filter_to_saved_garden_plans(self):
        non_garden_address = {
            **build_address(),
            "street": "789 Pine St",
            "zip": "78704",
        }
        non_garden_preview = {
            **build_property_preview(),
            "query": "789 Pine St, Austin, TX 78704, United States",
            "formatted_address": "789 Pine St, Austin, TX 78704, United States",
            "latitude": 30.2659,
            "longitude": -97.7482,
            "address": non_garden_address,
        }

        self.client.post(
            "/api/property-record",
            json={
                "address": non_garden_address,
                "property_preview": non_garden_preview,
                "roof_selection": build_roof_selection(),
            },
        )
        self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "garden_zones": build_garden_zones(),
            },
        )

        response = self.client.post(
            "/api/property-record/recent",
            json={
                "max_items": 8,
                "require_garden_zones": True,
            },
        )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["records"]), 1)
        self.assertEqual(payload["records"][0]["address"]["street"], "123 Main St")
        self.assertEqual(len(payload["records"][0]["garden_zones"]), 2)

    def test_solar_potential_uses_persisted_roof_selection(self):
        property_response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "roof_selection": build_roof_selection(),
            },
        )
        guid = property_response.json()["guid"]

        with patch.object(main, "get_nrel_api_key", return_value=None):
            with patch.object(main, "check_existing_zip_data", return_value=(None, None)):
                with patch.object(main, "geocode_address", return_value=(30.2672, -97.7431)):
                    with patch.object(main, "get_nasa_power_data", return_value=build_solar_data()):
                        with patch.object(main, "get_timezone", return_value="America/Chicago"):
                            response = self.client.post(
                                "/api/solar-potential",
                                json={
                                    "guid": guid,
                                    "panel_efficiency": 0.2,
                                    "electricity_rate": 0.16,
                                    "installation_cost_per_watt": 3.0,
                                },
                            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["sizing_source"], "roof-geometry")
        self.assertEqual(payload["system_size_kw"], 10.5)
        self.assertEqual(payload["roof_area_square_feet"], 1000)
        self.assertEqual(payload["latitude"], 30.2672)
        self.assertEqual(payload["time_zone"], "America/Chicago")
        self.assertEqual(payload["match_quality"], "high")
        self.assertEqual(payload["confidence"]["id"], "high")
        self.assertTrue(payload["confidence"]["factors"])
        self.assertTrue(payload["assumptions"])
        self.assertEqual(payload["production_model"]["id"], "roof-backed-monthly-v2")
        self.assertEqual(payload["data_provider"], "nasa")
        self.assertEqual(payload["production_model"]["peak_month"]["month"], "01")
        self.assertAlmostEqual(payload["monthly_production"]["01"], 1363.7, places=1)
        self.assertGreater(payload["annual_production"], 16000)
        self.assertGreater(payload["specific_yield"], 1500)
        self.assertGreater(payload["capacity_factor"], 0.17)

    def test_solar_potential_uses_saved_property_context_for_site_losses(self):
        property_response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "property_context": build_property_context(),
                "roof_selection": build_roof_selection(),
            },
        )
        guid = property_response.json()["guid"]

        with patch.object(main, "get_nrel_api_key", return_value=None):
            with patch.object(main, "check_existing_zip_data", return_value=(None, None)):
                with patch.object(main, "geocode_address", return_value=(30.2672, -97.7431)):
                    with patch.object(main, "get_nasa_power_data", return_value=build_solar_data()):
                        with patch.object(main, "get_timezone", return_value="America/Chicago"):
                            response = self.client.post(
                                "/api/solar-potential",
                                json={
                                    "guid": guid,
                                    "panel_efficiency": 0.2,
                                    "electricity_rate": 0.16,
                                    "installation_cost_per_watt": 3.0,
                                },
                            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["production_model"]["id"], "roof-backed-monthly-v2")
        self.assertTrue(payload["production_model"]["site_context_available"])
        self.assertEqual(payload["production_model"]["assumed_azimuth"], 180.0)
        self.assertEqual(
            payload["production_model"]["tilt_source"],
            "latitude baseline nudged by local terrain aspect",
        )
        self.assertGreater(payload["production_model"]["modeled_site_losses_percent"], 0)
        self.assertLess(payload["monthly_production"]["01"], 1363.7)
        self.assertTrue(
            any(
                "Nearby building and terrain context contribute about"
                in assumption
                for assumption in payload["assumptions"]
            )
        )

    def test_solar_potential_prefers_nrel_pvwatts_when_api_key_is_available(self):
        property_response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "roof_selection": build_roof_selection(),
            },
        )
        guid = property_response.json()["guid"]

        with patch.object(main, "get_nrel_api_key", return_value="test-key"):
            with patch.object(main, "check_existing_zip_data", return_value=(None, None)):
                with patch.object(main, "geocode_address", return_value=(30.2672, -97.7431)):
                    with patch.object(main, "get_nrel_pvwatts_data", return_value=build_nrel_solar_data()):
                        with patch.object(main, "get_timezone", return_value="America/Chicago"):
                            response = self.client.post(
                                "/api/solar-potential",
                                json={
                                    "guid": guid,
                                    "panel_efficiency": 0.2,
                                    "electricity_rate": 0.16,
                                    "installation_cost_per_watt": 3.0,
                                },
                            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["production_model"]["id"], "nrel-pvwatts-v8")
        self.assertEqual(payload["data_source"], "nrel-pvwatts")
        self.assertEqual(payload["data_provider"], "nrel-pvwatts")
        self.assertEqual(payload["production_model"]["assumed_azimuth"], 180.0)
        self.assertEqual(payload["production_model"]["assumed_tilt"], 30.3)
        self.assertEqual(payload["production_model"]["weather_data_source"], "NSRDB PSM V3 GOES tmy-2020 3.2.0")
        self.assertAlmostEqual(payload["monthly_production"]["01"], 1001.7, places=1)
        self.assertAlmostEqual(payload["annual_production"], 15655.5, places=1)
        self.assertAlmostEqual(payload["specific_yield"], 1491.0, places=2)
        self.assertAlmostEqual(payload["capacity_factor"], 0.1594, places=4)
        self.assertEqual(payload["peak_month"]["month"], "07")

    def test_solar_potential_requests_refined_nrel_inputs_from_roof_and_context(self):
        property_response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "property_context": build_west_facing_property_context(),
                "roof_selection": build_west_facing_roof_selection(),
            },
        )
        guid = property_response.json()["guid"]

        with patch.object(main, "get_nrel_api_key", return_value="test-key"):
            with patch.object(main, "check_existing_zip_data", return_value=(None, None)):
                with patch.object(main, "geocode_address", return_value=(30.2672, -97.7431)):
                    with patch.object(
                        main,
                        "get_nrel_pvwatts_data",
                        side_effect=lambda lat, lon, tilt=None, azimuth=None, losses=None: build_nrel_solar_data(
                            latitude=lat,
                            longitude=lon,
                            tilt=tilt or 30.3,
                            azimuth=azimuth or 180.0,
                            losses=losses or 14.0,
                        ),
                    ) as mocked_pvwatts:
                        with patch.object(main, "get_timezone", return_value="America/Chicago"):
                            response = self.client.post(
                                "/api/solar-potential",
                                json={
                                    "guid": guid,
                                    "panel_efficiency": 0.2,
                                    "electricity_rate": 0.16,
                                    "installation_cost_per_watt": 3.0,
                                },
                            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(mocked_pvwatts.call_count, 1)
        self.assertEqual(mocked_pvwatts.call_args.kwargs["azimuth"], 270.0)
        self.assertAlmostEqual(mocked_pvwatts.call_args.kwargs["tilt"], 32.8, places=1)
        self.assertGreater(mocked_pvwatts.call_args.kwargs["losses"], 14.0)
        self.assertEqual(payload["production_model"]["assumed_azimuth"], 270.0)
        self.assertAlmostEqual(payload["production_model"]["assumed_tilt"], 32.8, places=1)
        self.assertTrue(payload["production_model"]["site_context_available"])

    def test_save_solar_report_persists_snapshot_to_property_record(self):
        property_response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "roof_selection": build_roof_selection(),
            },
        )
        guid = property_response.json()["guid"]

        with patch.object(main, "get_nrel_api_key", return_value=None):
            with patch.object(main, "check_existing_zip_data", return_value=(None, None)):
                with patch.object(main, "geocode_address", return_value=(30.2672, -97.7431)):
                    with patch.object(main, "get_nasa_power_data", return_value=build_solar_data()):
                        with patch.object(main, "get_timezone", return_value="America/Chicago"):
                            response = self.client.post(
                                "/api/solar-report",
                                json={
                                    "guid": guid,
                                    "panel_efficiency": 0.2,
                                    "electricity_rate": 0.16,
                                    "installation_cost_per_watt": 3.0,
                                },
                            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(len(payload["reports"]), 1)
        self.assertEqual(payload["report"]["production_model"]["id"], "roof-backed-monthly-v2")
        self.assertEqual(payload["estimate"]["system_size_kw"], 10.5)

        record = data_persistence.get_property_record(guid)
        self.assertEqual(len(record["saved_solar_reports"]), 1)
        self.assertEqual(
            record["saved_solar_reports"][0]["annual_production"],
            payload["report"]["annual_production"],
        )
        self.assertEqual(
            record["saved_solar_reports"][0]["confidence"]["id"],
            payload["report"]["confidence"]["id"],
        )

    def test_create_shareable_solar_quote_persists_to_saved_report_and_is_publicly_fetchable(self):
        property_response = self.client.post(
            "/api/property-record",
            json={
                "address": build_address(),
                "property_preview": build_property_preview(),
                "roof_selection": build_roof_selection(),
            },
        )
        guid = property_response.json()["guid"]

        with patch.object(main, "get_nrel_api_key", return_value=None):
            with patch.object(main, "check_existing_zip_data", return_value=(None, None)):
                with patch.object(main, "geocode_address", return_value=(30.2672, -97.7431)):
                    with patch.object(main, "get_nasa_power_data", return_value=build_solar_data()):
                        with patch.object(main, "get_timezone", return_value="America/Chicago"):
                            report_response = self.client.post(
                                "/api/solar-report",
                                json={
                                    "guid": guid,
                                    "panel_efficiency": 0.2,
                                    "electricity_rate": 0.16,
                                    "installation_cost_per_watt": 3.0,
                                },
                            )

        saved_report = report_response.json()["report"]
        quote_response = self.client.post(
            "/api/solar-quote",
            json={
                "guid": guid,
                "report_id": saved_report["id"],
            },
        )

        self.assertEqual(quote_response.status_code, 200)
        quote_payload = quote_response.json()
        self.assertEqual(quote_payload["report"]["id"], saved_report["id"])
        self.assertEqual(quote_payload["quote"]["status"], "share-ready")
        self.assertTrue(quote_payload["quote"]["share_path"].startswith("/quote/"))

        record = data_persistence.get_property_record(guid)
        stored_quote = record["saved_solar_reports"][0]["homeowner_quote"]
        self.assertEqual(stored_quote["id"], quote_payload["quote"]["id"])

        public_quote_response = self.client.get(f"/api/solar-quote/{stored_quote['id']}")
        self.assertEqual(public_quote_response.status_code, 200)
        public_quote_payload = public_quote_response.json()
        self.assertEqual(public_quote_payload["quote"]["id"], stored_quote["id"])
        self.assertEqual(public_quote_payload["report"]["id"], saved_report["id"])
        self.assertEqual(public_quote_payload["address"]["street"], "123 Main St")

    def test_property_climate_endpoint_returns_snapshot(self):
        climate_snapshot = {
            "hardiness_zone": {
                "label": "9a",
                "average_annual_extreme_min_f": 22.4,
                "range_f": "20 to 25°F",
                "estimated": True,
            },
            "annual": {
                "average_temperature_f": 71.2,
                "average_relative_humidity": 68.0,
                "average_daily_shortwave_radiation_kwh_m2": 4.95,
            },
            "growing_season": {
                "label": "April-September",
                "average_temperature_f": 82.1,
                "average_relative_humidity": 62.0,
                "average_daily_shortwave_radiation_kwh_m2": 6.11,
            },
            "summary": "Estimated hardiness band 9a using 2016-2025 historical weather.",
        }

        with patch.object(main, "get_timezone", return_value="America/Chicago"):
            with patch.object(main, "get_property_climate_snapshot", return_value=climate_snapshot) as mocked_snapshot:
                response = self.client.post(
                    "/api/property-climate",
                    json={
                        "latitude": 30.2672,
                        "longitude": -97.7431,
                    },
                )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["hardiness_zone"]["label"], "9a")
        self.assertEqual(payload["annual"]["average_temperature_f"], 71.2)
        self.assertEqual(payload["growing_season"]["average_relative_humidity"], 62.0)
        mocked_snapshot.assert_called_once_with(30.2672, -97.7431, "America/Chicago")

    def test_garden_crop_catalog_is_seeded_into_persistence(self):
        payload = data_persistence.get_garden_crop_catalog()

        self.assertIsNotNone(payload)
        self.assertEqual(payload["catalog_id"], "default")
        self.assertEqual(payload["version"], "2026-04-03")
        self.assertTrue(payload["stored_at"])
        self.assertGreater(len(payload["crops"]), 20)
        self.assertTrue(any(crop["id"] == "tomato" for crop in payload["crops"]))
        self.assertTrue(any(source["id"] == "usda-hardiness-map" for source in payload["source_basis"]))

    def test_garden_crop_catalog_endpoint_returns_persisted_catalog(self):
        response = self.client.post("/api/garden-crop-catalog", json={})

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["catalog_id"], "default")
        self.assertEqual(payload["version"], "2026-04-03")
        self.assertGreater(len(payload["crops"]), 20)
        tomato = next(crop for crop in payload["crops"] if crop["id"] == "tomato")
        self.assertEqual(tomato["sun"]["primary"], ["full-sun"])

    def test_property_context_endpoint_returns_snapshot(self):
        context_snapshot = build_property_context()

        with patch.object(main, "get_property_context_snapshot", return_value=context_snapshot) as mocked_snapshot:
            response = self.client.post(
                "/api/property-context",
                json={
                    "latitude": 30.2672,
                    "longitude": -97.7431,
                    "bounds": build_property_preview()["bounds"],
                    "match_quality": "high",
                },
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["context_version"], "property-context-v1")
        self.assertEqual(payload["building_context"]["building_count"], 2)
        mocked_snapshot.assert_called_once_with(
            30.2672,
            -97.7431,
            bounds=build_property_preview()["bounds"],
            match_quality="high",
        )


if __name__ == "__main__":
    unittest.main()
