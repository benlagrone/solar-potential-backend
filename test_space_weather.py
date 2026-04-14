import json
import unittest
from unittest.mock import patch

import requests
from fastapi.testclient import TestClient

import live_conditions
import main


class FakeResponse:
    def __init__(self, text, status_code=200):
        self.text = text
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} upstream error")


def build_scales_payload():
    return {
        "0": {
            "DateStamp": "2026-04-05",
            "R": {"Scale": "0", "Text": "none"},
            "S": {"Scale": "0", "Text": "none"},
            "G": {"Scale": "2", "Text": "moderate"},
        },
        "1": {
            "DateStamp": "2026-04-06",
            "R": {"Scale": "0", "Text": "none", "MinorProb": "20", "MajorProb": "5"},
            "S": {"Scale": "0", "Text": "none", "Prob": "10"},
            "G": {"Scale": "1", "Text": "minor"},
        },
        "2": {
            "DateStamp": "2026-04-07",
            "R": {"Scale": "0", "Text": "none", "MinorProb": "20", "MajorProb": "5"},
            "S": {"Scale": "0", "Text": "none", "Prob": "10"},
            "G": {"Scale": "1", "Text": "minor"},
        },
        "3": {
            "DateStamp": "2026-04-08",
            "R": {"Scale": "0", "Text": "none", "MinorProb": "20", "MajorProb": "5"},
            "S": {"Scale": "0", "Text": "none", "Prob": "10"},
            "G": {"Scale": "0", "Text": "none"},
        },
    }


def build_xray_payload():
    return [
        {"time_tag": "2026-04-05T14:00:00Z", "energy": "0.1-0.8nm", "flux": 2.5e-06},
        {"time_tag": "2026-04-05T14:05:00Z", "energy": "0.1-0.8nm", "flux": 4.5e-06},
    ]


def build_flares_payload():
    return [
        {
            "classType": "C4.5",
            "peakTime": "2026-04-05T12:00Z",
            "beginTime": "2026-04-05T11:48Z",
            "sourceLocation": "N10W04",
        }
    ]


def build_storms_payload():
    return [
        {
            "startTime": "2026-04-05T10:00Z",
            "allKpIndex": [
                {"observedTime": "2026-04-05T12:00Z", "kpIndex": "4.67"},
                {"observedTime": "2026-04-05T15:00Z", "kpIndex": "5.33"},
            ],
        }
    ]


def build_aurora_payload():
    return {
        "Observation Time": "2026-04-05T19:17:00Z",
        "Forecast Time": "2026-04-05T20:05:00Z",
        "Data Format": "[Longitude, Latitude, Aurora]",
        "coordinates": [
            [262, 30, 0],
            [262, 31, 0],
            [262, 32, 2],
            [262, 33, 4],
            [262, 34, 6],
            [263, 32, 1],
            [261, 32, 1],
        ],
        "type": "Feature",
    }


def build_drap_payload():
    return """# Product Valid At: 2026-04-05 19:30 UTC
# Estimated Recovery Time: 2026-04-05 21:00 UTC
# X-Ray Message: M1 flare absorption is active
# Proton Message: No proton event in progress
-104 -102 -100 -98 -96
28 | 0 0 0 0 0
30 | 0 0 6.0 9.0 3.0
32 | 0 0 4.0 6.5 2.0
"""


def build_glotec_index_payload():
    return [
        {
            "time_tag": "2026-04-05T19:25:00Z",
            "url": "/products/glotec/geojson_2d_urt/2026/04/05/glotec_2d_urt_20260405T192500Z.geojson",
        }
    ]


def build_glotec_grid_payload():
    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-97.75, 30.27]},
                "properties": {
                    "tec": 19.3,
                    "anomaly": 5.7,
                    "hmF2": 312.4,
                    "NmF2": 1.93e12,
                    "quality_flag": 0,
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-122.33, 47.61]},
                "properties": {
                    "tec": 11.2,
                    "anomaly": 1.1,
                    "hmF2": 274.0,
                    "NmF2": 1.11e12,
                    "quality_flag": 0,
                },
            },
        ],
    }


def build_surface_snapshot():
    return {
        "latitude": 30.2672,
        "longitude": -97.7431,
        "time_zone": "America/Chicago",
        "observed_at": "2026-04-05T09:00",
        "source": "open-meteo-forecast",
        "is_daylight": False,
        "current": {
            "ghi_w_m2": 0.0,
            "dni_w_m2": 0.0,
            "intensity_level": "low",
        },
        "trend": {
            "previous_hour_ghi_w_m2": 0.0,
            "change_from_previous_hour_w_m2": 0.0,
            "next_hour_ghi_w_m2": 0.0,
            "change_to_next_hour_w_m2": 0.0,
            "max_hourly_ramp_w_m2": 0.0,
            "ramp_start_time": None,
            "ramp_end_time": None,
        },
        "next_peak": {
            "time": "2026-04-05T21:00",
            "ghi_w_m2": 0.0,
            "dni_w_m2": 0.0,
        },
        "spike_level": "low",
        "summary": "It is dark at the property.",
        "freshness": {
            "status": "fresh",
            "checked_at": "2026-04-05T14:00:00+00:00",
            "fetched_at": "2026-04-05T14:00:00+00:00",
            "latest_fetched_at": "2026-04-05T14:00:00+00:00",
            "expires_at": "2026-04-05T14:10:00+00:00",
            "age_seconds": 0.0,
            "seconds_until_expiry": 600.0,
            "is_stale": False,
            "refresh_failed": False,
            "source_count": 1,
            "sources": {
                "open-meteo-forecast": {
                    "source": "open-meteo-forecast",
                    "status": "fresh",
                    "fetched_at": "2026-04-05T14:00:00+00:00",
                    "expires_at": "2026-04-05T14:10:00+00:00",
                    "ttl_seconds": 600,
                    "age_seconds": 0.0,
                    "seconds_until_expiry": 600.0,
                    "is_stale": False,
                    "cache_hit": False,
                    "refresh_failed": False,
                }
            },
        },
        "reasons": [],
        "hourly_profile": [],
    }


class SpaceWeatherLogicTests(unittest.TestCase):
    def tearDown(self):
        live_conditions._CACHE.clear()

    def test_decode_json_payload_repairs_truncated_array(self):
        payload = '[["time_tag","density"],["2026-04-05 14:00:00.000","0.19"],'

        decoded = live_conditions._decode_json_payload(payload)

        self.assertEqual(decoded, [["time_tag", "density"], ["2026-04-05 14:00:00.000", "0.19"]])

    def test_build_aurora_context_marks_nearby_viewline(self):
        context = live_conditions._build_aurora_context(
            build_aurora_payload(),
            30.2672,
            -97.7431,
            is_daylight=False,
        )

        self.assertEqual(context["forecast_status"], "available")
        self.assertEqual(context["reach"], "nearby-viewline")
        self.assertEqual(context["visibility"], "possible")
        self.assertGreater(context["distance_to_viewline_km"], 0)


class SpaceWeatherEndpointTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(main.app)
        live_conditions._CACHE.clear()

    def tearDown(self):
        live_conditions._CACHE.clear()

    def _fake_requests_get(self, aurora_status_code=200):
        plasma_truncated = (
            '[["time_tag","density","speed","temperature"],'
            '["2026-04-05 14:03:00.000","0.52","535.9","117988"],'
        )
        payload_by_url = {
            live_conditions.NOAA_SCALES_URL: FakeResponse(json.dumps(build_scales_payload())),
            live_conditions.NOAA_ALERTS_URL: FakeResponse(json.dumps([])),
            live_conditions.NOAA_PLASMA_URL: FakeResponse(plasma_truncated),
            live_conditions.NOAA_XRAY_URL: FakeResponse(json.dumps(build_xray_payload())),
            live_conditions.NOAA_DRAP_URL: FakeResponse(build_drap_payload()),
            live_conditions.NOAA_GLOTEC_INDEX_URL: FakeResponse(json.dumps(build_glotec_index_payload())),
            f"{live_conditions.NOAA_BASE_URL}/products/glotec/geojson_2d_urt/2026/04/05/glotec_2d_urt_20260405T192500Z.geojson": FakeResponse(
                json.dumps(build_glotec_grid_payload())
            ),
            live_conditions.NOAA_AURORA_OVATION_URL: FakeResponse(
                json.dumps(build_aurora_payload()),
                status_code=aurora_status_code,
            ),
            live_conditions.NASA_DONKI_FLR_URL: FakeResponse(json.dumps(build_flares_payload())),
            live_conditions.NASA_DONKI_GST_URL: FakeResponse(json.dumps(build_storms_payload())),
        }

        def _dispatch(url, params=None, timeout=15):
            response = payload_by_url.get(url)
            if response is None:
                raise AssertionError(f"Unexpected URL requested: {url}")
            return response

        return _dispatch

    @patch.object(main, "get_timezone", return_value="America/Chicago")
    @patch.object(live_conditions, "get_surface_irradiance_snapshot", return_value=build_surface_snapshot())
    def test_space_weather_endpoint_accepts_truncated_plasma_payload(
        self,
        _surface_snapshot,
        _timezone,
    ):
        with patch.object(live_conditions.requests, "get", side_effect=self._fake_requests_get()):
            response = self.client.post(
                "/api/space-weather",
                json={"latitude": 30.2672, "longitude": -97.7431},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertIn("freshness", payload)
        self.assertIn("reasons", payload)
        self.assertEqual(payload["global"]["solar_wind"]["observed_at"], "2026-04-05 14:03:00.000")
        self.assertEqual(payload["local"]["aurora_viewline"]["reach"], "nearby-viewline")
        self.assertEqual(payload["local"]["drap"]["status"], "available")
        self.assertEqual(payload["local"]["drap"]["risk"], "moderate")
        self.assertEqual(payload["local"]["glotec"]["status"], "available")
        self.assertEqual(payload["local"]["glotec"]["risk"], "moderate")
        self.assertTrue(any(reason["id"] == "drap" for reason in payload["reasons"]))
        self.assertTrue(any(reason["id"] == "glotec" for reason in payload["reasons"]))

    @patch.object(main, "get_timezone", return_value="America/Chicago")
    @patch.object(live_conditions, "get_surface_irradiance_snapshot", return_value=build_surface_snapshot())
    def test_space_weather_endpoint_survives_missing_aurora_feed(
        self,
        _surface_snapshot,
        _timezone,
    ):
        with patch.object(
            live_conditions.requests,
            "get",
            side_effect=self._fake_requests_get(aurora_status_code=503),
        ):
            response = self.client.post(
                "/api/space-weather",
                json={"latitude": 30.2672, "longitude": -97.7431},
            )

        self.assertEqual(response.status_code, 200)
        payload = response.json()
        self.assertEqual(payload["local"]["aurora_viewline"]["forecast_status"], "unavailable")
        self.assertTrue(payload["freshness"]["sources"]["noaa-ovation-aurora"]["refresh_failed"])


if __name__ == "__main__":
    unittest.main()
