import unittest
from unittest.mock import patch

import property_context


class PropertyContextTests(unittest.TestCase):
    def test_build_overpass_buildings_fetches_json_payload(self):
        expected_payload = {"elements": []}

        with patch.object(property_context, "_fetch_json", return_value=expected_payload) as mocked_fetch:
            payload = property_context._build_overpass_buildings(30.2672, -97.7431, 75)

        self.assertIs(payload, expected_payload)
        self.assertEqual(mocked_fetch.call_args.kwargs["ttl_seconds"], 86400)
        self.assertIn('way["building"]', mocked_fetch.call_args.kwargs["params"]["data"])

    def test_property_context_snapshot_includes_parcel_context(self):
        building_context = {
            "summary": "2 nearby building footprints found.",
            "directional_pressure": {
                "north": 0.18,
                "south": 1.12,
                "east": 0.22,
                "west": 0.14,
            },
            "nearby_buildings": [
                {"id": "b-1", "shadow_pressure": 1.12, "distance_m": 18.0},
                {"id": "b-2", "shadow_pressure": 0.44, "distance_m": 33.0},
            ],
            "nearest_building": {
                "id": "b-1",
                "shadow_pressure": 1.12,
                "distance_m": 18.0,
            },
        }
        canopy_context = {
            "summary": "3 nearby canopy features found.",
            "directional_pressure": {
                "north": 0.08,
                "south": 0.22,
                "east": 0.12,
                "west": 0.64,
            },
            "nearby_canopy": [
                {"id": "c-1", "canopy_pressure": 0.64, "distance_m": 14.0},
                {"id": "c-2", "canopy_pressure": 0.32, "distance_m": 21.0},
                {"id": "c-3", "canopy_pressure": 0.18, "distance_m": 29.0},
            ],
            "nearest_canopy": {
                "id": "c-1",
                "canopy_pressure": 0.64,
                "distance_m": 14.0,
            },
        }
        terrain_context = {
            "summary": "Local terrain reads as rolling with a south-facing bias.",
            "dominant_aspect": "south-facing",
            "terrain_class": "rolling",
            "slope_percent": 7.4,
        }

        with patch.object(property_context, "_build_building_context", return_value=building_context):
            with patch.object(property_context, "_build_canopy_context", return_value=canopy_context):
                with patch.object(property_context, "_build_terrain_context", return_value=terrain_context):
                    snapshot = property_context.get_property_context_snapshot(
                        30.2672,
                        -97.7431,
                        bounds={
                            "south": 30.2668,
                            "north": 30.2676,
                            "west": -97.7437,
                            "east": -97.7426,
                        },
                        match_quality="high",
                    )

        parcel_context = snapshot["parcel_context"]
        self.assertEqual(snapshot["context_version"], "property-context-v3")
        self.assertIsNotNone(parcel_context["planning_core_bounds"])
        self.assertGreater(parcel_context["gross_area_sq_ft"], parcel_context["planning_core_area_sq_ft"])
        self.assertGreater(parcel_context["planning_core_share"], parcel_context["estimated_plantable_share"])
        self.assertEqual(parcel_context["terrain_limit"], "moderate")
        self.assertIn(parcel_context["open_side"], {"north", "south", "east", "west"})
        self.assertIn("Planning envelope covers", snapshot["summary"])


if __name__ == "__main__":
    unittest.main()
