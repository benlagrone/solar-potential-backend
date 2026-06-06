import unittest
from unittest.mock import patch

import property_context


class PropertyContextTests(unittest.TestCase):
    @staticmethod
    def _bounds_center(bounds):
        return (
            (bounds["south"] + bounds["north"]) / 2,
            (bounds["west"] + bounds["east"]) / 2,
        )

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
        self.assertEqual(snapshot["context_version"], "property-context-v4")
        self.assertIsNotNone(parcel_context["planning_core_bounds"])
        self.assertIn("parcel_intelligence", snapshot)
        self.assertIn("tree_canopy_context", snapshot)
        self.assertIn("garden_sun_context", snapshot)
        self.assertEqual(snapshot["garden_sun_context"]["cloud_adjustment"]["status"], "uses-property-climate-when-loaded")
        self.assertGreater(parcel_context["gross_area_sq_ft"], parcel_context["planning_core_area_sq_ft"])
        self.assertGreater(parcel_context["planning_core_share"], parcel_context["estimated_plantable_share"])
        self.assertEqual(parcel_context["terrain_limit"], "moderate")
        self.assertIn(parcel_context["open_side"], {"north", "south", "east", "west"})
        self.assertIn("Planning envelope covers", snapshot["summary"])

    def test_property_context_snapshot_shifts_garden_envelope_away_from_street_side_point(self):
        building_geometry = {
            "type": "Polygon",
            "coordinates": [[
                [-97.74318, 30.26710],
                [-97.74302, 30.26710],
                [-97.74302, 30.26696],
                [-97.74318, 30.26696],
                [-97.74318, 30.26710],
            ]],
        }
        primary_building = {
            "id": "b-1",
            "kind": "residential",
            "shadow_pressure": 1.12,
            "distance_m": 18.0,
            "footprint_area_square_meters": 78.0,
            "centroid_within_match_envelope": True,
            "centroid": {
                "lat": 30.26703,
                "lng": -97.74310,
            },
            "geometry": building_geometry,
        }
        building_context = {
            "summary": "1 nearby building footprint found.",
            "directional_pressure": {
                "north": 0.12,
                "south": 0.64,
                "east": 0.18,
                "west": 0.08,
            },
            "nearby_buildings": [primary_building],
            "nearest_building": primary_building,
        }
        canopy_context = {
            "summary": "1 nearby canopy feature found.",
            "directional_pressure": {
                "north": 0.08,
                "south": 0.14,
                "east": 0.10,
                "west": 0.12,
            },
            "nearby_canopy": [],
            "nearest_canopy": None,
        }
        terrain_context = {
            "summary": "Local terrain reads as gentle with a south-facing bias.",
            "dominant_aspect": "south-facing",
            "terrain_class": "gentle",
            "slope_percent": 4.1,
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

        raw_center_lat, _ = self._bounds_center(snapshot["match_envelope"]["bounds"])
        planning_center_lat, _ = self._bounds_center(snapshot["parcel_context"]["bounds"])
        focus_anchor = snapshot["parcel_context"]["focus_anchor"]

        self.assertIsNotNone(focus_anchor)
        self.assertEqual(focus_anchor["street_side"], "north")
        self.assertEqual(focus_anchor["garden_side"], "south")
        self.assertLess(planning_center_lat, raw_center_lat)

    def test_parcel_intelligence_scores_primary_structure_candidate(self):
        primary_building = {
            "id": "b-1",
            "name": "Main house",
            "kind": "house",
            "shadow_pressure": 0.48,
            "distance_m": 10.0,
            "footprint_area_square_meters": 140.0,
            "footprint_area_square_feet": 1507.0,
            "centroid_within_match_envelope": True,
            "dominant_edge_bearing": 90,
            "dominant_edge_length_m": 16.0,
            "centroid": {
                "lat": 30.26718,
                "lng": -97.74308,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-97.74318, 30.26724],
                    [-97.74298, 30.26724],
                    [-97.74298, 30.26708],
                    [-97.74318, 30.26708],
                    [-97.74318, 30.26724],
                ]],
            },
        }
        shed = {
            "id": "b-2",
            "name": "Shed",
            "kind": "shed",
            "shadow_pressure": 0.2,
            "distance_m": 23.0,
            "footprint_area_square_meters": 32.0,
            "footprint_area_square_feet": 344.0,
            "centroid_within_match_envelope": True,
            "centroid": {
                "lat": 30.26694,
                "lng": -97.74330,
            },
            "geometry": {
                "type": "Polygon",
                "coordinates": [[
                    [-97.74334, 30.26699],
                    [-97.74325, 30.26699],
                    [-97.74325, 30.26691],
                    [-97.74334, 30.26691],
                    [-97.74334, 30.26699],
                ]],
            },
        }
        building_context = {
            "summary": "2 nearby building footprints found.",
            "directional_pressure": {
                "north": 0.2,
                "south": 0.4,
                "east": 0.1,
                "west": 0.1,
            },
            "nearby_buildings": [shed, primary_building],
            "nearest_building": primary_building,
        }
        canopy_context = {
            "summary": "No nearby canopy features were found.",
            "directional_pressure": {
                "north": 0,
                "south": 0,
                "east": 0,
                "west": 0,
            },
            "nearby_canopy": [],
            "nearest_canopy": None,
        }
        terrain_context = {
            "summary": "Local terrain reads as flat.",
            "dominant_aspect": "flat",
            "terrain_class": "flat",
            "slope_percent": 0.5,
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

        intelligence = snapshot["parcel_intelligence"]
        self.assertTrue(intelligence["available"])
        self.assertEqual(intelligence["primary_structure"]["building_id"], "b-1")
        self.assertGreaterEqual(intelligence["primary_structure_confidence_score"], 80)
        self.assertIn("solar", intelligence["feature_coverage"])
        self.assertFalse(intelligence["parcel"]["certified_boundary"])


if __name__ == "__main__":
    unittest.main()
