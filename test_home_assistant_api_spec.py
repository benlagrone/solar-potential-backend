from pathlib import Path
import unittest

import yaml


class HomeAssistantApiSpecTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec_path = Path(__file__).parent / "docs" / "home-assistant-api-spec.yaml"
        cls.spec = yaml.safe_load(spec_path.read_text(encoding="utf-8"))

    def test_spec_defines_api_key_security_and_expected_responses(self):
        operation = self.spec["paths"]["/api/home-assistant/snapshot"]["get"]
        security_scheme = self.spec["components"]["securitySchemes"]["HomeAssistantApiKey"]

        self.assertEqual(self.spec["openapi"], "3.1.0")
        self.assertEqual(operation["security"], [{"HomeAssistantApiKey": []}])
        self.assertEqual(security_scheme["type"], "apiKey")
        self.assertEqual(security_scheme["in"], "header")
        self.assertEqual(security_scheme["name"], "X-API-Key")
        self.assertTrue({"200", "401", "422", "503"}.issubset(operation["responses"]))

    def test_spec_covers_home_assistant_sensor_fields(self):
        schemas = self.spec["components"]["schemas"]

        self.assertTrue(
            {
                "ghi_w_m2",
                "dni_w_m2",
                "cloud_cover_percent",
                "is_daylight",
            }.issubset(schemas["SurfaceIrradiance"]["properties"])
        )
        self.assertTrue(
            {
                "azimuth_degrees",
                "elevation_degrees",
                "zenith_degrees",
                "hour_angle_degrees",
                "declination_degrees",
                "above_horizon",
            }.issubset(schemas["SunPosition"]["properties"])
        )
        self.assertTrue(
            {
                "alert_level",
                "geomagnetic_storm_scale",
                "solar_wind_speed_km_s",
            }.issubset(schemas["SpaceWeather"]["properties"])
        )


if __name__ == "__main__":
    unittest.main()
