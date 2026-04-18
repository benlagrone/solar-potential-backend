import unittest
from os import environ
from unittest.mock import patch

from fastapi.testclient import TestClient

import data_persistence
import main


class FakeLocation:
    def __init__(self, address, latitude, longitude, raw):
        self.address = address
        self.latitude = latitude
        self.longitude = longitude
        self.raw = raw


def build_raw_address(house_number, road, city="Houston", state="Texas", postcode="77024"):
    return {
        "house_number": house_number,
        "road": road,
        "city": city,
        "state": state,
        "postcode": postcode,
        "country": "United States",
    }


def build_candidate(osm_id, latitude, longitude, house_number, road, addresstype="house"):
    return FakeLocation(
        address=f"{house_number} {road}, Houston, Texas 77024, United States",
        latitude=latitude,
        longitude=longitude,
        raw={
            "address": build_raw_address(house_number, road),
            "addresstype": addresstype,
            "class": "building" if addresstype == "house" else "highway",
            "type": addresstype,
            "boundingbox": ["29.767100", "29.767180", "-95.550800", "-95.550720"],
            "osm_type": "way",
            "osm_id": osm_id,
        },
    )


class GeocodeSelectionTests(unittest.TestCase):
    def setUp(self):
        data_persistence.reset_memory_storage()

    def tearDown(self):
        data_persistence.reset_memory_storage()

    def test_build_nominatim_geolocator_uses_defaults_for_blank_env_values(self):
        with patch.object(main, "Nominatim") as mock_nominatim:
            with patch.dict(
                environ,
                {
                    "GEOCODER_USER_AGENT": "   ",
                    "GEOCODER_NOMINATIM_DOMAIN": "",
                    "GEOCODER_NOMINATIM_SCHEME": "   ",
                },
                clear=False,
            ):
                main.build_nominatim_geolocator()

        _, kwargs = mock_nominatim.call_args
        self.assertEqual(kwargs["user_agent"], "solar_potential_app")
        self.assertEqual(kwargs["domain"], "nominatim.openstreetmap.org")
        self.assertEqual(kwargs["scheme"], "https")

    def test_get_geocoder_provider_defaults_to_arcgis_without_custom_nominatim(self):
        with patch.dict(
            environ,
            {
                "GEOCODER_PROVIDER": "hybrid",
                "GEOCODER_NOMINATIM_DOMAIN": "",
            },
            clear=False,
        ):
            self.assertEqual(main.get_geocoder_provider(), "arcgis")

    def test_get_geocoder_provider_keeps_hybrid_with_custom_nominatim(self):
        with patch.dict(
            environ,
            {
                "GEOCODER_PROVIDER": "hybrid",
                "GEOCODER_NOMINATIM_DOMAIN": "nominatim.internal.example",
            },
            clear=False,
        ):
            self.assertEqual(main.get_geocoder_provider(), "hybrid")

    def test_geocode_location_uses_arcgis_main_path_without_custom_nominatim(self):
        requested_address = {
            "street": "12518 Boheme Dr",
            "city": "Houston",
            "state": "TX",
            "zip": "77024",
            "country": "United States",
        }
        arcgis_location = main.build_location(
            "12518 Boheme Dr, Houston, Texas, 77024",
            29.767836,
            -95.551491,
            {
                "provider": "arcgis",
                "source": "arcgis-pointaddress",
                "boundingbox": ["29.767092", "29.769092", "-95.552499", "-95.550499"],
                "address": {
                    "house_number": "12518",
                    "road": "Boheme Dr",
                    "city": "Houston",
                    "state": "TX",
                    "postcode": "77024",
                    "country": "United States",
                },
            },
        )

        with patch.dict(
            environ,
            {
                "GEOCODER_PROVIDER": "hybrid",
                "GEOCODER_NOMINATIM_DOMAIN": "",
            },
            clear=False,
        ):
            with patch.object(main, "fetch_nominatim_candidates", side_effect=AssertionError("should not call Nominatim")):
                with patch.object(main, "fetch_arcgis_forward_candidates", return_value=[]):
                    with patch.object(main, "fetch_arcgis_point_address", return_value=arcgis_location):
                        with patch.object(
                            main,
                            "reverse_geocode_arcgis_location",
                            return_value={
                                "address": {
                                    "Address": "12518 Boheme Dr",
                                    "LongLabel": "12518 Boheme Dr, Houston, TX, 77024, USA",
                                    "AddNum": "12518",
                                    "City": "Houston",
                                    "RegionAbbr": "TX",
                                    "Postal": "77024",
                                    "CntryName": "United States",
                                }
                            },
                        ):
                            result = main.geocode_location(requested_address)

        self.assertEqual(result["source"], "arcgis-pointaddress")
        self.assertEqual(result["match_quality"], "high")

    def test_reverse_geocode_location_uses_arcgis_without_custom_nominatim(self):
        with patch.dict(
            environ,
            {
                "GEOCODER_PROVIDER": "hybrid",
                "GEOCODER_NOMINATIM_DOMAIN": "",
            },
            clear=False,
        ):
            with patch.object(
                main,
                "reverse_geocode_arcgis_location",
                return_value={
                    "address": {
                        "Address": "12518 Boheme Dr",
                        "LongLabel": "12518 Boheme Dr, Houston, TX, 77024, USA",
                        "AddNum": "12518",
                        "City": "Houston",
                        "RegionAbbr": "TX",
                        "Postal": "77024",
                        "CntryName": "United States",
                    }
                },
            ) as arcgis_mock:
                with patch.object(main.geolocator, "reverse", side_effect=AssertionError("should not call Nominatim reverse")):
                    location = main.reverse_geocode_location(29.767836, -95.551491)

        arcgis_mock.assert_called_once_with(29.767836, -95.551491)
        self.assertEqual(location.raw.get("source"), "arcgis-reverse")
        self.assertEqual(location.raw.get("address", {}).get("postcode"), "77024")

    def test_score_address_match_normalizes_street_suffixes(self):
        requested_address = {
            "street": "12518 Boheme Dr",
            "city": "Houston",
            "state": "TX",
            "zip": "77024",
            "country": "United States",
        }
        candidate_address = {
            "street": "12518 Boheme Drive",
            "city": "Houston",
            "state": "Texas",
            "zip": "77024",
            "country": "United States",
        }

        score = main.score_address_match(
            requested_address,
            candidate_address,
            "12518 Boheme Drive, Houston, Texas 77024, United States",
        )

        self.assertGreaterEqual(score, 22)

    def test_geocode_location_prefers_reverse_confirmed_candidate(self):
        requested_address = {
            "street": "12518 Boheme Dr",
            "city": "Houston",
            "state": "TX",
            "zip": "77024",
            "country": "United States",
        }
        block_off_candidate = build_candidate(1, 29.767210, -95.550680, "12518", "Boheme Drive")
        exact_candidate = build_candidate(2, 29.766980, -95.550910, "12518", "Boheme Drive")
        geocode_results = iter(
            [
                [block_off_candidate, exact_candidate],
                [block_off_candidate, exact_candidate],
            ]
        )
        reverse_lookup = {
            "29.76721, -95.55068": FakeLocation(
                address="12502 Boheme Drive, Houston, Texas 77024, United States",
                latitude=29.767210,
                longitude=-95.550680,
                raw={"address": build_raw_address("12502", "Boheme Drive")},
            ),
            "29.76698, -95.55091": FakeLocation(
                address="12518 Boheme Drive, Houston, Texas 77024, United States",
                latitude=29.766980,
                longitude=-95.550910,
                raw={"address": build_raw_address("12518", "Boheme Drive")},
            ),
        }

        with patch.dict(
            environ,
            {
                "GEOCODER_PROVIDER": "hybrid",
                "GEOCODER_NOMINATIM_DOMAIN": "nominatim.internal.example",
            },
            clear=False,
        ):
            with patch.object(main.geolocator, "geocode", side_effect=lambda *args, **kwargs: next(geocode_results)):
                with patch.object(main.geolocator, "reverse", side_effect=lambda query, **kwargs: reverse_lookup[query]):
                    with patch.object(main, "fetch_arcgis_point_address", return_value=None):
                        result = main.geocode_location(requested_address)

        self.assertIs(result["location"], exact_candidate)
        self.assertEqual(result["match_quality"], "high")

    def test_geocode_location_uses_arcgis_point_address_for_road_backed_house_match(self):
        requested_address = {
            "street": "12518 Boheme Dr",
            "city": "Houston",
            "state": "TX",
            "zip": "77024",
            "country": "United States",
        }
        road_backed_candidate = FakeLocation(
            address="12518, Boheme Drive, Houston, Texas 77024, United States",
            latitude=29.767210,
            longitude=-95.550680,
            raw={
                "address": build_raw_address("12518", "Boheme Drive"),
                "osm_type": "way",
                "osm_id": 15357050,
                "class": "place",
                "type": "house",
                "addresstype": "place",
                "boundingbox": ["29.767160", "29.767260", "-95.550730", "-95.550630"],
            },
        )
        arcgis_location = main.build_location(
            "12518 Boheme Dr, Houston, Texas, 77024",
            29.767836,
            -95.551491,
            {
                "provider": "arcgis",
                "source": "arcgis-pointaddress",
                "boundingbox": ["29.767092", "29.769092", "-95.552499", "-95.550499"],
                "address": {
                    "house_number": "12518",
                    "road": "Boheme Dr",
                    "city": "Houston",
                    "state": "TX",
                    "postcode": "77024",
                    "country": "United States",
                },
            },
        )

        with patch.dict(
            environ,
            {
                "GEOCODER_PROVIDER": "hybrid",
                "GEOCODER_NOMINATIM_DOMAIN": "nominatim.internal.example",
            },
            clear=False,
        ):
            with patch.object(main.geolocator, "geocode", side_effect=[[road_backed_candidate], [road_backed_candidate]]):
                with patch.object(
                    main.geolocator,
                    "reverse",
                    return_value=FakeLocation(
                        address="12518 Boheme Drive, Houston, Texas 77024, United States",
                        latitude=29.767210,
                        longitude=-95.550680,
                        raw={"address": build_raw_address("12518", "Boheme Drive")},
                    ),
                ):
                    with patch.object(main, "fetch_arcgis_point_address", return_value=arcgis_location):
                        with patch.object(
                            main,
                            "reverse_geocode_arcgis_location",
                            return_value={
                                "address": {
                                    "Address": "12518 Boheme Dr",
                                    "LongLabel": "12518 Boheme Dr, Houston, TX, 77024, USA",
                                    "AddNum": "12518",
                                    "City": "Houston",
                                    "RegionAbbr": "TX",
                                    "Postal": "77024",
                                    "CntryName": "United States",
                                }
                            },
                        ):
                            result = main.geocode_location(requested_address)

        self.assertEqual(result["source"], "arcgis-pointaddress")
        self.assertAlmostEqual(result["location"].latitude, 29.767836, places=6)
        self.assertAlmostEqual(result["location"].longitude, -95.551491, places=6)

    def test_geocode_location_uses_arcgis_candidate_when_nominatim_reverse_disagrees(self):
        requested_address = {
            "street": "12518 Boheme Dr",
            "city": "Houston",
            "state": "TX",
            "zip": "77024",
            "country": "United States",
        }
        nominatim_candidate = FakeLocation(
            address="12518, Boheme Drive, Houston, Texas 77024, United States",
            latitude=29.767210,
            longitude=-95.550680,
            raw={
                "address": build_raw_address("12518", "Boheme Drive"),
                "osm_type": "way",
                "osm_id": 15357050,
                "type": "house",
                "addresstype": "place",
                "boundingbox": ["29.767160", "29.767260", "-95.550730", "-95.550630"],
            },
        )
        arcgis_location = main.build_location(
            "12518 Boheme Dr, Houston, Texas, 77024",
            29.768092,
            -95.551499,
            {
                "provider": "arcgis",
                "source": "arcgis-pointaddress",
                "boundingbox": ["29.767092", "29.769092", "-95.552499", "-95.550499"],
                "address": {
                    "house_number": "12518",
                    "road": "Boheme Dr",
                    "city": "Houston",
                    "state": "TX",
                    "postcode": "77024",
                    "country": "United States",
                },
            },
        )

        with patch.dict(
            environ,
            {
                "GEOCODER_PROVIDER": "hybrid",
                "GEOCODER_NOMINATIM_DOMAIN": "nominatim.internal.example",
            },
            clear=False,
        ):
            with patch.object(main.geolocator, "geocode", side_effect=[[nominatim_candidate], [nominatim_candidate]]):
                with patch.object(
                    main.geolocator,
                    "reverse",
                    return_value=FakeLocation(
                        address="CVS Pharmacy, Benignus Road, Houston, Texas 77024, United States",
                        latitude=29.7670505,
                        longitude=-95.5505829,
                        raw={
                            "address": {
                                "road": "Benignus Road",
                                "city": "Houston",
                                "state": "Texas",
                                "postcode": "77024",
                                "country": "United States",
                            }
                        },
                    ),
                ):
                    with patch.object(main, "fetch_arcgis_point_address", return_value=arcgis_location):
                        with patch.object(
                            main,
                            "reverse_geocode_arcgis_location",
                            return_value={
                                "address": {
                                    "Address": "12518 Boheme Dr",
                                    "LongLabel": "12518 Boheme Dr, Houston, TX, 77024, USA",
                                    "AddNum": "12518",
                                    "City": "Houston",
                                    "RegionAbbr": "TX",
                                    "Postal": "77024",
                                    "CntryName": "United States",
                                }
                            },
                        ):
                            result = main.geocode_location(requested_address)

        self.assertEqual(result["source"], "arcgis-pointaddress")
        self.assertAlmostEqual(result["location"].latitude, 29.768092, places=6)
        self.assertAlmostEqual(result["location"].longitude, -95.551499, places=6)


class GeocodeCacheTests(unittest.TestCase):
    def setUp(self):
        data_persistence.reset_memory_storage()
        self.client = TestClient(main.app)

    def tearDown(self):
        data_persistence.reset_memory_storage()

    def test_property_preview_uses_cached_forward_result(self):
        requested_address = {
            "street": "12518 Boheme Dr",
            "city": "Houston",
            "state": "TX",
            "zip": "77024",
            "country": "United States",
        }
        candidate = build_candidate(99, 29.766980, -95.550910, "12518", "Boheme Drive")
        geocode_payload = {
            "location": candidate,
            "match_quality": "high",
            "match_score": 31,
            "source": "test-provider",
        }

        with patch.object(main, "geocode_location", return_value=geocode_payload) as geocode_mock:
            first_response = self.client.post("/api/property-preview", json=requested_address)
            second_response = self.client.post("/api/property-preview", json=requested_address)

        self.assertEqual(first_response.status_code, 200)
        self.assertEqual(second_response.status_code, 200)
        self.assertEqual(geocode_mock.call_count, 1)
        self.assertEqual(
            first_response.json()["formatted_address"],
            second_response.json()["formatted_address"],
        )


if __name__ == "__main__":
    unittest.main()
