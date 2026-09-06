# Home Assistant REST Integration

Solar Buddy exposes a compact read-only endpoint for Home Assistant:

```text
GET /api/home-assistant/snapshot?latitude=<latitude>&longitude=<longitude>
```

The snapshot combines current surface irradiance, cloud cover, geometric solar position, and
localized space weather. Solar position is calculated for the forecast feed's observation time and
requested coordinates. It does not expose device-control, automation, property-record mutation, or
irrigation routes. If one upstream feed fails, the endpoint returns `status: partial` and preserves
the other component.

Every request must include the dedicated API key in the `X-API-Key` header. A missing or incorrect
key returns HTTP `401`. If the backend has no `HOME_ASSISTANT_API_KEY` configured, it fails closed
with HTTP `503`.

## Fortress Runtime Boundary

Solar Buddy currently binds to loopback port `18031` on its Fortress deployment host. Home
Assistant currently runs on `fortress.sextant`, so `127.0.0.1:18031` inside Home Assistant does not
reach Solar Buddy. Do not install the package with that loopback URL.

Set `solar_buddy_snapshot_url` only after the workspace deployment capability provides an approved
route that is reachable from Sextant, for example:

```text
https://<approved-solar-api-host>/api/home-assistant/snapshot?latitude=<latitude>&longitude=<longitude>
```

Keep the credential in Home Assistant secrets and the corresponding backend deployment secret. Do
not place private coordinates or credentials in the committed package.

## Configuration

1. Copy `docs/home-assistant-package.yaml.example` into the Home Assistant packages directory.
2. Put the full URL in Home Assistant's uncommitted `secrets.yaml` as
   `solar_buddy_snapshot_url`, and put the dedicated key there as `solar_buddy_api_key`.
3. Ensure `configuration.yaml` loads that packages directory.
4. Run Home Assistant's configuration check, then restart Home Assistant.

The example polls every five minutes and creates:

- a source-status sensor with the compact response stored as attributes
- global horizontal irradiance and direct normal irradiance sensors in `W/m²`
- total cloud cover as a percentage
- solar azimuth, elevation, and zenith-angle sensors in degrees
- a localized space-weather alert sensor
- a geomagnetic-storm-scale sensor
- a daylight binary sensor

Do not put private coordinates in a committed package. Keep them in Home Assistant's local
`secrets.yaml`.

## Smoke Check

From the Home Assistant host, using the approved reachable route:

```bash
curl --fail --silent --show-error \
  --header 'X-API-Key: <HOME_ASSISTANT_API_KEY>' \
  'https://<approved-solar-api-host>/api/home-assistant/snapshot?latitude=<latitude>&longitude=<longitude>'
```

Confirm that `status` is `ok` or `partial`, then confirm the `sensor.solar_buddy_api` entity and
derived sensors update in Home Assistant. A successful API response proves the data path, not any
physical solar-device control capability.
