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

## Recommended Fortress Runtime Path

During the locked interim both services run on `fortress-phronesis`. Home Assistant uses host
networking and the Solar Buddy backend binds to loopback port `18031`, so the private consumer URL
is:

```text
http://127.0.0.1:18031/api/home-assistant/snapshot?latitude=<latitude>&longitude=<longitude>
```

This keeps the property's coordinates out of public URLs and does not require an API credential.

## Configuration

1. Copy `docs/home-assistant-package.yaml.example` into the Home Assistant packages directory.
2. Put the full URL in Home Assistant's uncommitted `secrets.yaml` as
   `solar_buddy_snapshot_url`.
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

From the Home Assistant host:

```bash
curl --fail --silent --show-error \
  'http://127.0.0.1:18031/api/home-assistant/snapshot?latitude=<latitude>&longitude=<longitude>'
```

Confirm that `status` is `ok` or `partial`, then confirm the `sensor.solar_buddy_api` entity and
derived sensors update in Home Assistant. A successful API response proves the data path, not any
physical solar-device control capability.
