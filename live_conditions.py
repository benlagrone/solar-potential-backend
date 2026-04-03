from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import time
from typing import Any, Optional

import requests


logger = logging.getLogger(__name__)

_CACHE: dict[tuple[str, tuple[tuple[str, str], ...]], dict[str, Any]] = {}

NOAA_SCALES_URL = "https://services.swpc.noaa.gov/products/noaa-scales.json"
NOAA_ALERTS_URL = "https://services.swpc.noaa.gov/products/alerts.json"
NOAA_PLASMA_URL = "https://services.swpc.noaa.gov/products/solar-wind/plasma-2-hour.json"
NOAA_XRAY_URL = "https://services.swpc.noaa.gov/json/goes/primary/xrays-1-day.json"
NASA_DONKI_FLR_URL = "https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/get/FLR"
NASA_DONKI_GST_URL = "https://kauai.ccmc.gsfc.nasa.gov/DONKI/WS/get/GST"
OPEN_METEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


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

    response = requests.get(url, params=params, timeout=15)
    response.raise_for_status()
    data = response.json()
    _CACHE[key] = {
        "data": data,
        "expires_at": now + ttl_seconds,
    }
    return data


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _optional_float(value: Any):
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _average(values: list[Optional[float]]):
    numeric_values = [value for value in values if value is not None]
    if not numeric_values:
        return None
    return sum(numeric_values) / len(numeric_values)


def _round_float(value: Optional[float], digits: int = 1):
    if value is None:
        return None
    return round(value, digits)


def _celsius_to_fahrenheit(value: Optional[float]):
    if value is None:
        return None
    return (value * 9 / 5) + 32


def _megajoules_to_kwh(value: Optional[float]):
    if value is None:
        return None
    return value / 3.6


def _parse_time(value: Optional[str]):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_scale(bucket: Optional[dict[str, Any]]) -> dict[str, Any]:
    bucket = bucket or {}
    scale_value = bucket.get("Scale")
    try:
        normalized_scale = int(scale_value) if scale_value not in (None, "") else 0
    except (TypeError, ValueError):
        normalized_scale = 0

    return {
        "scale": normalized_scale,
        "label": str(bucket.get("Text") or "none").lower(),
        "probability_minor": bucket.get("MinorProb"),
        "probability_major": bucket.get("MajorProb"),
        "probability": bucket.get("Prob"),
    }


def _extract_scale_entry(scales_payload: Any, key: str) -> dict[str, Any]:
    if not isinstance(scales_payload, dict):
        return {}
    return scales_payload.get(key) or {}


def _format_xray_class(flux_value: Any) -> str:
    flux = max(_safe_float(flux_value), 0.0)
    if flux >= 1e-4:
        return f"X{flux / 1e-4:.1f}"
    if flux >= 1e-5:
        return f"M{flux / 1e-5:.1f}"
    if flux >= 1e-6:
        return f"C{flux / 1e-6:.1f}"
    if flux >= 1e-7:
        return f"B{flux / 1e-7:.1f}"
    return f"A{flux / 1e-8:.1f}"


def _extract_alert_headline(message: str) -> str:
    for raw_line in (message or "").splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if any(token in line for token in ("WARNING:", "WATCH:", "ALERT:")):
            return line
    return ""


def _categorize_alert_level(headline: str) -> str:
    upper_headline = headline.upper()
    if "WARNING:" in upper_headline:
        return "warning"
    if "WATCH:" in upper_headline:
        return "watch"
    if "ALERT:" in upper_headline:
        return "alert"
    return "info"


def _latitude_band(latitude: float) -> str:
    absolute_latitude = abs(latitude)
    if absolute_latitude >= 55:
        return "high"
    if absolute_latitude >= 40:
        return "mid"
    if absolute_latitude >= 25:
        return "low"
    return "equatorial"


def _aurora_potential(geomagnetic_scale: int, latitude: float, is_daylight: bool) -> str:
    if is_daylight or geomagnetic_scale <= 0:
        return "low"

    absolute_latitude = abs(latitude)
    if geomagnetic_scale >= 3 and absolute_latitude >= 45:
        return "likely"
    if geomagnetic_scale >= 2 and absolute_latitude >= 50:
        return "possible"
    if geomagnetic_scale >= 1 and absolute_latitude >= 55:
        return "possible"
    return "low"


def _hf_radio_risk(radio_blackout_scale: int, is_daylight: bool) -> str:
    if not is_daylight or radio_blackout_scale <= 0:
        return "low"
    if radio_blackout_scale >= 3:
        return "high"
    if radio_blackout_scale >= 1:
        return "moderate"
    return "low"


def _gnss_risk(geomagnetic_scale: int, radiation_scale: int, latitude: float) -> str:
    absolute_latitude = abs(latitude)
    if geomagnetic_scale >= 3:
        return "high"
    if geomagnetic_scale >= 2 and absolute_latitude >= 40:
        return "moderate"
    if radiation_scale >= 2 and absolute_latitude >= 50:
        return "moderate"
    return "low"


def _space_weather_alert_level(
    radio_blackout_scale: int,
    radiation_scale: int,
    geomagnetic_scale: int,
    is_daylight: bool,
    latitude: float,
    warning_count: int,
    watch_count: int,
) -> str:
    absolute_latitude = abs(latitude)

    if radio_blackout_scale >= 2 and is_daylight:
        return "alert"
    if geomagnetic_scale >= 3 and absolute_latitude >= 45:
        return "alert"
    if radiation_scale >= 2 and absolute_latitude >= 55:
        return "alert"
    if warning_count > 0:
        return "watch"
    if radio_blackout_scale >= 1 or radiation_scale >= 1 or geomagnetic_scale >= 2 or watch_count > 0:
        return "watch"
    return "low"


def _build_space_weather_summary(
    alert_level: str,
    radio_blackout_scale: int,
    radiation_scale: int,
    geomagnetic_scale: int,
    is_daylight: bool,
    aurora_potential: str,
    gnss_risk: str,
) -> str:
    if alert_level == "alert" and radio_blackout_scale >= 2 and is_daylight:
        return (
            "A flare-driven radio blackout signal is active for the sunlit side of Earth. "
            "Communication effects matter more here than any residential ground-level radiation concern."
        )

    if alert_level == "alert" and geomagnetic_scale >= 3:
        return (
            "Geomagnetic conditions are elevated enough to matter locally. Expect stronger aurora "
            "potential and a higher chance of GNSS instability."
        )

    if alert_level == "watch" and geomagnetic_scale >= 1:
        summary = "Geomagnetic conditions are elevated but not extreme for this property."
        if aurora_potential in {"possible", "likely"}:
            summary += " Aurora chances improve after dark."
        if gnss_risk != "low":
            summary += " Minor GNSS instability is possible."
        return summary

    if radiation_scale >= 1:
        return (
            "Solar activity is elevated, but the strongest radiation-storm effects remain more "
            "relevant for high-latitude or high-altitude operations than for a homeowner on the ground."
        )

    return "Space-weather conditions are quiet to minor for this location right now."


def _find_hour_index(times: list[str], current_time: Optional[str]) -> int:
    if not times:
        return 0

    if current_time:
        current_hour = current_time[:13] + ":00"
        for index, value in enumerate(times):
            if value == current_hour:
                return index

    return max(0, len(times) - 1)


def _intensity_level(ghi_value: float, is_daylight: bool) -> str:
    if not is_daylight or ghi_value < 50:
        return "low"
    if ghi_value >= 700:
        return "high"
    if ghi_value >= 350:
        return "moderate"
    return "low"


def _spike_level(max_hourly_ramp: float, peak_ghi: float, is_daylight: bool) -> str:
    if not is_daylight and peak_ghi < 250:
        return "low"
    if max_hourly_ramp >= 350 or peak_ghi >= 900:
        return "alert"
    if max_hourly_ramp >= 200 or peak_ghi >= 700:
        return "watch"
    return "low"


def _build_surface_summary(
    is_daylight: bool,
    current_ghi: float,
    peak_time: Optional[str],
    peak_ghi: float,
    spike_level: str,
    next_hour_change: Optional[float],
) -> str:
    if not is_daylight:
        if peak_time and peak_ghi > 0:
            return (
                f"It is currently dark at this property. The next daylight peak in the forecast is "
                f"around {peak_time.split('T')[-1][:5]} local time at about {round(peak_ghi)} W/m²."
            )
        return "It is currently dark at this property, so surface irradiance is minimal."

    if spike_level == "alert":
        return (
            f"Surface sunlight is very strong with sharp hour-to-hour movement. Current GHI is about "
            f"{round(current_ghi)} W/m² and the forecast peak reaches about {round(peak_ghi)} W/m²."
        )

    if spike_level == "watch":
        direction = "up" if (next_hour_change or 0) >= 0 else "down"
        magnitude = abs(round(next_hour_change or 0))
        return (
            f"Surface irradiance is active enough to watch. Current GHI is about {round(current_ghi)} W/m², "
            f"with the next hour shifting about {magnitude} W/m² {direction}."
        )

    return (
        f"Surface sunlight is stable right now at about {round(current_ghi)} W/m², with the next peak "
        f"near {peak_time.split('T')[-1][:5] if peak_time else 'the upcoming daylight window'} local time."
    )


def _build_recent_headlines(alerts_payload: Any) -> tuple[int, int, int, list[str]]:
    warning_count = 0
    watch_count = 0
    alert_count = 0
    headlines: list[str] = []

    if not isinstance(alerts_payload, list):
        return warning_count, watch_count, alert_count, headlines

    for item in alerts_payload:
        headline = _extract_alert_headline(str(item.get("message") or ""))
        if not headline:
            continue
        level = _categorize_alert_level(headline)
        if level == "warning":
            warning_count += 1
        elif level == "watch":
            watch_count += 1
        elif level == "alert":
            alert_count += 1
        if len(headlines) < 3:
            headlines.append(headline)

    return warning_count, watch_count, alert_count, headlines


def _historical_climate_window():
    current_date = datetime.now(timezone.utc).date()
    end_year = current_date.year - 1
    start_year = end_year - 9
    return f"{start_year}-01-01", f"{end_year}-12-31", start_year, end_year


def _build_hardiness_band(avg_annual_extreme_min_f: Optional[float]) -> dict[str, Any]:
    if avg_annual_extreme_min_f is None:
        return {
            "label": None,
            "average_annual_extreme_min_f": None,
            "range_f": None,
            "estimated": True,
        }

    band_index = int((avg_annual_extreme_min_f + 60) // 5)
    clamped_index = max(0, min(25, band_index))
    zone_number = (clamped_index // 2) + 1
    subzone = "a" if clamped_index % 2 == 0 else "b"
    lower_bound = -60 + (clamped_index * 5)
    upper_bound = lower_bound + 5

    if clamped_index == 0 and avg_annual_extreme_min_f < -60:
        range_f = "Below -60°F"
    elif clamped_index == 25 and avg_annual_extreme_min_f >= 65:
        range_f = "65°F and warmer"
    else:
        range_f = f"{lower_bound:.0f} to {upper_bound:.0f}°F"

    return {
        "label": f"{zone_number}{subzone}",
        "average_annual_extreme_min_f": _round_float(avg_annual_extreme_min_f, 1),
        "range_f": range_f,
        "estimated": True,
    }


def _build_climate_extreme(
    monthly_profiles: dict[str, dict[str, Any]],
    profile_key: str,
    value_key: str,
    maximize: bool = True,
):
    candidates = []
    for month_key, profile in monthly_profiles.items():
        value = profile.get(profile_key)
        if value is None:
            continue
        candidates.append((month_key, value))

    if not candidates:
        return None

    month_key, value = (
        max(candidates, key=lambda candidate: candidate[1])
        if maximize
        else min(candidates, key=lambda candidate: candidate[1])
    )
    return {
        "month": month_key,
        value_key: value,
    }


def _build_open_meteo_payload(latitude: float, longitude: float, time_zone_name: str):
    return _fetch_json(
        OPEN_METEO_FORECAST_URL,
        params={
            "latitude": round(latitude, 6),
            "longitude": round(longitude, 6),
            "timezone": time_zone_name or "UTC",
            "past_hours": 2,
            "forecast_hours": 24,
            "current": "is_day,shortwave_radiation,direct_normal_irradiance",
            "hourly": "shortwave_radiation,direct_normal_irradiance",
        },
        ttl_seconds=600,
    )


def _build_open_meteo_historical_payload(latitude: float, longitude: float, time_zone_name: str):
    start_date, end_date, _, _ = _historical_climate_window()
    return _fetch_json(
        OPEN_METEO_ARCHIVE_URL,
        params={
            "latitude": round(latitude, 6),
            "longitude": round(longitude, 6),
            "timezone": time_zone_name or "UTC",
            "start_date": start_date,
            "end_date": end_date,
            "daily": (
                "temperature_2m_mean,"
                "temperature_2m_min,"
                "relative_humidity_2m_mean,"
                "shortwave_radiation_sum"
            ),
        },
        ttl_seconds=86400,
    )


def get_surface_irradiance_snapshot(latitude: float, longitude: float, time_zone_name: str):
    forecast_payload = _build_open_meteo_payload(latitude, longitude, time_zone_name)
    current = forecast_payload.get("current") or {}
    hourly = forecast_payload.get("hourly") or {}
    hourly_times = list(hourly.get("time") or [])
    hourly_ghi = [_safe_float(value) for value in (hourly.get("shortwave_radiation") or [])]
    hourly_dni = [_safe_float(value) for value in (hourly.get("direct_normal_irradiance") or [])]
    current_time = current.get("time")
    current_index = _find_hour_index(hourly_times, current_time)
    is_daylight = bool(current.get("is_day"))
    current_ghi = round(_safe_float(current.get("shortwave_radiation")), 1)
    current_dni = round(_safe_float(current.get("direct_normal_irradiance")), 1)
    previous_hour_ghi = round(hourly_ghi[current_index - 1], 1) if current_index > 0 else None
    next_hour_ghi = round(hourly_ghi[current_index + 1], 1) if current_index + 1 < len(hourly_ghi) else None
    change_from_previous_hour = (
        round(current_ghi - previous_hour_ghi, 1) if previous_hour_ghi is not None else None
    )
    change_to_next_hour = round(next_hour_ghi - current_ghi, 1) if next_hour_ghi is not None else None

    future_times = hourly_times[current_index:] or hourly_times
    future_ghi = hourly_ghi[current_index:] or hourly_ghi
    future_dni = hourly_dni[current_index:] or hourly_dni

    peak_index = 0
    if future_ghi:
        peak_index = max(range(len(future_ghi)), key=lambda index: future_ghi[index])
    peak_time = future_times[peak_index] if future_times else current_time
    peak_ghi = round(future_ghi[peak_index], 1) if future_ghi else current_ghi
    peak_dni = round(future_dni[peak_index], 1) if future_dni else current_dni

    max_hourly_ramp = 0.0
    ramp_start_time = None
    ramp_end_time = None
    for index in range(max(current_index - 1, 0), max(len(hourly_ghi) - 1, 0)):
        left_value = hourly_ghi[index]
        right_value = hourly_ghi[index + 1]
        ramp_value = abs(right_value - left_value)
        if ramp_value > max_hourly_ramp:
            max_hourly_ramp = ramp_value
            ramp_start_time = hourly_times[index]
            ramp_end_time = hourly_times[index + 1]

    intensity_level = _intensity_level(current_ghi, is_daylight)
    spike_level = _spike_level(max_hourly_ramp, peak_ghi, is_daylight)
    hourly_profile = []
    for index, time_value in enumerate(future_times[:8]):
        hourly_profile.append(
            {
                "time": time_value,
                "ghi_w_m2": round(future_ghi[index], 1),
                "dni_w_m2": round(future_dni[index], 1),
            }
        )

    return {
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "time_zone": forecast_payload.get("timezone") or time_zone_name or "UTC",
        "observed_at": current_time,
        "source": "open-meteo-forecast",
        "is_daylight": is_daylight,
        "current": {
            "ghi_w_m2": current_ghi,
            "dni_w_m2": current_dni,
            "intensity_level": intensity_level,
        },
        "trend": {
            "previous_hour_ghi_w_m2": previous_hour_ghi,
            "change_from_previous_hour_w_m2": change_from_previous_hour,
            "next_hour_ghi_w_m2": next_hour_ghi,
            "change_to_next_hour_w_m2": change_to_next_hour,
            "max_hourly_ramp_w_m2": round(max_hourly_ramp, 1),
            "ramp_start_time": ramp_start_time,
            "ramp_end_time": ramp_end_time,
        },
        "next_peak": {
            "time": peak_time,
            "ghi_w_m2": peak_ghi,
            "dni_w_m2": peak_dni,
        },
        "spike_level": spike_level,
        "summary": _build_surface_summary(
            is_daylight,
            current_ghi,
            peak_time,
            peak_ghi,
            spike_level,
            change_to_next_hour,
        ),
        "hourly_profile": hourly_profile,
    }


def get_property_climate_snapshot(latitude: float, longitude: float, time_zone_name: str):
    historical_payload = _build_open_meteo_historical_payload(latitude, longitude, time_zone_name)
    daily = historical_payload.get("daily") or {}
    daily_times = list(daily.get("time") or [])
    daily_temperature_mean = list(daily.get("temperature_2m_mean") or [])
    daily_temperature_min = list(daily.get("temperature_2m_min") or [])
    daily_relative_humidity = list(daily.get("relative_humidity_2m_mean") or [])
    daily_shortwave_radiation = list(daily.get("shortwave_radiation_sum") or [])

    growing_season_months = {"04", "05", "06", "07", "08", "09"}
    monthly_buckets = {
        f"{month:02d}": {
            "temperature_c": [],
            "relative_humidity": [],
            "shortwave_radiation_mj_m2": [],
        }
        for month in range(1, 13)
    }
    annual_temperature_values: list[Optional[float]] = []
    annual_humidity_values: list[Optional[float]] = []
    annual_radiation_values: list[Optional[float]] = []
    growing_temperature_values: list[Optional[float]] = []
    growing_humidity_values: list[Optional[float]] = []
    growing_radiation_values: list[Optional[float]] = []
    yearly_extreme_mins_f: dict[str, list[float]] = {}

    for time_value, temperature_mean, temperature_min, relative_humidity, shortwave_radiation in zip(
        daily_times,
        daily_temperature_mean,
        daily_temperature_min,
        daily_relative_humidity,
        daily_shortwave_radiation,
    ):
        month_key = str(time_value)[5:7]
        year_key = str(time_value)[:4]
        mean_temperature_c = _optional_float(temperature_mean)
        min_temperature_c = _optional_float(temperature_min)
        mean_relative_humidity = _optional_float(relative_humidity)
        total_shortwave_radiation_mj = _optional_float(shortwave_radiation)

        if month_key not in monthly_buckets:
            continue

        monthly_buckets[month_key]["temperature_c"].append(mean_temperature_c)
        monthly_buckets[month_key]["relative_humidity"].append(mean_relative_humidity)
        monthly_buckets[month_key]["shortwave_radiation_mj_m2"].append(total_shortwave_radiation_mj)

        annual_temperature_values.append(mean_temperature_c)
        annual_humidity_values.append(mean_relative_humidity)
        annual_radiation_values.append(total_shortwave_radiation_mj)

        if month_key in growing_season_months:
            growing_temperature_values.append(mean_temperature_c)
            growing_humidity_values.append(mean_relative_humidity)
            growing_radiation_values.append(total_shortwave_radiation_mj)

        min_temperature_f = _celsius_to_fahrenheit(min_temperature_c)
        if min_temperature_f is not None:
            yearly_extreme_mins_f.setdefault(year_key, []).append(min_temperature_f)

    monthly_profiles = {}
    for month_key, bucket in monthly_buckets.items():
        avg_temperature_c = _average(bucket["temperature_c"])
        avg_relative_humidity = _average(bucket["relative_humidity"])
        avg_shortwave_radiation_mj = _average(bucket["shortwave_radiation_mj_m2"])
        avg_temperature_f = _celsius_to_fahrenheit(avg_temperature_c)
        avg_shortwave_radiation_kwh = _megajoules_to_kwh(avg_shortwave_radiation_mj)

        monthly_profiles[month_key] = {
            "average_temperature_c": _round_float(avg_temperature_c, 1),
            "average_temperature_f": _round_float(avg_temperature_f, 1),
            "average_relative_humidity": _round_float(avg_relative_humidity, 1),
            "average_daily_shortwave_radiation_mj_m2": _round_float(avg_shortwave_radiation_mj, 2),
            "average_daily_shortwave_radiation_kwh_m2": _round_float(avg_shortwave_radiation_kwh, 2),
        }

    avg_annual_extreme_min_f = _average(
        [min(values) for values in yearly_extreme_mins_f.values() if values]
    )
    annual_avg_temperature_c = _average(annual_temperature_values)
    annual_avg_temperature_f = _celsius_to_fahrenheit(annual_avg_temperature_c)
    annual_avg_humidity = _average(annual_humidity_values)
    annual_avg_radiation_mj = _average(annual_radiation_values)
    annual_avg_radiation_kwh = _megajoules_to_kwh(annual_avg_radiation_mj)
    growing_avg_temperature_c = _average(growing_temperature_values)
    growing_avg_temperature_f = _celsius_to_fahrenheit(growing_avg_temperature_c)
    growing_avg_humidity = _average(growing_humidity_values)
    growing_avg_radiation_mj = _average(growing_radiation_values)
    growing_avg_radiation_kwh = _megajoules_to_kwh(growing_avg_radiation_mj)
    period_start, period_end, start_year, end_year = _historical_climate_window()
    hardiness_band = _build_hardiness_band(avg_annual_extreme_min_f)

    return {
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "time_zone": historical_payload.get("timezone") or time_zone_name or "UTC",
        "source": "open-meteo-historical-weather",
        "period_start": period_start,
        "period_end": period_end,
        "years_sampled": max(end_year - start_year + 1, 0),
        "season_window": {
            "label": "April-September",
            "start_month": "04",
            "end_month": "09",
        },
        "hardiness_zone": hardiness_band,
        "annual": {
            "average_temperature_c": _round_float(annual_avg_temperature_c, 1),
            "average_temperature_f": _round_float(annual_avg_temperature_f, 1),
            "average_relative_humidity": _round_float(annual_avg_humidity, 1),
            "average_daily_shortwave_radiation_mj_m2": _round_float(annual_avg_radiation_mj, 2),
            "average_daily_shortwave_radiation_kwh_m2": _round_float(annual_avg_radiation_kwh, 2),
        },
        "growing_season": {
            "label": "April-September",
            "average_temperature_c": _round_float(growing_avg_temperature_c, 1),
            "average_temperature_f": _round_float(growing_avg_temperature_f, 1),
            "average_relative_humidity": _round_float(growing_avg_humidity, 1),
            "average_daily_shortwave_radiation_mj_m2": _round_float(growing_avg_radiation_mj, 2),
            "average_daily_shortwave_radiation_kwh_m2": _round_float(growing_avg_radiation_kwh, 2),
        },
        "monthly_profiles": monthly_profiles,
        "monthly_temperature_f": {
            month_key: profile.get("average_temperature_f")
            for month_key, profile in monthly_profiles.items()
        },
        "monthly_relative_humidity": {
            month_key: profile.get("average_relative_humidity")
            for month_key, profile in monthly_profiles.items()
        },
        "monthly_shortwave_radiation_kwh_m2": {
            month_key: profile.get("average_daily_shortwave_radiation_kwh_m2")
            for month_key, profile in monthly_profiles.items()
        },
        "seasonal_extremes": {
            "warmest_month": _build_climate_extreme(
                monthly_profiles,
                "average_temperature_f",
                "temperature_f",
            ),
            "coolest_month": _build_climate_extreme(
                monthly_profiles,
                "average_temperature_f",
                "temperature_f",
                maximize=False,
            ),
            "most_humid_month": _build_climate_extreme(
                monthly_profiles,
                "average_relative_humidity",
                "relative_humidity",
            ),
            "sunniest_month": _build_climate_extreme(
                monthly_profiles,
                "average_daily_shortwave_radiation_kwh_m2",
                "shortwave_radiation_kwh_m2",
            ),
        },
        "summary": (
            f"Estimated hardiness band {hardiness_band.get('label') or 'unknown'} using {start_year}-{end_year} "
            f"historical weather. Average growing-season conditions run about "
            f"{_round_float(growing_avg_temperature_f, 1) or 0:.1f}°F and "
            f"{_round_float(growing_avg_humidity, 0) or 0:.0f}% relative humidity."
        ),
        "model_note": (
            "Property-level climate averages from historical weather. This does not model "
            "parcel-specific shade, tree canopy, soil, or irrigation conditions."
        ),
    }


def get_space_weather_snapshot(latitude: float, longitude: float, time_zone_name: str):
    scales_payload = _fetch_json(NOAA_SCALES_URL, ttl_seconds=60)
    alerts_payload = _fetch_json(NOAA_ALERTS_URL, ttl_seconds=60)
    plasma_payload = _fetch_json(NOAA_PLASMA_URL, ttl_seconds=60)
    xray_payload = _fetch_json(NOAA_XRAY_URL, ttl_seconds=60)

    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=7)
    flares_payload = _fetch_json(
        NASA_DONKI_FLR_URL,
        params={
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        },
        ttl_seconds=1800,
    )
    storms_payload = _fetch_json(
        NASA_DONKI_GST_URL,
        params={
            "startDate": start_date.isoformat(),
            "endDate": end_date.isoformat(),
        },
        ttl_seconds=1800,
    )
    irradiance_snapshot = get_surface_irradiance_snapshot(latitude, longitude, time_zone_name)

    current_scale_entry = _extract_scale_entry(scales_payload, "0")
    day_one_scale_entry = _extract_scale_entry(scales_payload, "1")
    day_two_scale_entry = _extract_scale_entry(scales_payload, "2")
    day_three_scale_entry = _extract_scale_entry(scales_payload, "3")
    radio_blackout_scale = _format_scale(current_scale_entry.get("R"))
    radiation_storm_scale = _format_scale(current_scale_entry.get("S"))
    geomagnetic_storm_scale = _format_scale(current_scale_entry.get("G"))

    warning_count, watch_count, alert_count, recent_headlines = _build_recent_headlines(alerts_payload)
    plasma_row = (plasma_payload[-1] if isinstance(plasma_payload, list) and len(plasma_payload) > 1 else [])
    solar_wind = {
        "observed_at": plasma_row[0] if len(plasma_row) > 0 else None,
        "density_p_cm3": round(_safe_float(plasma_row[1]), 2) if len(plasma_row) > 1 else 0.0,
        "speed_km_s": round(_safe_float(plasma_row[2]), 1) if len(plasma_row) > 2 else 0.0,
        "temperature_k": round(_safe_float(plasma_row[3]), 0) if len(plasma_row) > 3 else 0.0,
    }

    long_band_rows = []
    if isinstance(xray_payload, list):
        long_band_rows = [row for row in xray_payload if row.get("energy") == "0.1-0.8nm"]
    long_band_rows.sort(key=lambda row: str(row.get("time_tag") or ""))
    latest_long_band_row = long_band_rows[-1] if long_band_rows else {}
    peak_long_band_row = (
        max(long_band_rows, key=lambda row: _safe_float(row.get("flux"))) if long_band_rows else {}
    )

    latest_flare_event = None
    if isinstance(flares_payload, list) and flares_payload:
        latest_flare = max(
            flares_payload,
            key=lambda event: str(event.get("peakTime") or event.get("beginTime") or ""),
        )
        latest_flare_event = {
            "class": latest_flare.get("classType"),
            "peak_time": latest_flare.get("peakTime") or latest_flare.get("beginTime"),
            "source_location": latest_flare.get("sourceLocation"),
        }

    latest_geomagnetic_storm = None
    if isinstance(storms_payload, list) and storms_payload:
        latest_storm = max(
            storms_payload,
            key=lambda event: str(event.get("startTime") or ""),
        )
        kp_index = 0.0
        for entry in latest_storm.get("allKpIndex") or []:
            kp_index = max(kp_index, _safe_float(entry.get("kpIndex")))
        latest_geomagnetic_storm = {
            "start_time": latest_storm.get("startTime"),
            "max_kp_index": round(kp_index, 2),
        }

    is_daylight = bool(irradiance_snapshot.get("is_daylight"))
    latitude_band = _latitude_band(latitude)
    aurora_potential = _aurora_potential(geomagnetic_storm_scale["scale"], latitude, is_daylight)
    hf_radio_risk = _hf_radio_risk(radio_blackout_scale["scale"], is_daylight)
    gnss_risk = _gnss_risk(
        geomagnetic_storm_scale["scale"],
        radiation_storm_scale["scale"],
        latitude,
    )
    ground_note = (
        "No elevated residential ground-level concern."
        if radiation_storm_scale["scale"] < 3
        else "Elevated concern stays weighted toward high-latitude or high-altitude operations, not typical residential ground level."
    )
    alert_level = _space_weather_alert_level(
        radio_blackout_scale["scale"],
        radiation_storm_scale["scale"],
        geomagnetic_storm_scale["scale"],
        is_daylight,
        latitude,
        warning_count,
        watch_count,
    )

    summary = _build_space_weather_summary(
        alert_level,
        radio_blackout_scale["scale"],
        radiation_storm_scale["scale"],
        geomagnetic_storm_scale["scale"],
        is_daylight,
        aurora_potential,
        gnss_risk,
    )

    return {
        "latitude": round(latitude, 6),
        "longitude": round(longitude, 6),
        "time_zone": irradiance_snapshot.get("time_zone") or time_zone_name or "UTC",
        "observed_at": solar_wind["observed_at"] or latest_long_band_row.get("time_tag"),
        "global": {
            "radio_blackout_scale": radio_blackout_scale,
            "radiation_storm_scale": radiation_storm_scale,
            "geomagnetic_storm_scale": geomagnetic_storm_scale,
            "solar_wind": solar_wind,
            "current_xray_class": _format_xray_class(latest_long_band_row.get("flux")),
            "peak_xray_24h_class": _format_xray_class(peak_long_band_row.get("flux")),
            "lookahead": {
                "day_1": {
                    "date": day_one_scale_entry.get("DateStamp"),
                    "radio_blackout": _format_scale(day_one_scale_entry.get("R")),
                    "radiation_storm": _format_scale(day_one_scale_entry.get("S")),
                    "geomagnetic_storm": _format_scale(day_one_scale_entry.get("G")),
                },
                "day_2": {
                    "date": day_two_scale_entry.get("DateStamp"),
                    "radio_blackout": _format_scale(day_two_scale_entry.get("R")),
                    "radiation_storm": _format_scale(day_two_scale_entry.get("S")),
                    "geomagnetic_storm": _format_scale(day_two_scale_entry.get("G")),
                },
                "day_3": {
                    "date": day_three_scale_entry.get("DateStamp"),
                    "radio_blackout": _format_scale(day_three_scale_entry.get("R")),
                    "radiation_storm": _format_scale(day_three_scale_entry.get("S")),
                    "geomagnetic_storm": _format_scale(day_three_scale_entry.get("G")),
                },
            },
        },
        "local": {
            "is_daylight": is_daylight,
            "latitude_band": latitude_band,
            "aurora_visibility_potential": aurora_potential,
            "hf_radio_risk": hf_radio_risk,
            "gnss_risk": gnss_risk,
            "ground_radiation_note": ground_note,
        },
        "alert_level": alert_level,
        "alert_count": alert_count,
        "watch_count": watch_count,
        "warning_count": warning_count,
        "recent_headlines": recent_headlines,
        "recent_activity": {
            "latest_flare_event": latest_flare_event,
            "latest_geomagnetic_storm": latest_geomagnetic_storm,
        },
        "summary": summary,
        "sources": [
            "noaa-scales",
            "noaa-alerts",
            "noaa-solar-wind",
            "noaa-goes-xray",
            "nasa-donki",
        ],
    }
