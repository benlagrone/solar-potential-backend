import json
from fastapi import FastAPI, HTTPException, Request
from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.openapi.utils import get_openapi
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import logging
from data_persistence import (
    store_personal_info, store_browser_data, store_solar_data,
    check_existing_address_data, check_existing_solar_data, check_existing_zip_data
)
from google_sheets_service import GoogleSheetsService
import uuid
from geopy.geocoders import Nominatim
from datetime import datetime, timedelta
import requests
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
import ssl
import certifi
from timezonefinder import TimezoneFinder

load_dotenv()

app = FastAPI(docs_url=None, redoc_url=None)  # Disable default docs

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize Google Sheets service
sheets_service = GoogleSheetsService()

# Models
class Address(BaseModel):
    street: str
    city: str
    state: str
    zip: str
    country: str

class BrowserData(BaseModel):
    userAgent: str
    screenResolution: str
    languagePreference: str
    timeZone: str
    referrerUrl: str
    deviceType: str

class UserData(BaseModel):
    address: Address
    browserData: BrowserData

class SolarPotentialRequest(BaseModel):
    guid: str
    system_size: float = 7.0  # in kW, default 7kW
    panel_efficiency: float = 0.20  # default 20%
    electricity_rate: float  # in $/kWh
    installation_cost_per_watt: float = 3.0  # in $/W, default $3/W

# Create a custom SSL context
ctx = ssl.create_default_context(cafile=certifi.where())
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

geolocator = Nominatim(user_agent="solar_potential_app", ssl_context=ctx)

import json
import requests
import logging
from fastapi import HTTPException
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

import requests
import logging
from fastapi import HTTPException
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def get_nasa_power_data(lat: float, lon: float):
    """
    Fetch solar radiation data from NASA POWER API for a given latitude and longitude.
    
    Args:
    lat (float): Latitude of the location
    lon (float): Longitude of the location
    
    Returns:
    dict: A dictionary containing solar radiation data
    """
    base_url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    
    # Calculate date range for the past year
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    params = {
        "parameters": "ALLSKY_SFC_SW_DWN,CLRSKY_SFC_SW_DWN",
        "community": "RE",
        "longitude": lon,
        "latitude": lat,
        "start": start_date.strftime("%Y%m%d"),
        "end": end_date.strftime("%Y%m%d"),
        "format": "JSON"
    }

    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        all_sky_data = data['properties']['parameter']['ALLSKY_SFC_SW_DWN']
        clear_sky_data = data['properties']['parameter']['CLRSKY_SFC_SW_DWN']

        # Calculate average all sky radiation
        all_sky_values = [float(value) for value in all_sky_data.values() if value != -999]
        avg_all_sky_radiation = sum(all_sky_values) / len(all_sky_values) if all_sky_values else 0

        # Calculate average clear sky radiation
        clear_sky_values = [float(value) for value in clear_sky_data.values() if value != -999]
        avg_clear_sky_radiation = sum(clear_sky_values) / len(clear_sky_values) if clear_sky_values else 0

        # Prepare monthly data
        monthly_all_sky = {str(i).zfill(2): [] for i in range(1, 13)}
        monthly_clear_sky = {str(i).zfill(2): [] for i in range(1, 13)}

        for date, value in all_sky_data.items():
            month = date[4:6]
            if float(value) != -999:
                monthly_all_sky[month].append(float(value))

        for date, value in clear_sky_data.items():
            month = date[4:6]
            if float(value) != -999:
                monthly_clear_sky[month].append(float(value))

        monthly_all_sky = {k: round(sum(v) / len(v), 2) if v else 0 for k, v in monthly_all_sky.items()}
        monthly_clear_sky = {k: round(sum(v) / len(v), 2) if v else 0 for k, v in monthly_clear_sky.items()}

        # Calculate data quality (percentage of valid data points)
        all_sky_data_quality = len(all_sky_values) / len(all_sky_data)
        clear_sky_data_quality = len(clear_sky_values) / len(clear_sky_data)

        # Find best and worst months
        valid_all_sky = {k: v for k, v in monthly_all_sky.items() if v != 0}
        valid_clear_sky = {k: v for k, v in monthly_clear_sky.items() if v != 0}
        
        best_all_sky = max(valid_all_sky.items(), key=lambda x: x[1]) if valid_all_sky else (None, None)
        worst_all_sky = min(valid_all_sky.items(), key=lambda x: x[1]) if valid_all_sky else (None, None)
        best_clear_sky = max(valid_clear_sky.items(), key=lambda x: x[1]) if valid_clear_sky else (None, None)
        worst_clear_sky = min(valid_clear_sky.items(), key=lambda x: x[1]) if valid_clear_sky else (None, None)

        return {
            'avg_all_sky_radiation': round(avg_all_sky_radiation, 2),
            'avg_clear_sky_radiation': round(avg_clear_sky_radiation, 2),
            'monthly_all_sky': monthly_all_sky,
            'monthly_clear_sky': monthly_clear_sky,
            'all_sky_data_quality': round(all_sky_data_quality * 100, 2),
            'clear_sky_data_quality': round(clear_sky_data_quality * 100, 2),
            'best_all_sky': {'month': best_all_sky[0], 'value': round(best_all_sky[1], 2) if best_all_sky[1] is not None else None},
            'worst_all_sky': {'month': worst_all_sky[0], 'value': round(worst_all_sky[1], 2) if worst_all_sky[1] is not None else None},
            'best_clear_sky': {'month': best_clear_sky[0], 'value': round(best_clear_sky[1], 2) if best_clear_sky[1] is not None else None},
            'worst_clear_sky': {'month': worst_clear_sky[0], 'value': round(worst_clear_sky[1], 2) if worst_clear_sky[1] is not None else None},
            'latitude': lat,
            'longitude': lon,
            'period': "daily average",
            'start_date': start_date.strftime("%Y-%m-%d"),
            'end_date': end_date.strftime("%Y-%m-%d")
        }

    except requests.RequestException as e:
        logger.error(f"Error fetching NASA POWER data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error fetching NASA POWER data: {str(e)}")
    except (KeyError, ValueError) as e:
        logger.error(f"Error processing NASA POWER data: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Error processing NASA POWER data: {str(e)}")

def geocode_address(address):
    try:
        location = geolocator.geocode(address, timeout=10)
        if location:
            return location.latitude, location.longitude
        else:
            raise HTTPException(status_code=404, detail="Address not found")
    except GeocoderTimedOut:
        raise HTTPException(status_code=408, detail="Geocoding service timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# API Endpoints
@app.post("/api/user-data", response_model=dict, summary="Store User Data", description="Stores user data including address and browser information.")
async def store_user_data(request: Request, user_data: UserData):
    try:
        ip_address = request.client.host
        guid = str(uuid.uuid4())
        store_personal_info(guid, user_data.address.dict())  # Convert to dict
        store_browser_data(guid, user_data.browserData.dict(), ip_address)  # Convert to dict
        return {"guid": guid}
    except Exception as e:
        logging.error(f"Error storing user data: {e}")
        raise HTTPException(status_code=500, detail="Internal Server Error")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_timezone(lat, lon):
    tf = TimezoneFinder()
    return tf.timezone_at(lat=lat, lng=lon)

@app.post("/api/solar-potential", response_model=dict, summary="Calculate Solar Potential", description="Calculates the solar potential based on user data and system specifications.")
def calculate_solar_potential(input_data: SolarPotentialRequest):
    guid = input_data.guid
    address = check_existing_address_data(guid)
    if not address:
        raise HTTPException(status_code=404, detail="GUID not found")

    logger.info(f"Address data for GUID {guid}: {address}")

    solar_data, time_zone = check_existing_solar_data(guid)
    if not solar_data:
        try:
            solar_data, time_zone = check_existing_zip_data(address['zip'])
        except Exception as e:
            logger.error(f"Error checking existing ZIP data: {str(e)}")
            solar_data, time_zone = None, None

    # If we still don't have solar_data, we need to fetch it from the NASA API
    if not solar_data:
        # Geocode the address to get lat and lon
        lat, lon = geocode_address(f"{address['street']}, {address['city']}, {address['state']} {address['zip']}, {address['country']}")
        
        # Fetch data from NASA API
        nasa_data = get_nasa_power_data(lat, lon)
        solar_data = nasa_data
        time_zone = get_timezone(lat, lon)
        store_solar_data(guid, solar_data, time_zone, address, "nasa")
    else:
        lat, lon = solar_data.get('latitude'), solar_data.get('longitude')

    # Ensure we have lat and lon
    if lat is None or lon is None:
        raise HTTPException(status_code=500, detail="Unable to determine latitude and longitude")

    # Calculate radiation data
    avg_all_sky_radiation = round(solar_data.get('avg_all_sky_radiation', 0), 2)
    avg_clear_sky_radiation = round(solar_data.get('avg_clear_sky_radiation', 0), 2)

    # Calculate data quality
    all_sky_data_quality = round(min(solar_data.get('all_sky_data_quality', 0) * 100, 100), 2)
    clear_sky_data_quality = round(min(solar_data.get('clear_sky_data_quality', 0) * 100, 100), 2)

    # Prepare monthly data
    monthly_all_sky = {str(i).zfill(2): round(solar_data.get('monthly_all_sky', {}).get(str(i).zfill(2), 0), 2) for i in range(1, 13)}
    monthly_clear_sky = {str(i).zfill(2): round(solar_data.get('monthly_clear_sky', {}).get(str(i).zfill(2), 0), 2) for i in range(1, 13)}

    # Calculate best and worst values
    best_all_sky = round(max(monthly_all_sky.values()), 2) if monthly_all_sky else 0
    worst_all_sky = round(min(monthly_all_sky.values()), 2) if monthly_all_sky else 0
    best_clear_sky = round(max(monthly_clear_sky.values()), 2) if monthly_clear_sky else 0
    worst_clear_sky = round(min(monthly_clear_sky.values()), 2) if monthly_clear_sky else 0

    # Calculate solar potential
    avg_daily_production = round(avg_all_sky_radiation * input_data.system_size * input_data.panel_efficiency * 0.75, 2)
    annual_production = round(avg_daily_production * 365, 2)
    annual_savings = round(annual_production * input_data.electricity_rate, 2)
    system_cost = round(input_data.system_size * 1000 * input_data.installation_cost_per_watt, 2)
    payback_period = round(system_cost / annual_savings, 2) if annual_savings > 0 else None
    total_savings = round(sum([annual_savings * (1.02 ** year) for year in range(25)]), 2)

    # Determine overall data quality
    overall_quality = "high" if all_sky_data_quality > 80 and clear_sky_data_quality > 80 else "medium" if all_sky_data_quality > 60 and clear_sky_data_quality > 60 else "low"

    response_data = {
        "address": f"{address['street']}, {address['city']}, {address['state']} {address['zip']}, {address['country']}",
        "latitude": round(lat, 6),
        "longitude": round(lon, 6),
        "avg_all_sky_radiation": avg_all_sky_radiation,
        "avg_clear_sky_radiation": avg_clear_sky_radiation,
        "all_sky_data_quality": all_sky_data_quality,
        "clear_sky_data_quality": clear_sky_data_quality,
        "monthly_all_sky": monthly_all_sky,
        "monthly_clear_sky": monthly_clear_sky,
        "best_all_sky": best_all_sky,
        "worst_all_sky": worst_all_sky,
        "best_clear_sky": best_clear_sky,
        "worst_clear_sky": worst_clear_sky,
        "unit": "kWh/m²/day",
        "period": "daily average",
        "daily_production": avg_daily_production,
        "annual_production": annual_production,
        "annual_savings": annual_savings,
        "system_cost": system_cost,
        "payback_period": payback_period,
        "total_savings_25_years": total_savings,
        "time_zone": time_zone,
        "data_source": "nasa" if not solar_data else "cache",
        "data_quality": overall_quality
    }

    return response_data

@app.get("/api/privacy-policy", response_model=dict, summary="Get Privacy Policy", description="Returns the privacy policy of the application.")
def get_privacy_policy():
    policy_text = """
    Your privacy is important to us. This privacy policy explains what personal data we collect from you and how we use it.
    """
    return {"policyText": policy_text}

# Custom OpenAPI schema generation
def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema
    openapi_schema = get_openapi(
        title="Solar Potential Calculator API",
        version="1.0.0",
        description="API for calculating solar potential and storing user data.",
        routes=app.routes,
    )
    app.openapi_schema = openapi_schema
    return app.openapi_schema

app.openapi = custom_openapi

# Serve Swagger UI
@app.get("/docs", include_in_schema=False)
async def get_swagger_ui():
    return get_swagger_ui_html(openapi_url="/openapi.json", title="Solar Potential Calculator API")


#uvicorn main:app --reload --port 8000 - to start the server
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)