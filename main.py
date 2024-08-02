# Solar-potential-backend
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv
import os
import requests
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from datetime import datetime, timedelta
import ssl
import certifi
import warnings
import logging
from datetime import datetime, timedelta
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from urllib3.exceptions import InsecureRequestWarning

# Suppress only the single InsecureRequestWarning
warnings.simplefilter('ignore', InsecureRequestWarning)

load_dotenv()

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],  # React app's address
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Create a custom SSL context
ctx = ssl.create_default_context(cafile=certifi.where())
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

geolocator = Nominatim(user_agent="solar_potential_app", ssl_context=ctx)

class AddressInput(BaseModel):
    address: str

@app.get("/")
def read_root():
    return {"message": "Solar Potential API is running"}

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


def get_nasa_power_data(lat, lon):
    end_date = datetime.now() - timedelta(days=365)  # One year ago from today
    start_date = end_date - timedelta(days=365 * 3)  # Three years before the end date
    
    url = "https://power.larc.nasa.gov/api/temporal/daily/point"
    params = {
        "start": start_date.strftime("%Y%m%d"),
        "end": end_date.strftime("%Y%m%d"),
        "latitude": lat,
        "longitude": lon,
        "community": "re",
        "parameters": "ALLSKY_SFC_SW_DWN,CLRSKY_SFC_SW_DWN",
        "format": "json",
        "time-standard": "lst"
    }
    
    try:
        response = requests.get(url, params=params, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        raise HTTPException(status_code=500, detail=f"Error fetching NASA POWER data: {str(e)}")

def calculate_monthly_averages(data):
    monthly_data = {month: [] for month in range(1, 13)}
    for date, value in data.items():
        if value != -999.0:
            month = int(date[4:6])
            monthly_data[month].append(value)
    
    monthly_averages = {month: sum(values) / len(values) if values else None 
                        for month, values in monthly_data.items()}
    return monthly_averages

def calculate_best_worst_scenarios(data):
    valid_data = [v for v in data.values() if v != -999.0]
    if not valid_data:
        return None, None
    return max(valid_data), min(valid_data)

class SolarCalculationInput(BaseModel):
    address: str
    system_size: float = 7.0  # in kW, default 7kW
    panel_efficiency: float = 0.20  # default 20%
    electricity_rate: float  # in $/kWh
    installation_cost_per_watt: float = 3.0  # in $/W, default $3/W

@app.post("/solar-calculation")
def calculate_solar_potential(input_data: SolarCalculationInput):
    # First, get the solar potential data
    solar_data = get_solar_potential(AddressInput(address=input_data.address))
    
    # Calculate daily energy production
    avg_daily_production = solar_data['avg_all_sky_radiation'] * input_data.system_size * input_data.panel_efficiency * 0.75
    
    # Calculate annual energy production
    annual_production = avg_daily_production * 365
    
    # Calculate annual savings
    annual_savings = annual_production * input_data.electricity_rate
    
    # Calculate system cost
    system_cost = input_data.system_size * 1000 * input_data.installation_cost_per_watt
    
    # Calculate simple payback period
    payback_period = system_cost / annual_savings
    
    # Calculate 25-year savings (assuming 2% annual increase in electricity rates)
    total_savings = sum([annual_savings * (1.02 ** year) for year in range(25)])
    
    return {
        "daily_production": avg_daily_production,
        "annual_production": annual_production,
        "annual_savings": annual_savings,
        "system_cost": system_cost,
        "payback_period": payback_period,
        "total_savings_25_years": total_savings,
        "solar_data": solar_data
    }

@app.post("/solar-potential")
def get_solar_potential(address_input: AddressInput):
    address = address_input.address
    try:
        lat, lon = geocode_address(address)
        logger.info(f"Geocoded address to: {lat}, {lon}")
        
        nasa_data = get_nasa_power_data(lat, lon)
        logger.info("Successfully retrieved NASA POWER data")
        
        all_sky_radiation = nasa_data['properties']['parameter']['ALLSKY_SFC_SW_DWN']
        clear_sky_radiation = nasa_data['properties']['parameter']['CLRSKY_SFC_SW_DWN']
        
        # Calculate averages and data quality
        valid_all_sky = [v for v in all_sky_radiation.values() if v != -999.0]
        valid_clear_sky = [v for v in clear_sky_radiation.values() if v != -999.0]
        
        avg_all_sky = sum(valid_all_sky) / len(valid_all_sky) if valid_all_sky else None
        avg_clear_sky = sum(valid_clear_sky) / len(valid_clear_sky) if valid_clear_sky else None
        all_sky_quality = len(valid_all_sky) / len(all_sky_radiation) * 100
        clear_sky_quality = len(valid_clear_sky) / len(clear_sky_radiation) * 100
        
        # Calculate monthly averages
        monthly_all_sky = calculate_monthly_averages(all_sky_radiation)
        monthly_clear_sky = calculate_monthly_averages(clear_sky_radiation)
        
        # Calculate best and worst case scenarios
        best_all_sky, worst_all_sky = calculate_best_worst_scenarios(all_sky_radiation)
        best_clear_sky, worst_clear_sky = calculate_best_worst_scenarios(clear_sky_radiation)
        
        # Calculate date range for the data
        end_date = datetime.now() - timedelta(days=365)
        start_date = end_date - timedelta(days=365 * 3)
        
        return {
            "address": address,
            "latitude": lat,
            "longitude": lon,
            "avg_all_sky_radiation": avg_all_sky,
            "avg_clear_sky_radiation": avg_clear_sky,
            "all_sky_data_quality": all_sky_quality,
            "clear_sky_data_quality": clear_sky_quality,
            "monthly_all_sky": monthly_all_sky,
            "monthly_clear_sky": monthly_clear_sky,
            "best_all_sky": best_all_sky,
            "worst_all_sky": worst_all_sky,
            "best_clear_sky": best_clear_sky,
            "worst_clear_sky": worst_clear_sky,
            "unit": "kWh/m^2/day",
            "period": f"3-year average ({start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')})"
        }
    except HTTPException as e:
        logger.error(f"HTTP exception: {str(e)}")
        raise e
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)