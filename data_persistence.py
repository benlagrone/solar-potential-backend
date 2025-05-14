import os
from datetime import datetime, timedelta
from google_sheets_service import GoogleSheetsService
import logging
import json

logger = logging.getLogger(__name__)
sheets_service = GoogleSheetsService()

# Data Storage Functions

def store_personal_info(guid, address):
    sheet_id = os.getenv('PERSONAL_INFO_SHEET_ID')
    values = [[guid, address['street'], address['city'], address['state'], address['zip'], address['country']]]
    print(values)
    sheets_service.append_to_sheet(sheet_id, 'A:F', values)

import csv
import io

def store_browser_data(guid, browser_data, ip_address):
    sheet_id = os.getenv('BROWSER_DATA_SHEET_ID')
    
    # Prepare the data
    row_data = [
        guid, 
        browser_data['userAgent'], 
        browser_data['screenResolution'],
        browser_data['languagePreference'], 
        browser_data['timeZone'],
        browser_data['referrerUrl'], 
        browser_data['deviceType'], 
        ip_address
    ]
    
    # Ensure all values are strings
    row_data = [str(value) for value in row_data]
    
    # Create a list of lists for the values
    values = [row_data]
    
    print(values)
    sheets_service.append_to_sheet(sheet_id, 'A:H', values)

def store_solar_data(guid, solar_data, time_zone, address, data_source):
    sheet_id = os.getenv('SOLAR_DATA_SHEET_ID')
    solar_data_json = json.dumps(solar_data)
    values = [[guid, solar_data_json, time_zone, data_source, datetime.now().strftime('%Y-%m-%d')]]
    # print(values)
    sheets_service.append_to_sheet(sheet_id, 'A:E', values)

# Data Retrieval Functions
def check_existing_address_data(guid):
    sheet_id = os.getenv('PERSONAL_INFO_SHEET_ID')
    values = sheets_service.read_from_sheet(sheet_id, 'A:F')
    for row in values:
        if row[0] == guid:
            address_data = {
                'street': row[1],
                'city': row[2],
                'state': row[3],
                'zip': row[4],
                'country': row[5]
            }
            logger.info(f"Address data for GUID {guid}: {address_data}")
            return address_data
    logger.info(f"No address data found for GUID {guid}")
    return None

def check_existing_solar_data(guid):
    sheet_id = os.getenv('SOLAR_DATA_SHEET_ID')
    values = sheets_service.read_from_sheet(sheet_id, 'A:E')
    thirty_days_ago = datetime.now() - timedelta(days=30)
    for row in values:
        if row[0] == guid and datetime.strptime(row[4], '%Y-%m-%d') > thirty_days_ago:
            try:
                solar_data = json.loads(row[1])
                time_zone = row[2]
                logger.info(f"Solar data for GUID {guid}: {solar_data}, Time zone: {time_zone}")
                return solar_data, time_zone
            except json.JSONDecodeError:
                logger.error(f"Failed to parse solar data for GUID {guid}: {row[1]}")
    logger.info(f"No recent solar data found for GUID {guid}")
    return None, None

def check_existing_zip_data(zip_code):
    sheet_id = os.getenv('PERSONAL_INFO_SHEET_ID')
    values = sheets_service.read_from_sheet(sheet_id, 'A:F')
    matching_guids = [row[0] for row in values if len(row) > 4 and row[4] == zip_code]
    
    if matching_guids:
        solar_sheet_id = os.getenv('SOLAR_DATA_SHEET_ID')
        solar_values = sheets_service.read_from_sheet(solar_sheet_id, 'A:E')
        thirty_days_ago = datetime.now() - timedelta(days=30)
        
        for row in solar_values:
            if len(row) > 4 and row[0] in matching_guids and datetime.strptime(row[4], '%Y-%m-%d') > thirty_days_ago:
                try:
                    solar_data = json.loads(row[1])
                    time_zone = row[2]
                    logger.info(f"Solar data for ZIP {zip_code}: {solar_data}, Time zone: {time_zone}")
                    return solar_data, time_zone
                except json.JSONDecodeError:
                    logger.error(f"Failed to parse solar data for ZIP {zip_code}: {row[1]}")
    
    logger.info(f"No recent solar data found for ZIP {zip_code}")
    return None, None