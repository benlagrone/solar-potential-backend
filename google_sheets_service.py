from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import os
import uuid

load_dotenv()

class GoogleSheetsService:
    def __init__(self):
        # print(f"****Current Working Directory: {os.getcwd()}")
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        # print(f"***GOOGLE_APPLICATION_CREDENTIALS: {credentials_path}") 
        # print(os.getenv("PERSONAL_INFO_SHEET_ID"))
        if not credentials_path:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not set")
        
        # Open and read the credentials file to ensure it can be accessed
        try:
            with open(credentials_path, 'r') as file:
                content = file.read()
                # print("Credentials file content successfully read.")
                # print(content)  # Optionally print the content for debugging
        except FileNotFoundError:
            raise FileNotFoundError(f"Credentials file not found at path: {credentials_path}")
        except Exception as e:
            raise Exception(f"An error occurred while reading the credentials file: {e}")
        
        credentials = Credentials.from_service_account_file(credentials_path)
        self.service = build('sheets', 'v4', credentials=credentials)
        self.personal_info_sheet_id = os.getenv('PERSONAL_INFO_SHEET_ID')
        self.browser_data_sheet_id = os.getenv('BROWSER_DATA_SHEET_ID')
        self.solar_data_sheet_id = os.getenv('SOLAR_DATA_SHEET_ID')



    def append_to_sheet(self, sheet_id, range_name, values):
        body = {
            'values': values
        }
        result = self.service.spreadsheets().values().append(
            spreadsheetId=sheet_id,
            range=range_name,
            valueInputOption='USER_ENTERED',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        return result

    def read_from_sheet(self, sheet_id, range_name):
        result = self.service.spreadsheets().values().get(
            spreadsheetId=sheet_id,
            range=range_name
        ).execute()
        return result.get('values', [])

    # def store_user_data(self, address, browser_data, ip_address):
    #     guid = str(uuid.uuid4())
    #     self.append_to_sheet(self.personal_info_sheet_id, 'A1', [[guid, *address.values()]])
    #     self.append_to_sheet(self.browser_data_sheet_id, 'A1', [[guid, *browser_data.values(), ip_address]])
    #     return guid

    # def store_solar_data(self, guid, solar_data, time_zone):
    #     self.append_to_sheet(self.solar_data_sheet_id, 'A1', [[guid, *solar_data.values(), time_zone]])