import os
import logging
import pytz
import traceback
from datetime import datetime
from google_auth import create_google_service
from dotenv import load_dotenv  

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def log_error_to_sheets(function_name, error_message):
    try:
        SCOPES = [
            'https://www.googleapis.com/auth/gmail.readonly',
            'https://www.googleapis.com/auth/gmail.modify',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        
        # Initialize services
        sheets_service = create_google_service('sheets', 'v4', SCOPES)
        

        # Get additional error info
        error_type = type(error_message).__name__  # Get the error type (e.g., TypeError, ValueError)
        stack_trace = traceback.format_exc()  # Get the full stack trace of the exception

        # Prepare data to log
        log_sheet_id = os.getenv('GMAIL_LOG_SPREADSHEET_ID')
        range_name = 'Code Errors!A:E'  # Adjust the range according to where you want the errors logged
        values = [[datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y %I:%M:%S %p'), function_name, error_message, error_type, stack_trace]]
        body = {'values': values}
        
        # Log error to the sheet
        sheets_service.spreadsheets().values().append(
            spreadsheetId=log_sheet_id,
            range=range_name,
            valueInputOption="RAW",
            body=body
        ).execute()

        logging.info("Error logged to Google Sheets.")

    except Exception as e:
        # Handle errors during logging
        logging.error(f"Error occurred while logging to Google Sheets: {e}")

        # Optionally, log to a file or send a local notification
        with open("local_error_log.txt", "a") as log_file:
            log_file.write(f"{datetime.now()} - Error while logging to sheets: {e}\n")