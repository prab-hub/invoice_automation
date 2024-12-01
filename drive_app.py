import os
import logging
import io
import json
import ast
from datetime import datetime
import time
import pytz
from subprocess import Popen, PIPE
from dotenv import load_dotenv

from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from google_auth import create_google_service
from logging_utils import log_error_to_sheets

from azure.ai.formrecognizer import DocumentAnalysisClient
from azure.core.credentials import AzureKeyCredential
from openai import OpenAI

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger('azure.core').setLevel(logging.WARNING)
logging.getLogger('azure.ai.formrecognizer').setLevel(logging.WARNING)

# Load environment variables
load_dotenv()

# Azure Form Recognizer details
AZURE_ENDPOINT = os.getenv("AZURE_ENDPOINT")
AZURE_KEY = os.getenv("AZURE_KEY")

# OpenAI details
OPENAI_API_KEY = os.getenv("OPENAI_API")
OPENAI_MODEL = os.getenv("OPENAI_MODEL")

# Initialize Azure Form Recognizer client & OpenAI clients
form_recognizer_client = DocumentAnalysisClient(AZURE_ENDPOINT, AzureKeyCredential(AZURE_KEY))
openai_client = OpenAI(api_key=OPENAI_API_KEY)

main_output_sheet_id=os.getenv('MAIN_OUTPUT_SHEET_ID')
output_folder_id = os.getenv('OUTPUT_DRIVE_FOLDER_ID')

processed_folder_id = os.getenv('PROCESSED_FOLDER_ID')
failed_folder_id = os.getenv('FAILED_FOLDER_ID') 

drive_log_sheet_id = os.getenv('DRIVE_LOG_SPREADSHEET_ID')

# Functions 
   
def process_file(drive_service, sheets_service, file_id, input_folder_id):
    try:
        start_time = time.time()
        file_content = download_file_from_drive(drive_service, file_id)
        extracted_text = extract_text_from_pdf(file_content)
        optimized_content = optimize_content_with_chatgpt(extracted_text)
        add_to_sheet = add_to_sheets(sheets_service, drive_service, optimized_content, file_id, input_folder_id, start_time)
        logging.info(f"File {file_id} processed successfully.")
        return add_to_sheet
        
    except Exception as e:
        logging.error(f"Error processing file: {e}")
        log_error_to_sheets("process_file in drive_app.py", str(e))
        return str(e)
    
def download_file_from_drive(drive_service, file_id):
    try:
        request = drive_service.files().get_media(fileId=file_id)
        file_content = io.BytesIO()
        downloader = MediaIoBaseDownload(file_content, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        file_content.seek(0)  # Reset pointer to the start
        logging.info(f"File {file_id} downloaded successfully.")
        return file_content
    
    except Exception as e:
        logging.error(f"Error downloading file: {e}")
        log_error_to_sheets("download_file_from_drive", str(e))
        raise Exception(f"Error downloading file: {e}")

def extract_text_from_pdf(file_content):
    try:
        # Start the document analysis
        poller = form_recognizer_client.begin_analyze_document("prebuilt-document", file_content)

        max_retries = 5  # You can adjust the retry count as needed
        retry_count = 0
        while retry_count < max_retries:
            time.sleep(15)  # Sleep for 15 seconds before checking the status
            try:
                # Retrieve the result once the operation is complete
                result = poller.result()
                logging.info("Analysis started, result obtained successfully.")
                break  # Break the loop if the result is successfully obtained
            except Exception as e:
                retry_count += 1
                if retry_count < max_retries:
                    logging.warning(f"Attempt {retry_count}/{max_retries} failed, retrying...")
                else:
                    logging.error(f"Failed to retrieve result after {max_retries} attempts. Error: {e}")
                    raise Exception(f"Failed to get result after {max_retries} attempts: {e}")
                if retry_count >= max_retries:
                    raise Exception(f"Failed to get result after {max_retries} attempts: {e}")

        # Retrieve the result once the operation is complete
        result = poller.result()
        
        # Extract and format the content
        extracted_content = "\n".join(
            f"Page {page.page_number}: " + " ".join(line.content for line in page.lines)
            for page in result.pages
        )

        logging.info("Text extraction from PDF completed successfully.")
        return extracted_content
        log_error_to_sheets("extract_text_from_pdf", str(e))
        raise Exception(f"Error extracting text from PDF: {e}")
    
    except Exception as e:
        raise Exception(f"Error extracting text from PDF: {e}")

def optimize_content_with_chatgpt(extracted_content):
    try:
        user_message = """
        From above markdown text return the following data in table format

        Date    
        Voucher Type     
        Invoice Number    
        Ledger/Vendor Name
        Ledger Amt    
        Dr/Cr    
        Item Name     
        Quantity    
        UOM     
        Rate       
        Value 

        Please note that ledger name should always be vendor name, NOT customer name. if no vendor name mark it as "-".

        Line item name/description should be captured completely, please don't save it partially.

        If anything doesn't exist mark it as "-".

        
        ["01/01/2022", "Sales", "INV001", "ABC Corp", "1000", "Dr", "Product A", "10", "pcs", "100", "1000"],
        ["02/01/2022", "Purchase", "INV002", "XYZ Ltd", "500", "Cr", "Product B", "5", "pcs", "100", "500"],
        ["03/01/2022", "Sales", "INV003", "LMN Inc", "1500", "Dr", "Product C", "15", "pcs", "100", "1500"],
        ["04/01/2022", "Purchase", "INV004", "OPQ Pvt", "1200", "Cr", "Product D", "12", "pcs", "100", "1200"],
        ["05/01/2022", "Sales", "INV005", "RST LLC", "2000", "Dr", "Product E", "20", "pcs", "100", "2000"]
    


        This is just an example. So please be flexible as per input data. But please follow the format at any cost.

        PLEASE only output python list, no other text required at any cost. Dont add ```json\n or similar markup. Just give as I asked.

        If no data can be fetched, just output "no data".
        """
        response = openai_client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": extracted_content},
                {"role": "user", "content": user_message}
            ]
        )

        optimized_content = response.choices[0].message.content
        input_tokens = response.usage.prompt_tokens
        output_tokens = response.usage.completion_tokens
        total_tokens = input_tokens + output_tokens

        chatgpt_output = {
            "optimized_content": optimized_content,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens
        }

        logging.info(f"Content optimized successfully:{chatgpt_output}")
        return chatgpt_output
    
    except Exception as e:
        logging.error(f"Error optimizing content with ChatGPT: {e}")
        log_error_to_sheets("optimize_content_with_chatgpt", str(e))
        raise Exception(f"Error optimizing content with ChatGPT: {e}")

def add_to_sheets(sheets_service, drive_service, chatgpt_output, file_id, input_folder_id, start_time):

    try:       
        # Get file metadata (file name and URL)
        file_metadata = drive_service.files().get(
            fileId=file_id,
            fields='name, webViewLink'
        ).execute()

        file_name = file_metadata.get('name')
        file_url = file_metadata.get('webViewLink')
        logging.info(f"File Name & URL: {file_name}, {file_url}")

        input_tokens = chatgpt_output["input_tokens"]
        output_tokens = chatgpt_output["output_tokens"]
        total_tokens = chatgpt_output["total_tokens"]

        source = (
        "email" if input_folder_id == "1Dao71ak2stmpAdkrcBXO_B_HV92_4gtU" 
        else "upload" if input_folder_id == "16zt19gMlf0PxXdJmuuZyyNAkXTTZutaB" 
        else "upload"
        )

        logging.info(f"Improper List:{chatgpt_output}")

        # Remove newline characters
        data_string = chatgpt_output["optimized_content"].replace('\n', '')

        # Use ast.literal_eval to safely parse the string
        data_list = ast.literal_eval(data_string)
        
        # Ensure the data is in a 2D list format
        if not isinstance(data_list[0], list):
            # If it's a 1D list, wrap it in another list
            data_new = [data_list]
        elif isinstance(data_list, tuple):
            # If it's a tuple, convert to list
            data_new = list(data_list)
        else:
            # If it's already a list of lists, use as-is
            data_new = data_list
        
        # Prepare the payload for Google Sheets
        payload = {"values": data_new}
        logging.info(f"Proper List: {payload}")

        response = sheets_service.spreadsheets().values().append(
            spreadsheetId=main_output_sheet_id,
            range='Sheet1!A:K',
            valueInputOption='RAW',
            body=payload  
            ).execute()

        # Check if the response indicates success (status code 200)
        if response and response.get('updates', {}).get('updatedCells', 0) > 0:
            logging.info("Successfully appended data to Main sheet.")

        # Logic after main sheet appending is successful:
        new_spreadsheet = sheets_service.spreadsheets().create(
            body={
                'properties': {'title': file_name},
                'sheets': [{'properties': {'title': 'Sheet1'}}]
            }
        ).execute()

        logging.info("Created new spreadsheet")

        new_spreadsheet_id = new_spreadsheet['spreadsheetId']
        new_spreadsheet_url = f"https://docs.google.com/spreadsheets/d/{new_spreadsheet_id}/edit"

        # Append to new sheet

        sheets_service.spreadsheets().values().append(
            spreadsheetId=new_spreadsheet_id,
            range='Sheet1!A:K',
            valueInputOption='RAW',
        body={'values': [["Date", "Voucher Type", "Invoice Number", "Ledger Name", "Ledger Amt", "Dr/Cr", "Item Name", "Quantity", "UOM", "Rate", "Value"]]} 
        ).execute()  

        sheets_service.spreadsheets().values().append(
            spreadsheetId=new_spreadsheet_id,
            range='Sheet1!A:K',
            valueInputOption='RAW',
        body=payload
        ).execute()

        logging.info("Updated new spreadsheet")
                                                      
        # Move output file
        drive_service.files().update(
            fileId=new_spreadsheet_id,
            addParents=output_folder_id,
            removeParents='root',
            fields='id, parents'
        ).execute()                       

        # Move input file
        drive_service.files().update(
            fileId=file_id,
            addParents=processed_folder_id,
            removeParents=input_folder_id,
            fields='id, parents'
        ).execute()

        logging.info("Moved both files")

        # Log details to the Drive log sheet
        end_time = time.time() 
        elapsed_time = end_time - start_time
        log_data = [
            [datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y %I:%M:%S %p'), file_name, file_id, file_url, new_spreadsheet_url, input_tokens, output_tokens, total_tokens, source, elapsed_time]
        ]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=drive_log_sheet_id,
            range='Invoices Successes!A:J',
            valueInputOption='RAW',
            body={'values': log_data}
        ).execute()

        logging.info("Logged Success in Google Sheets")
        return "Invoice Processing Successful"

    
    except (SyntaxError, ValueError) as e:
        logging.error(f"Error appending the content: {e}")

        # Logic after main sheet appending failed:
        drive_service.files().update(
            fileId=file_id,
            addParents=failed_folder_id,
            removeParents=input_folder_id,
            fields='id, parents'
        ).execute()

        # Log failure to the Drive log sheet
        end_time = time.time() 
        elapsed_time = end_time - start_time
        log_data = [
            [datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y %I:%M:%S %p'), file_name, file_id, file_url, chatgpt_output["optimized_content"], input_tokens, output_tokens, total_tokens, source, elapsed_time]
        ]
        sheets_service.spreadsheets().values().append(
            spreadsheetId=drive_log_sheet_id,
            range='Invoices Failed!A:J',
            valueInputOption='RAW',
            body={'values': log_data}
        ).execute()

        logging.info("Error-handling executed")
        return "Invoice Processing Failed"
    
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        log_error_to_sheets("add_to_sheets in drive_app.py", str(e))
        return "Invoice Processing Failed"


def process_drive_files():
    try:
        SCOPES = [
            'https://www.googleapis.com/auth/gmail.readonly',
            'https://www.googleapis.com/auth/gmail.modify',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]

        drive_service = create_google_service('drive', 'v3', SCOPES)
        sheets_service = create_google_service('sheets', 'v4', SCOPES)

        input_folder_id = os.getenv('INPUT_DRIVE_FOLDER_ID')
        gmail_input_folder_id = os.getenv('GMAIL_ATTACHMENTS_FOLDER_ID') 


        # List files in both input folders
        input_folders = [input_folder_id, gmail_input_folder_id]
        for folder_id in input_folders:
            try:
                results = drive_service.files().list(
                    q=f"'{folder_id}' in parents",
                    spaces='drive',
                    fields='files(id, name, mimeType)'
                ).execute()

                if 'files' not in results:
                    logging.error(f"No files found in folder: {folder_id}")
                    continue

                logging.info(f"Found {len(results['files'])} files in folder {folder_id}.")

                for file in results['files']:
                    logging.info(f"Processing File ID: {file['id']}, Name: {file['name']}")

                    # Process the file by passing the folder_id to process_file
                    result = process_file(drive_service, sheets_service, file['id'], folder_id)
                    print(result)
            
            except Exception as e:
                logging.error(f"Error processing files in folder {folder_id}: {e}")
                log_error_to_sheets('process_drive_files', f"Error processing files in folder {folder_id}: {e}")


    except Exception as e:
        logging.error(f"Drive processing error: {e}")
        log_error_to_sheets('process_drive_files in drive_app.py', f"Drive processing error: {e}")



