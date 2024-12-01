import os
import logging
import base64
import pytz
from datetime import datetime
from googleapiclient.http import MediaFileUpload
from google_auth import create_google_service
from logging_utils import log_error_to_sheets
from dotenv import load_dotenv  

# Load environment variables from .env
load_dotenv()

gmail_log_sheet_id = os.getenv('GMAIL_LOG_SPREADSHEET_ID')
gmail_attachments_folder_id = os.getenv('GMAIL_ATTACHMENTS_FOLDER_ID')

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_gmail_attachments():
    try:
        SCOPES = [
            'https://www.googleapis.com/auth/gmail.readonly',
            'https://www.googleapis.com/auth/gmail.modify',
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive',
            'https://www.googleapis.com/auth/spreadsheets'
        ]
        
        # Initialize services
        gmail_service = create_google_service('gmail', 'v1', SCOPES)
        drive_service = create_google_service('drive', 'v3', SCOPES)
        sheets_service = create_google_service('sheets', 'v4', SCOPES)

        # Ensure the 'processed' label exists
        label_name = "processed"
        labels = gmail_service.users().labels().list(userId='me').execute().get('labels', [])
        label_id = next((label['id'] for label in labels if label['name'] == label_name), None)

        if not label_id:
            # Create the label if it doesn't exist
            label_body = {"name": label_name, "labelListVisibility": "labelShow", "messageListVisibility": "show"}
            created_label = gmail_service.users().labels().create(userId='me', body=label_body).execute()
            label_id = created_label['id']
            logging.info(f"Created label '{label_name}' with ID: {label_id}")

        # Fetch messages with attachments that are not labeled as 'processed'
        messages = gmail_service.users().messages().list(
            userId='me', 
            q='has:attachment -label:processed',
        ).execute().get('messages', [])

        if not messages:
            logging.info("No messages with attachments found.")
            return

        for message in messages:
            msg = gmail_service.users().messages().get(userId='me', id=message['id']).execute()

            headers = msg['payload']['headers']
            subject = next((h['value'] for h in headers if h['name'] == 'Subject'), 'No Subject')
            sender = next((h['value'] for h in headers if h['name'] == 'From'), 'Unknown Sender')

            # Process attachments
            if 'parts' in msg['payload']:
                for part in msg['payload']['parts']:
                    if part.get('filename') and part.get('body', {}).get('attachmentId'):
                        attachment_id = part['body']['attachmentId']
                        attachment = gmail_service.users().messages().attachments().get(
                            userId='me', messageId=message['id'], id=attachment_id
                        ).execute()

                        try:

                            file_data = base64.urlsafe_b64decode(attachment['data'])
                            file_path = os.path.join("/tmp", part['filename'])  # Temporary file path

                            # Save attachment locally
                            with open(file_path, 'wb') as temp_file:
                                temp_file.write(file_data)

                            # Upload to Google Drive
                            file_metadata = {
                                'name': part['filename'],
                                'parents': [gmail_attachments_folder_id]
                            }
                            media = MediaFileUpload(file_path, mimetype='application/octet-stream')
                            drive_file = drive_service.files().create(
                                body=file_metadata,
                                media_body=media,
                                fields='id, webViewLink'
                            ).execute()

                            # Log details in Google Sheets
                            log_data = [
                                [
                                    datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%d-%m-%Y %I:%M:%S %p'),
                                    part['filename'],
                                    drive_file['id'],
                                    drive_file['webViewLink'],
                                    sender,
                                    subject,
                                ]
                            ]
                            sheets_service.spreadsheets().values().append(
                                spreadsheetId=gmail_log_sheet_id,
                                range='Gmail Logs!A:F',
                                valueInputOption='RAW',
                                body={'values': log_data}
                            ).execute()

                            # Clean up local file
                            os.remove(file_path)
                            logging.info(f"Processed attachment: {part['filename']}")

                        except Exception as e:
                            log_error_to_sheets("process_gmail_attachments (attachment handling)", str(e))
                            logging.error(f"Error processing attachment {part['filename']}: {e}")

            # Add 'processed' label
            gmail_service.users().messages().modify(
                userId='me',
                id=message['id'],
                body={"addLabelIds": [label_id]}
            ).execute()
            logging.info(f"Email {message['id']} labeled as 'processed'.")

    except Exception as e:
        log_error_to_sheets("process_gmail_attachments", str(e))
        logging.error(f"Gmail processing error: {e}")