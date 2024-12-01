import logging
from gmail_app import process_gmail_attachments
from drive_app import process_drive_files
from logging_utils import log_error_to_sheets

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

if __name__ == '__main__':
    logging.info("Starting Gmail and Drive processing...")
    try:
        process_gmail_attachments()
        logging.info("All Emails Processed.")
        process_drive_files()
        logging.info("All Files Processed.")
        
    except Exception as e:
        logging.error(f"Error in processing: {e}")
        log_error_to_sheets("app.py", str(e))