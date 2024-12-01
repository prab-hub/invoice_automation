import os
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_google_service(service_name, version, scopes):
    """
    Authenticate and create a Google API service client.
    """
    try:
        creds = None
        token_path = 'token.json'

        if os.path.exists(token_path):
            creds = Credentials.from_authorized_user_file(token_path, scopes)

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', scopes)
                creds = flow.run_local_server(port=0)

            # Save the credentials for the next run
            with open(token_path, 'w') as token:
                token.write(creds.to_json())

        return build(service_name, version, credentials=creds)
    
    except Exception as e:
        logging.error(f"Error creating Google service: {e}")
        raise