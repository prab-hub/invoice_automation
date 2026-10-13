# Invoice Automation

Pulls invoice attachments out of Gmail, extracts the text with Azure Form Recognizer, turns it into structured accounting rows with OpenAI, and writes the results to Google Sheets. Every step is logged to Google Sheets so you can see what succeeded, what failed, and what it cost in tokens.

## How it works

`app.py` runs two jobs in order:

1. **Gmail intake** (`gmail_app.py`)
   - Finds emails with attachments that don't have the `processed` label (the label is created if missing).
   - Uploads each attachment to the Gmail attachments folder in Google Drive.
   - Logs timestamp, file name, Drive file ID, Drive link, sender and subject to the `Gmail Logs` tab.
   - Labels the email `processed` so it is skipped next run.

2. **Drive processing** (`drive_app.py`)
   - Lists every file in two input folders: the manual upload folder and the Gmail attachments folder.
   - For each file:
     1. Downloads it from Drive.
     2. Extracts text with Azure Form Recognizer (`prebuilt-document` model).
     3. Sends the text to OpenAI and asks for a Python list of rows with these columns:
        `Date, Voucher Type, Invoice Number, Ledger/Vendor Name, Ledger Amt, Dr/Cr, Item Name, Quantity, UOM, Rate, Value`
     4. Parses the response and, on success:
        - appends the rows to the main output sheet,
        - creates a new spreadsheet named after the invoice (header row + rows) in the output folder,
        - moves the source file to the processed folder,
        - logs file details, output sheet link, token usage, source (`email` or `upload`) and processing time to the `Invoices Successes` tab.
     5. If the model output can't be parsed, moves the file to the failed folder and logs the raw output to the `Invoices Failed` tab.

Unexpected errors from any function go to the `Code Errors` tab. If logging to Sheets itself fails, the error is written to `local_error_log.txt`.

## Project structure

| File | Purpose |
| --- | --- |
| `app.py` | Entry point. Runs Gmail intake, then Drive processing. |
| `gmail_app.py` | Saves Gmail attachments to Drive and labels emails as processed. |
| `drive_app.py` | OCR, OpenAI extraction, Sheets output, file moves and run logging. |
| `google_auth.py` | OAuth flow and Google API client creation (`token.json` cache). |
| `logging_utils.py` | Writes errors to the `Code Errors` tab. |
| `requirements.txt` | Pinned Python dependencies. |

## Prerequisites

- Python 3.12
- A Google Cloud project with the **Gmail API**, **Google Drive API** and **Google Sheets API** enabled, and an OAuth client of type *Desktop app*. Download its client secret as `credentials.json` into the project root.
- An Azure AI Document Intelligence (Form Recognizer) resource: endpoint and key.
- An OpenAI API key.

## Google Drive and Sheets setup

Create these Drive folders and note their IDs (the last part of the folder URL):

- Input folder for manual uploads
- Gmail attachments folder
- Output folder for per-invoice spreadsheets
- Processed folder
- Failed folder

Create these spreadsheets with the exact tab names below:

| Spreadsheet | Tabs |
| --- | --- |
| Main output | `Sheet1` (columns A:K) |
| Gmail log | `Gmail Logs` (A:F), `Code Errors` (A:E) |
| Drive log | `Invoices Successes` (A:J), `Invoices Failed` (A:J) |

## Installation

```bash
git clone https://github.com/prab-hub/invoice_automation.git
cd invoice_automation
python3.12 -m venv venv312
source venv312/bin/activate
pip install -r requirements.txt
```

## Configuration

Create a `.env` file in the project root:

```env
# Azure Form Recognizer
AZURE_ENDPOINT=https://<your-resource>.cognitiveservices.azure.com/
AZURE_KEY=<azure-key>

# OpenAI
OPENAI_API=<openai-api-key>
OPENAI_MODEL=<model-name>

# Google Drive folders
INPUT_DRIVE_FOLDER_ID=<manual-upload-folder-id>
GMAIL_ATTACHMENTS_FOLDER_ID=<gmail-attachments-folder-id>
OUTPUT_DRIVE_FOLDER_ID=<output-folder-id>
PROCESSED_FOLDER_ID=<processed-folder-id>
FAILED_FOLDER_ID=<failed-folder-id>

# Google Sheets
MAIN_OUTPUT_SHEET_ID=<main-output-spreadsheet-id>
GMAIL_LOG_SPREADSHEET_ID=<gmail-log-spreadsheet-id>
DRIVE_LOG_SPREADSHEET_ID=<drive-log-spreadsheet-id>
```

`.env`, `credentials.json` and `token.json` are in `.gitignore`. Don't commit them.

## Running

```bash
python app.py
```

On the first run a browser window opens for Google sign-in. The token is saved to `token.json` and refreshed automatically after that.

To run it on a schedule, use cron (or any scheduler). Example, every 15 minutes:

```cron
*/15 * * * * cd /path/to/invoice_automation && venv312/bin/python app.py >> run.log 2>&1
```

## Known limitations

- **Source detection uses hardcoded folder IDs.** `add_to_sheets` in `drive_app.py` decides between `email` and `upload` by comparing against two fixed folder IDs. If you use different folders, every file is logged as `upload`. Compare against `GMAIL_ATTACHMENTS_FOLDER_ID` instead.
- **Every attachment is treated as an invoice.** Gmail intake picks up any email with an attachment, including signatures and non-invoice files. Narrow the Gmail query in `gmail_app.py` (for example `has:attachment filename:pdf -label:processed`) if needed.
- **Fixed 15 second wait on OCR.** `extract_text_from_pdf` sleeps 15 seconds before reading the Azure result, so each file takes at least that long.
- **Only the first page of Gmail results is read.** The Gmail and Drive list calls don't paginate, so very large backlogs are processed over several runs.
- **Timestamps are in IST** (`Asia/Kolkata`).
- `Flask` and `gunicorn` are in `requirements.txt` but not used by the current code.
