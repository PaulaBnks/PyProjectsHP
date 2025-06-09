
from simple_salesforce import Salesforce, SalesforceLogin
from google.cloud import bigquery
import json
import google.generativeai as genai
import requests
import os
from itertools import islice
import base64
from docx import Document
from io import BytesIO
import re
from google.oauth2 import service_account



run_on_git_actions = 'No'

if run_on_git_actions == 'Yes':
    # authenticate to Salesforce
   sf = Salesforce(username = os.environ["username"],
                password = os.environ["pwd"],
                security_token = os.environ["token"],
                client_id='python')
   session_id, instance = SalesforceLogin(username=os.environ["username"], password=os.environ["pwd"], security_token=os.environ["token"])
   print('salesforce info', sf)
else:
    # Authenticate to Salesforce
    print("🔐 Logging into Salesforce...")
    loginInfo = json.load(open(r"E:\Software\loginpd.json"))
    username = loginInfo['username']
    password = loginInfo['password']
    security_token = loginInfo['security_token']
    session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token)
    sf = Salesforce(instance=instance, session_id=session_id)
    print("✅ Salesforce login successful.")



# Authenticate to BigQuery
credentials = service_account.Credentials.from_service_account_file(
    r"E:\Software\python-project-403413-480829366e0a.json"
)

# Initialize BigQuery client
bq_client = bigquery.Client(project='southern-coda-233109', credentials=credentials)

# Authenticate to Gemini
print("🔐 Loading Gemini API key...")
with open(r"E:\Software\gemini_prod_key.txt", "r") as file:
    api_key = file.read().strip()
genai.configure(api_key = api_key)
model = genai.GenerativeModel("gemini-1.5-flash")
print("✅ Gemini model initialized.")

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def summarize_records(records, label):
    lines = [f"{label}:\n"]
    for r in records:
        desc = r.get('Description') or ''
        subj = r.get('Subject') or ''
        if desc:
            lines.append(f"• {subj}: {desc}")
    return "\n".join(lines)

def enforce_summary_structure(summary):
    sections = {
        "1. Key Themes": [],
        "2. Customer Concerns": [],
        "3. Prior Interactions": [],
        "4. Open Issues / Opportunities": []
    }
    current_section = None
    lines = summary.splitlines()
    for line in lines:
        line = line.strip()
        if line.startswith("### 1. Key Themes"):
            current_section = "1. Key Themes"
        elif line.startswith("### 2. Customer Concerns"):
            current_section = "2. Customer Concerns"
        elif line.startswith("### 3. Prior Interactions"):
            current_section = "3. Prior Interactions"
        elif line.startswith("### 4. Open Issues / Opportunities"):
            current_section = "4. Open Issues / Opportunities"
        elif line.startswith("*") and current_section:
            sections[current_section].append(line)
    result = ""
    for title, points in sections.items():
        result += f"### {title}\n"
        result += "\n".join(points) + "\n" if points else "* (No items found)\n"
    return result




# Define valid Base64 character set once
BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

def safe_base64_decode(data, account_id=None):
    """
    Attempts to decode base64. Returns decoded string, original string if it looks like plain text,
    or [Unreadable note content...] if decoding fails.

    Args:
        data (str or bytes): Input string or bytes that may be base64 encoded
        account_id (str): Optional, used in logging

    Returns:
        str: Decoded content, original string, or fallback message
    """
    # Early return for blank input
    if not data or (isinstance(data, str) and data.strip() == ''):
        return ''

    # Convert bytes to string if needed
    if isinstance(data, bytes):
        try:
            data = data.decode('utf-8')
        except UnicodeDecodeError:
            try:
                data = data.decode('latin1')  # Common alternate encoding
            except Exception as e:
                print("      ⚠️ Unable to decode byte stream.")
                logging.warning(f"Failed to decode byte stream for {account_id}: {e}")
                return "[Unreadable note content: Invalid byte stream]"

    # Strip whitespace
    data = data.strip()

    # Detect if the first few characters look like plain text
    if len(data) >= 2 and all(c in BASE64_CHARS for c in data[:50]):
        # If all first 50 chars are valid base64 characters, proceed
        pass
    else:
        print("      ℹ️ Detected plain text instead of base64.")
        return data

    # Remove non-base64 characters
    cleaned = re.sub(r'[^A-Za-z0-9+/=]', '', data)

    # Fix padding
    missing_padding = len(cleaned) % 4
    if missing_padding:
        cleaned += '=' * (4 - missing_padding)

    try:
        decoded_bytes = base64.b64decode(cleaned, validate=False, altchars=None)
        decoded_text = decoded_bytes.decode('utf-8', errors='replace')

        # If result contains many replacement chars, assume unreadable
        if decoded_text.count('\uFFFD') > len(decoded_text) // 4:
            raise ValueError("Too many invalid characters after decode")

        return decoded_text

    except Exception as e:
        print(f"      ⚠️ Base64 decode failed even after cleaning: {e}")
        print(f"         Sample: {cleaned[:50]}... Length: {len(cleaned)}")
        logging.warning(f"Failed to decode base64 note for {account_id}:\n{data[:300]}...\nError: {e}")
        return f"[Unreadable note content: {data[:100]}...]"


def extract_docx_text_from_bytes(file_bytes):
    try:
        doc = Document(BytesIO(file_bytes))
        return "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
    except Exception as e:
        print(f"      ⚠️ Failed to parse DOCX content: {e}")
        return None

print("📡 Querying Salesforce for target accounts...")
query = """
    SELECT Id, Name, AI_Summary__c 
    FROM Account 
    WHERE Account_Status__c = 'Prospect' and CompanyType__c != 'Subcontractor' AND Temp__c = False
"""
accounts = []
response = sf.query(query)
accounts.extend(response['records'])
while not response['done']:
    response = sf.query_more(response['nextRecordsUrl'], True)
    accounts.extend(response['records'])
accounts = [acc for acc in accounts if not acc.get('AI_Summary__c')]
print(f"🔍 Found {len(accounts)} accounts to process.")

BATCH_SIZE = 200
batch_num = 1

for batch in chunked(accounts, BATCH_SIZE):
    print(f"\n🚀 Processing batch #{batch_num} with {len(batch)} accounts...")
    updates = []

    for idx, acc in enumerate(batch, 1):
        accountid = acc['Id']
        account_name = acc.get('Name', 'Unknown')
        print(f"   ➤ [{idx}/{len(batch)}] Account: {account_name} ({accountid})")

        note_texts = []
        try:
            linked_docs = sf.query(f"SELECT ContentDocumentId FROM ContentDocumentLink WHERE LinkedEntityId = '{accountid}'")['records']
            doc_ids = [d['ContentDocumentId'] for d in linked_docs]
            for doc_id in doc_ids:
                versions = sf.query(f"SELECT Id, FileType FROM ContentVersion WHERE ContentDocumentId = '{doc_id}' ORDER BY CreatedDate DESC LIMIT 1")['records']
                for version in versions:
                    version_id = version['Id']
                    filetype = version.get('FileType')
                    url = f"https://{instance}/services/data/v62.0/sobjects/ContentVersion/{version_id}/VersionData"
                    headers = {'Authorization': f'Bearer {session_id}'}
                    response = requests.get(url, headers=headers)
                    if response.status_code != 200:
                        continue
                    content_bytes = response.content

                    if filetype in ['PLAINTEXT', 'HTML']:
                        content = content_bytes.decode('utf-8', errors='ignore')
                        if content.strip():
                            note_texts.append(content)
                    elif filetype == 'SNOTE':
                        try:
                            content = content_bytes.decode('utf-8', errors='ignore')
                            decoded = safe_base64_decode(content)
                            if decoded and decoded.strip():
                                note_texts.append(decoded)
                        except Exception as e:
                            print(f"      ⚠️ Failed processing SNOTE content: {e}")
                    elif filetype == 'WORD_X':
                        docx_text = extract_docx_text_from_bytes(content_bytes)
                        if docx_text:
                            note_texts.append(docx_text)
                    else:
                        print(f"      ⚠️ Skipping note with unsupported FileType: {filetype}")
        except Exception as e:
            print(f"      ⚠️ Failed fetching notes: {e}")

        try:
            task_query = f"SELECT Subject, Description, ActivityDate FROM `southern-coda-233109.salesforce_backups.task_snapshots` WHERE AccountId = '{accountid}' ORDER BY ActivityDate ASC"
            tasks = [dict(row) for row in bq_client.query(task_query).result()]
        except Exception as e:
            print(f"      ⚠️ Failed fetching tasks: {e}")
            tasks = []

        try:
            event_query = f"SELECT Subject, Description, StartDateTime FROM `southern-coda-233109.salesforce_backups.event_snapshots` WHERE AccountId = '{accountid}' ORDER BY StartDateTime DESC"
            events = [dict(row) for row in bq_client.query(event_query).result()]
        except Exception as e:
            print(f"      ⚠️ Failed fetching events: {e}")
            events = []

        if not any([note_texts, tasks, events]):
            print("      ⚠️ Skipping: No data to summarize.")
            continue

        combined_text = (
            f"### Notes ({len(note_texts)} total):\n" + "\n".join(note_texts) + "\n\n" +
            summarize_records(tasks, f"### Tasks ({len(tasks)} total)") + "\n\n" +
            summarize_records(events, f"### Events ({len(events)} total)")
        )

        try:
            print("      🧠 Generating AI summary...")
            prompt = f"""
            You are a helpful assistant for Salesforce sales reps.

            Generate a concise and well-structured summary of the account history for **{account_name}**, based on the provided Notes, Tasks, and Events.

            Please follow this EXACT format:

            # {account_name} Summary

            ### 1. Key Themes
            *  **Subheader 1:** Brief explanation or related points.

            ### 2. Customer Concerns
            *  **Subheader 1:** Brief explanation or related points.

            ### 3. Prior Interactions
            *  **Subheader 1:** Brief explanation or related points.

            ### 4. Open Issues / Opportunities
            *  **Subheader 1:** Brief explanation or related points.

            Rules:
            - Use only "###" for section headings.
            - Use "**Subheader:** Description" format.
            - If no content exists for a section, write "(No items found)".

            --- START OF ACCOUNT DATA ---

            {combined_text}

            --- END OF ACCOUNT DATA ---
            """
            gemini_response = model.generate_content(prompt)
            summary = gemini_response.candidates[0].content.parts[0].text
            enforced_summary = enforce_summary_structure(summary)
            updates.append({
                'Id': accountid,
                'AI_Summary__c': enforced_summary,
                'Temp__c': True
            })
        except Exception as e:
            print(f"      ❌ Gemini summary failed: {e}")

    if updates:
        try:
            results = sf.bulk.Account.update(updates)
            print(f"✅ Batch #{batch_num} completed. {len(results)} accounts updated.")
        except Exception as e:
            print(f"❌ Bulk update failed: {e}")
    else:
        print(f"⚠️ Batch #{batch_num} had no updates.")

    batch_num += 1

print("\n🎉 All batches completed.")