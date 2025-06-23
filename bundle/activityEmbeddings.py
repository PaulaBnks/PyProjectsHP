from simple_salesforce import SalesforceLogin, Salesforce
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import json
import google.generativeai as genai
import requests
import base64
from docx import Document
from io import BytesIO
from datetime import datetime, date
from google.oauth2 import service_account
from bs4 import BeautifulSoup
import re
import os
import dateparser  # For smart date parsing


###########################################################
## Generate Embeddings from Salesforce Notes,Events,Tasks##
###########################################################


# --- CONFIGURATION ---
run_on_git_actions = 'No'
# --- AUTHENTICATION ---
if run_on_git_actions == 'Yes':
    sf = Salesforce(
        username=os.environ["username"],
        password=os.environ["pwd"],
        security_token=os.environ["token"],
        client_id='python'
    )
    session_id, instance = SalesforceLogin(
        username=os.environ["username"],
        password=os.environ["pwd"],
        security_token=os.environ["token"]
    )
else:
    print("🔐 Logging into Salesforce...")
    login_info = json.load(open(r"E:\Software\loginpd.json"))
    username = login_info['username']
    password = login_info['password']
    security_token = login_info['security_token']
    session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token)
    sf = Salesforce(instance=instance, session_id=session_id)
    print("✅ Salesforce login successful.")
# Authenticate Gemini
print("🔐 Loading Gemini API key...")
with open(r"E:\Software\gemini_prod_key.txt", "r") as file:
    api_key = file.read().strip()
genai.configure(api_key=api_key)
embedding_model = genai.GenerativeModel("models/embedding-001")
print("✅ Gemini model initialized.")
# Authenticate BigQuery
credentials = service_account.Credentials.from_service_account_file(
    r"E:\Software\python-project-403413-480829366e0a.json"
)
bq_client = bigquery.Client(project='southern-coda-233109', credentials=credentials)
print("✅ BigQuery login successful.")

BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

# --- HELPER FUNCTIONS ---

def strip_html(text):
    if not text:
        print("⚠️ Empty input to strip_html")  # DEBUG
        return ""
    try:
        soup = BeautifulSoup(text, "html.parser")
        clean_text = soup.get_text(separator="\n", strip=True)
        #print("Cleaned HTML text:", repr(clean_text))  # DEBUG
        return clean_text
    except Exception as e:
        print(f"⚠️ Error stripping HTML: {e}")
        return text.strip()



def preprocess_note_content(text):
    print("Raw text before preprocessing:", repr(text))  # DEBUG
    lines = text.splitlines()
    cleaned_lines = []
    exclude_patterns = [
        r"^Avoma Meeting\s*$",
        r"https?",
        r"^You need to enable JavaScript to run this app.$"
    ]
    for line in lines:
        line_clean = line.strip()
        if not line_clean:
            continue
        excluded = any(re.search(pattern, line_clean) for pattern in exclude_patterns)
        #print(f"Line: {repr(line_clean)} | Excluded? {excluded}")  # DEBUG
        if not excluded:
            cleaned_lines.append(line_clean)
    return "\n".join(cleaned_lines)



def normalize_dates(text, base_date=None):
    """
    Converts informal or relative dates to [DD.MM.YYYY, HH:MM].
    Uses base_date (meeting start time) as reference.
    """
    def replace_match(match):
        phrase = match.group(0)
        parsed = dateparser.parse(phrase, settings={'RELATIVE_BASE': base_date})
        if parsed:
            return f"[{parsed.strftime('%d.%m.%Y, %H:%M')}]"
        return phrase
    pattern = r"(next \w+ at \d{1,2}:\d{2})|(\w+day at \d{1,2}:\d{2})|(tomorrow at \d{1,2}:\d{2})|(\d{1,2}:\d{2}\s*[APMapm]+\s*\w+\s*\d{1,2},\s*\d{4})"
    return re.sub(pattern, replace_match, text, flags=re.IGNORECASE)




def convert_bq_timestamp(dt_val):
    
    if dt_val is None:
        #print("⚠️ Received None as date input")
        return None
    
    # If it's already a datetime object, remove timezone and return
    if isinstance(dt_val, datetime):
        return dt_val.replace(tzinfo=None)

    # If it's a date object (no time), convert to datetime
    if isinstance(dt_val, date):
        return datetime.combine(dt_val, datetime.min.time())

    # If it's a string, try parsing
    if isinstance(dt_val, str):
        try:
            # Try format: 'YYYY-MM-DD HH:MM:SS'
            return datetime.strptime(dt_val, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                # Try format: 'YYYY-MM-DDTHH:MM:SS' (like ISO without fractional seconds)
                return datetime.strptime(dt_val.split('.')[0], "%Y-%m-%dT%H:%M:%S")
            except ValueError:
                try:
                    # Try full ISO format with timezone: '2025-04-04 09:00:00+00:00'
                    # Use fromisoformat which handles many formats including timezone
                    parsed = datetime.fromisoformat(dt_val.replace('Z', '+00:00'))
                    return parsed.replace(tzinfo=None)
                except Exception as e:
                    print(f"⚠️ Could not parse date string: {dt_val}")
                    return None

    # Fallback for any other unexpected type
    print(f"⚠️ Unexpected date type: {repr(dt_val)} (type: {type(dt_val)})")
    return None




def get_embedding(text):
    result = genai.embed_content(
        model="models/embedding-001",
        content=text,
        task_type="retrieval_document"
    )
    return result["embedding"]


def extract_note_content(version, session_id, instance):
    version_id = version['Id']
    filetype = version.get('FileType')
    url = f"https://{instance}/services/data/v62.0/sobjects/ContentVersion/{version_id}?fields=CreatedDate"
    headers = {'Authorization': f'Bearer {session_id}'}
    meta_response = requests.get(url, headers=headers)
    created_date = meta_response.json().get('CreatedDate', '') if meta_response.status_code == 200 else ''

    download_url = f"https://{instance}/services/data/v62.0/sobjects/ContentVersion/{version_id}/VersionData"
    content_response = requests.get(download_url, headers=headers)

    if content_response.status_code != 200:
        return None, created_date

    content_bytes = content_response.content
    if filetype in ['PLAINTEXT', 'HTML']:
        content = content_bytes.decode('utf-8', errors='ignore')
        return content.strip(), created_date
    elif filetype == 'SNOTE':
        decoded = safe_base64_decode(content_bytes.decode('utf-8', errors='ignore'))
        return decoded.strip(), created_date
    elif filetype == 'WORD_X':
        return extract_docx_text_from_bytes(content_bytes), created_date
    return None, created_date


def safe_base64_decode(data, account_id=None):
    """Attempts to decode base64 content safely."""
    if not data or (isinstance(data, str) and data.strip() == ''):
        return ''
    if isinstance(data, bytes):
        try:
            data = data.decode('utf-8')
        except UnicodeDecodeError:
            try:
                data = data.decode('latin1')
            except Exception as e:
                print("      ⚠️ Unable to decode byte stream.")
                return "[Unreadable note content: Invalid byte stream]"
    data = data.strip()
    if len(data) >= 2 and all(c in BASE64_CHARS for c in data[:50]):
        pass
    else:
        return data
    missing_padding = len(data) % 4
    if missing_padding:
        data += '=' * (4 - missing_padding)
    try:
        decoded_bytes = base64.b64decode(data, validate=False, altchars=None)
        decoded_text = decoded_bytes.decode('utf-8', errors='replace')
        if decoded_text.count('\uFFFD') > len(decoded_text) // 4:
            raise ValueError("Too many invalid characters after decode")
        return decoded_text
    except Exception as e:
        return f"[Unreadable note content: {data[:100]}...]"



def save_embedding(account_id, source_id, raw_text, clean_text, created_date, embedding, object_type="Note"):
    table_id = "southern-coda-233109.import.salesforce_embeddings"
    bq_created_date = created_date.strftime("%Y-%m-%d %H:%M:%S") if created_date else None
    schema = [
        SchemaField("id", "STRING"),
        SchemaField("account_id", "STRING"),
        SchemaField("object_type", "STRING"),
        SchemaField("created_date", "TIMESTAMP"),
        SchemaField("raw_text", "STRING"),
        SchemaField("clean_text", "STRING"),
        SchemaField("embedding", "FLOAT64", mode="REPEATED")
    ]
    rows_to_insert = [{
        "id": source_id,
        "account_id": account_id,
        "object_type": object_type,
        "created_date": bq_created_date,
        "raw_text": raw_text,
        "clean_text": clean_text,
        "embedding": embedding
    }]
    errors = bq_client.insert_rows(table_id, rows_to_insert, selected_fields=schema)
    if errors:
        print(f"❌ Error inserting row: {errors}")
    else:
        print(f"✅ Stored embedding for {object_type} '{source_id}'")



def convert_salesforce_time(dt_str):
    """Convert Salesforce datetime string to BigQuery-compatible format."""
    try:
        dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S.%f%z")
    except ValueError:
        try:
            dt = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S%z")
        except ValueError:
            try:
                dt = datetime.strptime(dt_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")
            except Exception as e:
                print(f"⚠️ Could not parse date: {dt_str}")
                return None
    return dt




### --- MAIN SCRIPT LOGIC ---  ###

accountid = '00109000013HoXkAAK'
print(f"\n📡 Fetching data for Account: {accountid}")

# Initialize list to store all items with text and metadata
items_to_embed = []

# 1. FETCH NOTES
try:
    linked_docs = sf.query(f"SELECT ContentDocumentId FROM ContentDocumentLink WHERE LinkedEntityId = '{accountid}'")['records']
    doc_ids = [d['ContentDocumentId'] for d in linked_docs]
    for doc_id in doc_ids:
        versions = sf.query(f"SELECT Id, FileType, ContentDocument.CreatedBy.Name, ContentDocument.CreatedDate FROM ContentVersion WHERE ContentDocumentId = '{doc_id}' ORDER BY CreatedDate DESC LIMIT 1")['records']
        for version in versions:
            content, created_date_str = extract_note_content(version, session_id, instance)
            if content:
                created_date = convert_salesforce_time(created_date_str)
                version_id = version['Id']
                items_to_embed.append({
                    'type': 'Note',
                    'id': version_id,
                    'text': content,
                    'date': created_date
                })
except Exception as e:
    print(f"⚠️ Error while fetching notes: {e}")

# 2. FETCH TASKS
task_query = f"""SELECT
                    Id,
                    Subject,
                    Description,
                    ActivityDate
                    FROM `southern-coda-233109.salesforce_backups.task_snapshots`
                    WHERE AccountId = '{accountid}'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY LastModifiedDate DESC) = 1
                    ORDER BY ActivityDate ASC"""

tasks_job = bq_client.query(task_query)
for row in tasks_job:
    subject = row.Subject or ''
    description = row.Description or ''
    full_text = f"{subject}: {description}"
    activity_date = convert_bq_timestamp(row.ActivityDate)
    items_to_embed.append({
        'type': 'Task',
        'id': row.Id,
        'text': full_text,
        'date': activity_date
    })

# 3. FETCH EVENTS
event_query = f"""SELECT
                    Id,
                    Subject,
                    Description,
                    StartDateTime
                    FROM `southern-coda-233109.salesforce_backups.event_snapshots`
                    WHERE AccountId = '{accountid}'
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY LastModifiedDate DESC) = 1
                    ORDER BY StartDateTime DESC"""

events_job = bq_client.query(event_query)
for row in events_job:
    subject = row.Subject or ''
    description = row.Description or ''
    full_text = f"{subject}: {description}"
    start_datetime = convert_bq_timestamp(row.StartDateTime)
    items_to_embed.append({
        'type': 'Event',
        'id': row.Id,
        'text': full_text,
        'date': start_datetime
    })

# --- GENERATE EMBEDDINGS FOR ALL ITEMS ---

print(f"\n🧠 Generating embeddings for {len(items_to_embed)} items...")
for idx, item in enumerate(items_to_embed):
    try:
        print(f"📌 {item['type']} {idx + 1}: {item['text'][:100]}... (ID: {item['id']})")
        # Step 0: Strip HTML if present
        html_stripped = strip_html(item['text'])
        # Step 1: Remove boilerplate lines
        clean_text = preprocess_note_content(html_stripped)
        # Step 2: Normalize dates using meeting date as reference
        enriched_text = normalize_dates(clean_text, base_date=item['date'])
        # Step 3: Generate embedding only if content is not empty
        if enriched_text.strip():
            try:
                embedding = get_embedding(enriched_text)
                print(f"🔢 Embedding (first 10 values): {embedding[:10]}\n")
            except Exception as e:
                print(f"❌ Failed to generate embedding: {e}")
                embedding = None
        else:
            print("🟡 Skipping embedding: Cleaned text is empty after preprocessing.")
            with open("empty_notes_debug.log", "a", encoding="utf-8") as f:
                f.write(f"\n--- EMPTY ITEM ID: {item['id']} ---\n")
                f.write(f"Raw Text:\n{item['text']}\nCleaned Text:\n{enriched_text}\n")
        # Step 4: Save to BigQuery
        save_embedding(
            account_id=accountid,
            source_id=item['id'],
            raw_text=item['text'],
            clean_text=enriched_text,
            created_date=item['date'],
            embedding=embedding,
            object_type=item['type']
        )
    except Exception as e:
        print(f"❌ Failed to process {item['type']} {idx+1}: {e}")