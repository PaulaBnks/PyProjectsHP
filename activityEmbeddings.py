from simple_salesforce import SalesforceLogin, Salesforce
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
import json
import google.generativeai as genai
import requests
import base64
from docx import Document
from io import BytesIO
from datetime import datetime
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

# --- HELPER FUNCTIONS ---

def strip_html(text):
    """Remove HTML tags and decode HTML entities."""
    if not text:
        return ""
    try:
        soup = BeautifulSoup(text, "html.parser")
        clean_text = soup.get_text(separator="\n", strip=True)
        return clean_text
    except Exception as e:
        print(f"⚠️ Error stripping HTML: {e}")
        return text.strip()

def preprocess_note_content(text):
    """
    Removes unhelpful lines like Avoma URLs and JavaScript placeholders.
    Keeps actual meeting content.
    """
    lines = text.splitlines()
    cleaned_lines = []
    exclude_patterns = [
        r"^Avoma Meeting\s*$",
        r"https?://",
        r"^You need to enable JavaScript to run this app.$"
    ]
    for line in lines:
        line_clean = line.strip()
        if not line_clean:
            continue  # Skip empty lines
        excluded = any(re.search(pattern, line_clean) for pattern in exclude_patterns)
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

def convert_bq_timestamp(dt_str):
    """Convert timestamp string from BigQuery format to datetime object"""
    try:
        return datetime.strptime(dt_str, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        try:
            return datetime.strptime(dt_str.split('.')[0], "%Y-%m-%dT%H:%M:%S")
        except Exception as e:
            print(f"⚠️ Could not parse date: {dt_str}")
            return None

def get_embedding(text):
    result = genai.embed_content(
        model="models/embedding-001",
        content=text,
        task_type="retrieval_document"
    )
    return result["embedding"]

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

# --- MAIN SCRIPT LOGIC ---

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