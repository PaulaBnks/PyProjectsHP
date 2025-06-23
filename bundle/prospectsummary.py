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

# Authenticate BigQuery
print("🔐 Logging into BigQuery...")
credentials = service_account.Credentials.from_service_account_file(
    r"E:\Software\python-project-403413-480829366e0a.json"
)
bq_client = bigquery.Client(project='southern-coda-233109', credentials=credentials)
print("✅ BigQuery login successful.")

# Authenticate Gemini
print("🔐 Loading Gemini API key...")
with open(r"E:\Software\gemini_prod_key.txt", "r") as file:
    api_key = file.read().strip()
genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-2.0-flash")
print("✅ Gemini model initialized.")


######################################################################################################################

# Cosuno Context for Gemini AI (in English for dev clarity)
COSUNO_CONTEXT = """
Cosuno is a cloud-based B2B SaaS platform that digitizes and automates the procurement process for the construction industry.
Designed for general contractors, property developers, architects, and planners, Cosuno streamlines the traditionally manual
and paper-driven procurement workflow by enabling seamless collaboration, automated tendering, and intelligent data analysis.

By connecting demand-side businesses with a growing network of over 80,000 subcontractors across Europe, Cosuno reduces procurement
time by up to 60%, increases bid volume, and enhances decision-making through real-time insights. The platform empowers construction
companies to save time, cut costs, and maximize profit margins—while fostering transparency and efficiency in one of the world's
least-digitized industries.

With its intuitive interface and two-sided marketplace model, Cosuno not only simplifies the bidding and tendering process but also
helps users build long-term subcontractor networks, ensuring smarter, faster, and more collaborative project execution.
"""



# --- HELPER FUNCTIONS ---

def chunked(iterable, size):
    """Split iterable into chunks of max size."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def summarize_records(records, label):
    """Summarize task or event records into text format."""
    lines = [f"{label}:"]
    for r in records:
        desc = r.get('Description') or ''
        subj = r.get('Subject') or ''
        if desc:
            lines.append(f"• {subj}: {desc}")
    return "\n".join(lines)

def enforce_summary_structure(summary):
    """Ensure output follows expected structure."""
    sections = {
        "1. Key Takeaways": [],
        "2. Action Items": [],
        "3. Background": [],
        "4. Pain Points": [],
        "5. Decision Process": [],
        "6. Decision Makers": []
    }
    current_section = None
    lines = summary.splitlines()
    for line in lines:
        line = line.strip()
        if line.startswith("### 1. Key Takeaways"):
            current_section = "1. Key Takeaways"
        elif line.startswith("### 2. Action Items"):
            current_section = "2. Action Items"
        elif line.startswith("### 3. Background"):
            current_section = "3. Background"
        elif line.startswith("### 4. Pain Points"):
            current_section = "4. Pain Points"
        elif line.startswith("### 5. Decision Process"):
            current_section = "5. Decision Process"
        elif line.startswith("### 6. Decision Makers"):
            current_section = "6. Decision Makers"
        elif line.startswith("*") and current_section:
            sections[current_section].append(line)

    result = ""
    for title, points in sections.items():
        result += f"### {title}\n"
        result += "\n".join(points) + "\n" if points else "* (No entries found)\n"
    return result

BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

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

def extract_docx_text_from_bytes(file_bytes):
    """Extract text from Word document bytes."""
    try:
        doc = Document(BytesIO(file_bytes))
        return "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
    except Exception as e:
        print(f"⚠️ DOCX could not be processed: {e}")
        return None

# --- MAIN SCRIPT LOGIC ---

print("📡 Query Salesforce for target accounts...")
query = """
    SELECT Id, Name, AI_Summary__c 
    FROM Account 
    WHERE Account_Status__c = 'Prospect' AND CompanyType__c != 'Subcontractor' AND Temp__c = False
"""
accounts = []
response = sf.query(query)
accounts.extend(response['records'])
while not response['done']:
    response = sf.query_more(response['nextRecordsUrl'], True)
    accounts.extend(response['records'])

accounts = [acc for acc in accounts if not acc.get('AI_Summary__c')]
print(f"🔍 Found: {len(accounts)} Accounts for processing.")

BATCH_SIZE = 200
batch_num = 1

for batch in chunked(accounts, BATCH_SIZE):
    print(f"\n🚀 Processing batch #{batch_num} with {len(batch)} accounts...")
    updates = []
    for idx, acc in enumerate(batch, 1):
        accountid = acc['Id']
        account_name = acc.get('Name', 'Unknown')
        print(f"   ➤ [{idx}/{len(batch)}] Konto: {account_name} ({accountid})")

        # Fetch Notes
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
                        content = content_bytes.decode('utf-8', errors='ignore')
                        decoded = safe_base64_decode(content)
                        if decoded and decoded.strip():
                            note_texts.append(decoded)
                    elif filetype == 'WORD_X':
                        docx_text = extract_docx_text_from_bytes(content_bytes)
                        if docx_text:
                            note_texts.append(docx_text)
        except Exception as e:
            print(f"⚠️ Error while fetching notes: {e}")

        # Fetch Tasks
        try:
            task_query = f"""
                SELECT Subject, Description, ActivityDate
                FROM `southern-coda-233109.salesforce_backups.task_snapshots`
                WHERE AccountId = '{accountid}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY LastModifiedDate DESC) = 1
                ORDER BY ActivityDate ASC
            """
            tasks = [dict(row) for row in bq_client.query(task_query).result()]
        except Exception as e:
            tasks = []

        # Fetch Events
        try:
            event_query = f"""
                SELECT Subject, Description, StartDateTime
                FROM `southern-coda-233109.salesforce_backups.event_snapshots`
                WHERE AccountId = '{accountid}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY Id ORDER BY LastModifiedDate DESC) = 1
                ORDER BY StartDateTime DESC
            """
            events = [dict(row) for row in bq_client.query(event_query).result()]
        except Exception as e:
            events = []

        if not any([note_texts, tasks, events]):
            print("⚠️ Skipping: No data available to summarize.")
            continue

        combined_text = (
            f"### Notes ({len(note_texts)} total):\n" + "\n".join(note_texts) + "\n" +
            summarize_records(tasks, f"### Tasks ({len(tasks)} total)") + "\n" +
            summarize_records(events, f"### Events ({len(events)} total)")
        )

        try:
            print("🧠 Generiere KI-Zusammenfassung...")
            prompt = f"""
{COSUNO_CONTEXT}

Du bist ein hilfreicher Assistent für Salesforce-Vertriebsmitarbeiter bei Cosuno. Deine Aufgabe ist es, basierend auf den bereitgestellten Notizen, Aufgaben und Ereignissen, eine prägnante und gut strukturierte Zusammenfassung des Account-Verlaufs für **{account_name}** zu erstellen.

Bitte halte dich strikt an dieses Format:

# {account_name} - Sandler Zusammenfassung

### 1. Key Takeaways
* **Subheader:** Kurze Erklärung oder verwandte Punkte.


### 2. Action Items
* **Subheader:** Kurze Erklärung oder verwandte Punkte.


### 3. Background
* **Subheader:** Kurze Erklärung oder verwandte Punkte.


### 4. Pain Points
* **Subheader:** Kurze Erklärung oder verwandte Punkte.


### 5. Decision Process
* **Subheader:** Kurze Erklärung oder verwandte Punkte.


### 6. Decision Makers
* **Subheader:** Kurze Erklärung oder verwandte Punkte.


#### 🧠 Abschnittsdefinitionen (für Modellinterne Steuerung)
1. **Key Takeaways**: Wichtigste Erkenntnisse aus den Interaktionen.
2. **Action Items**: Nächste Schritte oder zu erledigende Aufgaben.
3. **Background**: Unternehmensprofil, Rolle, Zuständigkeiten, aktueller Ausschreibungsprozess.
4. **Pain Points**: Herausforderungen, Frustrationen oder Hindernisse.
5. **Decision Process**: Wie Entscheidungen getroffen werden, inklusive Stufen, Kriterien und Einflussfaktoren.
6. **Decision Makers**: Personen oder Rollen, die an Einkaufsentscheidungen beteiligt sind.

#### 🔒 Formatvorgaben
- Verwende nur "###" für Überschriften.
- Jeder Punkt muss das Format "* **Subheader:** Beschreibung" haben.
- Subheaders sollten kurze Labels sein (z.B. Budgetbeschränkungen).
- Keine Markdown-Formatierung außer ### und *.
- Wiederhole keine Informationen in verschiedenen Abschnitten.
- Halte jeden Punkt kurz.
- Falls keine relevanten Informationen vorliegen, schreibe "(Keine Einträge gefunden)".
- Falls jedoch Interaktionen vorliegen, extrahiere nützliche Muster oder Einsichten.
- Gib immer alle sechs Abschnitte aus, auch wenn Inhalt knapp ist.
- Eingabe kann gemischt in Deutsch/Englisch sein. Ausgabe muss immer **Deutsch** sein.

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
            print(f"❌ AI summarization error: {e}")

    if updates:
        try:
            results = sf.bulk.Account.update(updates)
           print(f"✅ Batch #{batch_num} completed. {len(results)} accounts updated.")
        except Exception as e:
            print(f"❌ Bulk update failed: {e}")
    else:
        print(f"⚠️ Batch #{batch_num} had no updates.")
    batch_num += 1

print("\n🎉 All batches were processed successfully.")