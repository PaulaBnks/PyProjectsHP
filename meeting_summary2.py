from simple_salesforce import Salesforce, SalesforceLogin
import google.generativeai as genai
import requests
import os
import json
from datetime import datetime, timedelta
from itertools import islice
import base64
from docx import Document
from io import BytesIO
import re

# --- CONFIGURATION ---
RUN_ON_GIT_ACTIONS = 'No'  # Change to 'Yes' when running in CI/CD

# --- AUTHENTICATION ---

# Salesforce Login
print("🔐 Logging into Salesforce...")
if RUN_ON_GIT_ACTIONS == 'Yes':
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
    login_info = json.load(open(r"E:\Software\loginpd.json"))
    username = login_info['username']
    password = login_info['password']
    security_token = login_info['security_token']
    session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token)
    sf = Salesforce(instance=instance, session_id=session_id)
print("✅ Salesforce login successful.")

# Gemini Setup
print("🔐 Loading Gemini API key...")
with open(r"E:\Software\gemini_prod_key.txt", "r") as file:
    gemini_api_key = file.read().strip()
genai.configure(api_key=gemini_api_key)
model = genai.GenerativeModel("gemini-2.0-flash")
print("✅ Gemini model initialized.")

# --- HELPER FUNCTIONS ---
def chunked(iterable, size):
    """Split iterable into chunks of max size."""
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def is_recent(date_str):
    try:
        record_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (datetime.now() - record_date) < timedelta(days=180)
    except Exception:
        return False

def extract_note_content(version, session_id, instance):
    version_id = version['Id']
    filetype = version.get('FileType')
    download_url = f"https://{instance}/services/data/v62.0/sobjects/ContentVersion/{version_id}/VersionData" 
    headers = {'Authorization': f'Bearer {session_id}'}
    content_response = requests.get(download_url, headers=headers)
    if content_response.status_code != 200:
        return None, ''
    content_bytes = content_response.content
    if filetype in ['PLAINTEXT', 'HTML']:
        return content_bytes.decode('utf-8', errors='ignore').strip(), ''
    elif filetype == 'SNOTE':
        decoded = safe_base64_decode(content_bytes.decode('utf-8', errors='ignore'))
        return decoded.strip(), ''
    elif filetype == 'WORD_X':
        return extract_docx_text_from_bytes(content_bytes), ''
    return None, ''

BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

def safe_base64_decode(data):
    if not data or (isinstance(data, str) and data.strip() == ''):
        return ''
    if isinstance(data, bytes):
        try:
            data = data.decode('utf-8')
        except UnicodeDecodeError:
            try:
                data = data.decode('latin1')
            except Exception:
                return "[Unreadable note content: Invalid byte stream]"
    missing_padding = len(data) % 4
    if missing_padding:
        data += '=' * (4 - missing_padding)
    try:
        decoded_bytes = base64.b64decode(data, validate=False)
        decoded_text = decoded_bytes.decode('utf-8', errors='replace')
        if decoded_text.count('\uFFFD') > len(decoded_text) // 4:
            raise ValueError("Too many invalid characters after decode")
        return decoded_text
    except Exception:
        return f"[Unreadable note content: {data[:100]}...]"

def extract_docx_text_from_bytes(file_bytes):
    try:
        doc = Document(BytesIO(file_bytes))
        return "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
    except Exception:
        return None

# --- QUESTIONS TO ASK GEMINI ---
questions = [
    ("Was sind die wichtigsten Erkenntnisse?", "1. Key Takeaways"),
    ("Welche nächsten Schritte wurden besprochen?", "2. Action Items"),
    ("Was ist das Unternehmensprofil?", "3. Background"),
    ("Wie sieht der Ausschreibungsprozess aus?", "3a. Current Tendering Process"),
    ("Welche Probleme wurden erwähnt?", "4. Pain Points"),
    ("Wie läuft der Entscheidungsprozess ab?", "5. Decision Process"),
    ("Wer sind die Entscheidungsträger?", "6. Decision Makers")
]

# --- CUSTOMER SPEECH SIMULATION ---
def extract_participants(note_text, account_name):
    """
    Parses participant names from a meeting note.
    Uses account name to identify which company the participants belong to.
    
    Args:
        note_text (str): Full text of the meeting note
        account_name (str): Name of the prospect account from Salesforce
    
    Returns:
        tuple: (customer_names, cosuno_names)
    """
    customer_names = []
    cosuno_names = []

    try:
        parts = re.split(r"Participants\s*", note_text, flags=re.IGNORECASE)
        if len(parts) < 2:
            return customer_names, cosuno_names

        participants_block = parts[1].strip()
        lines = participants_block.splitlines()

        current_company = None
        account_keywords = [account_name] + account_name.split()

        for line in lines:
            line = line.strip()
            matched = False

            # Try matching by account name
            for keyword in account_keywords:
                if keyword.lower() in line.lower():
                    current_company = "customer"
                    names = [name.strip() for name in line.split(":")[1].split(",")]
                    customer_names.extend(names)
                    matched = True
                    break
            if matched:
                continue

            # Try matching Cosuno side
            if re.match(r"(Cosuno|cosuno)", line, re.IGNORECASE):
                current_company = "cosuno"
                names = [name.strip() for name in line.split(":")[1].split(",")]
                cosuno_names.extend(names)
                matched = True
                continue
            if current_company == "customer":
                names = [name.strip() for name in line.split(",")]
                customer_names.extend(names)
                matched = True
            elif current_company == "cosuno":
                names = [name.strip() for name in line.split(",")]
                cosuno_names.extend(names)
                matched = True
            if not matched:
                continue

        # Remove duplicates
        customer_names = list(set(customer_names))
        cosuno_names = list(set(cosuno_names))

    except Exception as e:
        print(f"⚠️ Error extracting participants: {e}")

    return customer_names, cosuno_names


def simulate_customer_statements(text_block, customer_names):
    """
    Simulates customer statements based on known names from Participants section.
    """
    lines = [line.strip() for line in text_block.splitlines() if line.strip()]
    simulated_lines = []

    for line in lines:
        for name in customer_names:
            if line.startswith(name):
                simulated_lines.append(f"[Customer] {line}")
                break

    return "\n".join(simulated_lines)

# --- MAIN SCRIPT LOGIC ---
print("📡 Query Salesforce for target accounts...")
query = """
    SELECT Id, Name, AI_Summary__c 
    FROM Account 
    WHERE Id = '0015q00000J5Wk4AAF'
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
                    content, created_date = extract_note_content(version, session_id, instance)
                    if content:
                        prefix = "[RECENT]" if is_recent(created_date) else ""
                        note_texts.append(f"{prefix} * [{created_date}] Note: {content[:300]}...")
        except Exception as e:
            print(f"⚠️ Error fetching notes: {e}")

        if not note_texts:
            print("⚠️ Skipping: No meeting notes found.")
            continue

        combined_text = "\n".join(note_texts)

        # Step 1: Extract participant names using account name
        customer_names, cosuno_names = extract_participants(combined_text, account_name)

        # Step 2: Simulate customer speech
        simulated_input = simulate_customer_statements(combined_text, customer_names)

        summary_data = []

        for question, section_title in questions:
            print(f"\n📌 Section: {section_title}")
            print(f"❓ Frage: {question}")

            prompt = f"""
Du bist ein Vertriebsassistent bei Cosuno und analysierst die folgenden Gesprächsnotizen einer Gesprächsrunde zwischen dem Cosuno-Vertriebsteam und **{account_name}**.

Deine Aufgabe ist es, eine strukturierte Zusammenfassung zu erstellen, **die sich ausschließlich auf Äußerungen oder Absichten von {account_name} bezieht** – also nicht auf Cosuno-Produktbeschreibungen oder interne Diskussionen.

Falls keine klaren Aussagen von {account_name} zu einem Thema vorliegen, antworte mit: "(Keine relevante Info gefunden)"

#### Wichtige Personen aus dem Kundenunternehmen:
{', '.join(customer_names)}

#### Wichtige Personen von Cosuno:
{', '.join(cosuno_names)}

#### Simulierte Kundenaussagen (basierend auf Notizen):
{simulated_input}

#### Frage:
{question}

### Antwort im gewünschten Format:
Beginne immer mit:
#### 🔍 Gemini Antwort:
Danach gib deine Antwort aus.
Beispiel:
#### 🔍 Gemini Antwort:
* Der Kunde betonte, dass digitale Lösungen Priorität haben.
* Integration in bestehende Prozesse ist besonders wichtig.
"""

            try:
                response = model.generate_content(prompt)
                candidate = response.candidates[0].content.parts[0].text.strip()
                match = re.search(r"#### 🔍 Gemini Antwort:\s*(.*)", candidate, re.DOTALL)
                answer = match.group(1).strip() if match else candidate

                lines = [line.strip() for line in answer.splitlines() if line.strip()]
                is_bullet = any(line.startswith('- ') or line.startswith('* ') for line in lines)

                processed_answer = []
                if is_bullet:
                    for line in lines:
                        if line.startswith('- ') or line.startswith('* '):
                            processed_answer.append(line[2:])
                        else:
                            processed_answer.append(line)
                else:
                    processed_answer = answer

                summary_data.append({
                    "title": section_title,
                    "question": question,
                    "answer_type": "bullets" if is_bullet else "paragraph",
                    "answer": processed_answer
                })

            except Exception as e:
                print(f"❌ Error generating answer: {e}")
                summary_data.append({
                    "title": section_title,
                    "question": question,
                    "answer_type": "paragraph",
                    "answer": f"(Fehler: {e})"
                })

        # Convert to JSON string
        try:
            ai_summary_json = json.dumps(summary_data, ensure_ascii=False, indent=2)
            updates.append({
                'Id': accountid,
                'AI_Summary__c': ai_summary_json
            })
        except Exception as e:
            print(f"❌ JSON serialization error: {e}")

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