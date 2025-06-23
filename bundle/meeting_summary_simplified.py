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
MAX_NOTE_LENGTH = 50000  # Maximum length of notes to process

# --- AUTHENTICATION ---
def login_to_salesforce():
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
    return sf, session_id, instance

# Gemini Setup
def setup_gemini():
    print("🔐 Loading Gemini API key...")
    with open(r"E:\Software\gemini_prod_key.txt", "r") as file:
        gemini_api_key = file.read().strip()
    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel("gemini-2.0-flash")
    print("✅ Gemini model initialized.")
    return model

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
    """Check if a note is recent (within last 180 days)"""
    try:
        record_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (datetime.now() - record_date) < timedelta(days=180)
    except Exception:
        return False

def extract_note_content(version, session_id, instance):
    """
    Extracts content from a ContentVersion record based on its type.
    Handles different file types and avoids decoding plain HTML/TEXT as Base64.
    """
    version_id = version['Id']
    filetype = version.get('FileType')
    download_url = f"https://{instance}/services/data/v62.0/sobjects/ContentVersion/{version_id}/VersionData"   
    headers = {'Authorization': f'Bearer {session_id}'}
    try:
        content_response = requests.get(download_url, headers=headers, timeout=10)
        if content_response.status_code != 200:
            print(f"❌ Failed to download content: HTTP {content_response.status_code}")
            return None, ''
        content_bytes = content_response.content
        # Handle known file types
        if filetype in ['PLAINTEXT', 'HTML']:
            decoded_text = content_bytes.decode('utf-8', errors='ignore').strip()
            return decoded_text, version.get('CreatedDate')
        elif filetype == 'SNOTE':
            decoded_data = content_bytes.decode('utf-8', errors='ignore').strip()
            if re.match(r'^[A-Za-z0-9+/]+={0,2}$', decoded_data):
                missing_padding = len(decoded_data) % 4
                if missing_padding:
                    decoded_data += '=' * (4 - missing_padding)
                decoded_bytes = base64.b64decode(decoded_data)
                decoded_text = decoded_bytes.decode('utf-8', errors='replace')
                if decoded_text.count('\uFFFD') > len(decoded_text) // 4:
                    raise ValueError("Too many invalid characters after decode")
                return decoded_text, version.get('CreatedDate')
            else:
                return decoded_data, version.get('CreatedDate')
        elif filetype == 'WORD_X':
            return extract_docx_text_from_bytes(content_bytes), version.get('CreatedDate')
        else:
            print(f"⚠️ Unsupported file type: {filetype}")
            return None, ''
    except Exception as e:
        print(f"❌ Error extracting content: {e}")
        return None, ''

def extract_docx_text_from_bytes(file_bytes):
    """Extract text from a Word document (.docx) file"""
    try:
        doc = Document(BytesIO(file_bytes))
        return "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
    except Exception as e:
        print(f"❌ DOCX extraction error: {e}")
        return None

# --- QUESTIONS TO ASK GEMINI ---
questions = [
    ("Was sind die wichtigsten Erkenntnisse?", "1. Key Takeaways"),
    ("Welche nächsten Schritte wurden im letzten Meeting besprochen?", "2. Action Items"),
    ("Was ist das Unternehmensprofil?", "3. Background"),
    ("Wie sieht der Ausschreibungsprozess aus?", "3a. Current Tendering Process"),
    ("Welche Probleme wurden erwähnt?", "4. Pain Points"),
    ("Wie läuft der Entscheidungsprozess ab?", "5. Decision Process"),
    ("Wer sind die Entscheidungsträger?", "6. Decision Makers")
]

# --- MAIN SCRIPT LOGIC ---
def main():
    try:
        sf, session_id, instance = login_to_salesforce()
        model = setup_gemini()

        print("📡 Query Salesforce for target accounts...")
        query = """
            SELECT Id, Name, AI_Summary__c 
            FROM Account 
            WHERE Account_Status__c = 'Prospect' and CompanyType__c != 'Subcontractor' AND Temp__c = False
        """

        # query = """
        #     SELECT Id, Name, AI_Summary__c 
        #     FROM Account 
        #     WHERE Id = '00109000013HnuQAAS'
        # """

        accounts = []
        response = sf.query(query)
        accounts.extend(response['records'])

        while not response['done']:
            response = sf.query_more(response['nextRecordsUrl'], True)
            accounts.extend(response['records'])

        # Filter out accounts that already have a summary
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

                    all_notes = []

                    for doc_id in doc_ids:
                        versions = sf.query(f"SELECT Id, FileType, CreatedDate FROM ContentVersion WHERE ContentDocumentId = '{doc_id}' ORDER BY CreatedDate DESC LIMIT 1")['records']
                        for version in versions:
                            content, created_date = extract_note_content(version, session_id, instance)
                            if content:
                                all_notes.append({
                                    'text': content,
                                    'created_date': created_date,
                                    'is_recent': is_recent(created_date)
                                })

                    # Sort by recency
                    all_notes.sort(key=lambda x: x['created_date'] or '', reverse=True)

                    # Build combined text with emphasis on recent notes
                    recent_notes = [n['text'] for n in all_notes if n['is_recent']]
                    older_notes = [n['text'] for n in all_notes if not n['is_recent']]

                    # Double-weight recent notes
                    combined_text = "\n\n".join(recent_notes * 2 + older_notes)

                except Exception as e:
                    print(f"⚠️ Error fetching notes: {e}")
                    continue

                if not combined_text.strip():
                    print("⚠️ No recent meeting notes found. Marking Temp__c = True and skipping summary.")
                    updates.append({
                        'Id': accountid,
                        'Temp__c': True
                    })
                    continue


                summary_data = []

                for question, section_title in questions:
                    print(f"\n📌 Section: {section_title}")
                    print(f"❓ Frage: {question}")

                    prompt = f"""
                    Du bist ein Vertriebsassistent bei Cosuno und analysierst die folgenden Gesprächsnotizen eines Verkaufsgesprächs zwischen einem Cosuno-Vertriebsmitarbeiter und dem potenziellen Kundenunternehmen **{account_name}**.
                    Deine Aufgabe ist es, eine strukturierte Zusammenfassung zu erstellen, basierend auf den Inhalten der Notizen.
                    Beziehe dich ausschließlich auf Informationen aus diesen Notizen – ignoriere externe Annahmen oder Wissensdatenbanken.
                    Falls keine klaren Aussagen zu einem Thema vorliegen, antworte mit: "(Keine relevante Info gefunden)"
                    Wenn du nach dem Entscheidungsprozess gefragt wirst, fasse bitte zusammen, welche Personen involviert werden müssen, um einen Vertrag abzuschließen und welche Rolle diese haben. Füge außerdem hinzu, welche Schritte erforderlich sind, um den Vertrag zu unterschreiben.
                    Die Antwort muss immer auf Deutsch verfasst sein.

                    #### Gesprächsnotizen:
                    {combined_text[:MAX_NOTE_LENGTH]}

                    Nutze diese Informationen, um deine Antworten zu formulieren.

                    Frage:
                    {question}

                    ### Antwort im gewünschten Format:
                    Beginne immer mit:
                    #### 🔍 Gemini Antwort:
                    Danach gib deine Antwort in Form eines prägnanten Absatzes ab.
                    Beispiel:
                    #### 🔍 Gemini Antwort:
                    Der Kunde betonte, dass digitale Lösungen Priorität haben. Integration in bestehende Prozesse ist besonders wichtig, da manuelle Nachfassaktionen oft zu Verzögerungen führen.
                    """

                    try:
                        response = model.generate_content(prompt)
                        candidate = response.candidates[0].content.parts[0].text.strip()
                        match = re.search(r"#### 🔍 Gemini Antwort:\s*(.*)", candidate, re.DOTALL)
                        answer = match.group(1).strip() if match else candidate.strip()

                        summary_data.append({
                            "title": section_title,
                            "question": question,
                            "answer": answer
                        })

                    except Exception as e:
                        print(f"❌ Error generating answer: {e}")
                        summary_data.append({
                            "title": section_title,
                            "question": question,
                            "answer": f"(Fehler: {e})"
                        })

                # Convert to JSON string
                try:
                    ai_summary_json = json.dumps(summary_data, ensure_ascii=False, indent=2)
                    updates.append({
                        'Id': accountid,
                        'AI_Summary__c': ai_summary_json,
                        'Temp__c': True
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

    except Exception as e:
        print(f"❌ Critical error in main: {e}")

if __name__ == "__main__":
    main()