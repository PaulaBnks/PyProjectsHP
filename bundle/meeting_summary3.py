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
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- CONFIGURATION ---
RUN_ON_GIT_ACTIONS = 'No'  # Change to 'Yes' when running in CI/CD
MAX_NOTE_LENGTH = 50000  # Maximum length of notes to process
DEFAULT_LANGUAGE = 'de'  # Default language for processing

# --- AUTHENTICATION ---
def login_to_salesforce():
    """Log in to Salesforce with appropriate credentials"""
    logging.info("🔐 Logging into Salesforce...")
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
    logging.info("✅ Salesforce login successful.")
    return sf, session_id, instance

# Gemini Setup
def setup_gemini():
    """Configure the Gemini API"""
    logging.info("🔐 Loading Gemini API key...")
    with open(r"E:\Software\gemini_prod_key.txt", "r") as file:
        gemini_api_key = file.read().strip()
    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel("gemini-2.0-flash")
    logging.info("✅ Gemini model initialized.")
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
        if not date_str or date_str.strip() == '':
            return False
        record_date = datetime.strptime(date_str[:10], "%Y-%m-%d")
        return (datetime.now() - record_date) < timedelta(days=180)
    except Exception as e:
        logging.warning(f"⚠️ Error parsing date: {e}")
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
            logging.error(f"❌ Failed to download content: HTTP {content_response.status_code}")
            return None, ''
            
        content_bytes = content_response.content
        
        # Handle known file types
        if filetype in ['PLAINTEXT', 'HTML']:
            decoded_text = content_bytes.decode('utf-8', errors='ignore').strip()
            return decoded_text, version.get('CreatedDate')
        
        elif filetype == 'SNOTE':
            # Try to detect if it's actually Base64
            try:
                decoded_data = content_bytes.decode('utf-8', errors='ignore').strip()
                # Check if it looks like Base64
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
                    # Not Base64 — treat as regular text
                    return decoded_data, version.get('CreatedDate')
            except Exception as e:
                logging.warning(f"⚠️ SNOTE decode error: {e}")
                return content_bytes.decode('utf-8', errors='ignore'), version.get('CreatedDate')
        
        elif filetype == 'WORD_X':
            return extract_docx_text_from_bytes(content_bytes), version.get('CreatedDate')
            
        else:
            logging.warning(f"⚠️ Unsupported file type: {filetype}")
            return None, ''
            
    except Exception as e:
        logging.error(f"❌ Error extracting content: {e}")
        return None, ''

BASE64_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

def safe_base64_decode(data):
    """
    Safely decode a Base64 string, handling errors and padding issues.
    """
    if not data or (isinstance(data, str) and data.strip() == ''):
        return ''
    
    try:
        # Ensure the string contains only valid Base64 characters
        if not all(c in BASE64_CHARS + '=' for c in data):
            logging.error(f"❌ Invalid Base64 string: {data[:50]}...")
            return "[Unreadable note content: Invalid Base64 string]"
        
        # Add padding if necessary
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        
        # Decode the string
        decoded_bytes = base64.b64decode(data, validate=True)
        decoded_text = decoded_bytes.decode('utf-8', errors='replace')
        
        # Check for too many replacement characters
        if decoded_text.count('\uFFFD') > len(decoded_text) // 4:
            raise ValueError("Too many invalid characters after decode")
        
        return decoded_text
    
    except Exception as e:
        logging.error(f"❌ Base64 decode error: {e}")
        return f"[Unreadable note content: {data[:100]}...]"

def extract_docx_text_from_bytes(file_bytes):
    """Extract text from a Word document (.docx) file"""
    try:
        doc = Document(BytesIO(file_bytes))
        return "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
    except Exception as e:
        logging.error(f"❌ DOCX extraction error: {e}")
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
    customer_names = []
    cosuno_names = []

    parts = re.split(r"Participants\s*", note_text, flags=re.IGNORECASE)
    if len(parts) > 1:
        participants_block = parts[1].strip()
        lines = participants_block.splitlines()
        current_company = None
        account_keywords = [account_name] + account_name.split()

        for line in lines:
            line = line.strip()
            matched = False

            for keyword in account_keywords:
                if keyword.lower() in line.lower():
                    current_company = "customer"
                    names = [name.strip() for name in re.split(r",|:", line)[1:]]
                    customer_names.extend(names)
                    matched = True
                    break

            if matched:
                continue

            if re.match(r"(Cosuno|cosuno)", line, re.IGNORECASE):
                current_company = "cosuno"
                names = [name.strip() for name in re.split(r",|:", line)[1:]]
                cosuno_names.extend(names)
                matched = True
                continue

            if current_company == "customer":
                names = [name.strip() for name in re.split(r",", line)]
                customer_names.extend(names)
            elif current_company == "cosuno":
                names = [name.strip() for name in re.split(r",", line)]
                cosuno_names.extend(names)

    if not customer_names:
        name_pattern = r"\b[A-Z][a-z]+(?:\s[A-Z][a-z]+)+\b"
        possible_names = re.findall(name_pattern, note_text)
        cosuno_keywords = ["Cosuno", "Maurice", "Lisian", "Tim-Hendrik"]
        for name in possible_names:
            if any(kw in name for kw in cosuno_keywords):
                cosuno_names.append(name)
            else:
                customer_names.append(name)

    customer_names = list(set(customer_names))
    cosuno_names = list(set(cosuno_names))

    return customer_names, cosuno_names



def detect_language(text):
    """Detect the language of the given text"""
    try:
        from langdetect import detect
        return detect(text[:1000])  # Only detect from first part
    except:
        return DEFAULT_LANGUAGE

def simulate_customer_statements(text_block, customer_names, account_name, language='de'):
    """
    Simulates customer statements using:
     - Known participant names
     - Contextual/language-based keywords
     - Fallback: whole lines if no names found
    """
    lines = [line.strip() for line in text_block.splitlines() if line.strip()]
    simulated_lines = []

    # Normalize account name for matching
    account_name_clean = re.sub(r"[^\w\s]", "", account_name).strip()
    account_keywords = [account_name_clean, *account_name_clean.split()]

    # Language-specific keywords
    keyword_patterns = {
        'de': [
            r"(?i)(sie|der kunde|die teilnehmer|erwähnt|sagte|wollen|interessiert|bedenken|diskutiert|plan|in betracht ziehen|feedback)",
            *[re.escape(kw) for kw in account_keywords]
        ],
        'en': [
            r"(?i)(they|the client|the customer|mentioned|said|want|interested|concerned|discussed|plan|considering|feedback)",
            *[re.escape(kw) for kw in account_keywords]
        ]
    }

    lang_patterns = keyword_patterns.get(language, keyword_patterns['de'])
    combined_pattern = "|".join(lang_patterns)
    pattern = re.compile(combined_pattern)

    # Strategy 1: Tag lines starting with known customer names
    for line in lines:
        for name in customer_names:
            if line.startswith(name):
                simulated_lines.append(f"[Customer] {line}")
                break

    # Strategy 2: Use contextual/language-based matching
    if not simulated_lines:
        for line in lines:
            if pattern.search(line):
                simulated_lines.append(f"[Customer] {line}")

    # Strategy 3: If still nothing, assume any line mentioning account name is customer speech
    if not simulated_lines:
        for line in lines:
            if any(kw.lower() in line.lower() for kw in account_keywords):
                simulated_lines.append(f"[Customer] {line}")

    # Strategy 4: Fallback – tag all lines if we have nothing
    if not simulated_lines and lines:
        simulated_lines = [f"[Customer] {line}" for line in lines]

    return "\n".join(simulated_lines)


# --- MAIN SCRIPT LOGIC ---
def main():
    try:
        sf, session_id, instance = login_to_salesforce()
        model = setup_gemini()
        
        logging.info("📡 Query Salesforce for target accounts...")
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
            
        # Filter out accounts that already have a summary
        accounts = [acc for acc in accounts if not acc.get('AI_Summary__c')]
        logging.info(f"🔍 Found: {len(accounts)} Accounts for processing.")
        
        BATCH_SIZE = 200
        batch_num = 1
        
        for batch in chunked(accounts, BATCH_SIZE):
            logging.info(f"\n🚀 Processing batch #{batch_num} with {len(batch)} accounts...")
            updates = []
            
            for idx, acc in enumerate(batch, 1):
                accountid = acc['Id']
                account_name = acc.get('Name', 'Unknown')
                logging.info(f"   ➤ [{idx}/{len(batch)}] Konto: {account_name} ({accountid})")
                
                # Fetch Notes
                note_texts = []
                
                try:
                    linked_docs = sf.query(f"SELECT ContentDocumentId FROM ContentDocumentLink WHERE LinkedEntityId = '{accountid}'")['records']
                    doc_ids = [d['ContentDocumentId'] for d in linked_docs]
                    
                    for doc_id in doc_ids:
                        versions = sf.query(f"SELECT Id, FileType, CreatedDate FROM ContentVersion WHERE ContentDocumentId = '{doc_id}' ORDER BY CreatedDate DESC LIMIT 1")['records']
                        
                        for version in versions:
                            content, created_date = extract_note_content(version, session_id, instance)
                            
                            if content and is_recent(version.get('CreatedDate')):
                                prefix = "[RECENT]" 
                                note_texts.append(f"{prefix} * [{version.get('CreatedDate')}] Note: {content[:300]}...")
                                
                except Exception as e:
                    logging.error(f"⚠️ Error fetching notes: {e}")
                
                if not note_texts:
                    logging.warning("⚠️ Skipping: No recent meeting notes found.")
                    continue
                    
                combined_text = "\n".join(note_texts)
                
                # Detect language from combined text
                lang = detect_language(combined_text)
                
                # Step 1: Extract participant names using account name
                customer_names, cosuno_names = extract_participants(combined_text, account_name)
                
                # Step 2: Simulate customer speech
                simulated_input = simulate_customer_statements(
                    combined_text, 
                    customer_names, 
                    account_name,
                    language=lang
                )
                
                summary_data = []
                
                for question, section_title in questions:
                    logging.info(f"\n📌 Section: {section_title}")
                    logging.info(f"❓ Frage: {question}")
                    print("📄 Simulated Input:\n", simulated_input[:2000])
                    
                    prompt = f"""
Du bist ein Vertriebsassistent bei Cosuno und analysierst die folgenden Gesprächsnotizen einer Gesprächsrunde zwischen dem Cosuno-Vertriebsteam und **{account_name}**.

Deine Aufgabe ist es, eine strukturierte Zusammenfassung zu erstellen, **die sich ausschließlich auf Äußerungen oder Absichten von {account_name} bezieht** – also nicht auf Cosuno-Produktbeschreibungen oder interne Diskussionen.

Der Kunde ({account_name}) ist ein **Prospect**, kein bestehender Kunde. Interpretiere alle Aussagen im Kontext des Vertriebsprozesses und der möglichen Implementierung von Cosuno.

Falls keine klaren Aussagen von {account_name} zu einem Thema vorliegen, antworte mit: "(Keine relevante Info gefunden)"

Die Antwort muss immer auf Deutsch verfasst sein.

#### Simulierte Kundenaussagen (basierend auf Notizen):
{simulated_input}

Diese simulierten Kundenaussagen basieren auf:
- Äußerungen, die direkt von Kundenvertretern stammen (z.B. Gesprächsnotizen oder direkte Zitate)
- Kontextuellen Hinweisen, die sich auf das Unternehmen {account_name} beziehen
- Diskussionen, die im Namen des Kunden geführt wurden

Nutze diese Informationen, um deine Antworten zu formulieren.

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
                        logging.error(f"❌ Error generating answer: {e}")
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
                    logging.error(f"❌ JSON serialization error: {e}")
            
            if updates:
                try:
                    results = sf.bulk.Account.update(updates)
                    logging.info(f"✅ Batch #{batch_num} completed. {len(results)} accounts updated.")
                except Exception as e:
                    logging.error(f"❌ Bulk update failed: {e}")
            else:
                logging.warning(f"⚠️ Batch #{batch_num} had no updates.")
                
            batch_num += 1
            
        logging.info("\n🎉 All batches were processed successfully.")
        
    except Exception as e:
        logging.error(f"❌ Critical error in main: {e}", exc_info=True)

if __name__ == "__main__":
    main()