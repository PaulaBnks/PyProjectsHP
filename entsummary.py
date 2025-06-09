
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
model = genai.GenerativeModel("gemini-2.0-flash")
print("✅ Gemini model initialized.")



print("📡 Querying Salesforce for target accounts...")

query = """
    SELECT Id, Name, AI_Summary__c 
    FROM Account 
    WHERE Id = '0015q00000GY38oAAD'
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

        if not any([note_texts ]):
            print("      ⚠️ Skipping: No data to summarize.")
            continue

        combined_text = (
            f"### Notes ({len(note_texts)} total):\n" + "\n".join(note_texts) + "\n\n" +
            summarize_records(tasks, f"### Tasks ({len(tasks)} total)") + "\n\n" +
            summarize_records(events, f"### Events ({len(events)} total)")
        )
        #print(combined_text)

        try:
            print("      🧠 Generating AI summary...")
            
            prompt = f"""
           
            """

            gemini_response = model.generate_content(prompt)
            summary = gemini_response.candidates[0].content.parts[0].text
            enforced_summary = enforce_summary_structure(summary)
            updates.append({
                'Id': accountid,
                'AI_Summary__c': enforced_summary
                
            })
        except Exception as e:
            print(f"      ❌ Gemini summary failed: {e}")

    
