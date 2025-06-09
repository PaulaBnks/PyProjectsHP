from simple_salesforce import Salesforce, SalesforceLogin
from google.cloud import bigquery
import json
from google import genai
import requests
import os

# Set your GCP credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"D:\software\python-project-403413-480829366e0a.json"

# Initialize BigQuery client
bq_client = bigquery.Client()

# Authenticate to Salesforce (only needed for Notes)
loginInfo = json.load(open('loginpd.json'))
username = loginInfo['username']
password = loginInfo['password']
security_token = loginInfo['security_token']

session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token)
sf = Salesforce(instance=instance, session_id=session_id)

# Target Account ID
accountid = '0015q00000AHIPvAAP'

# Authenticate to Gemini
with open(r"D:\software\gemini_key_file.txt", "r") as file:
    api_key = file.read().strip()

# Initialize Gemini client
client = genai.Client(api_key=api_key)

# ----------- Fetch Account Name -----------
account = sf.Account.get(accountid)
account_name = account.get('Name', 'Unknown')

###Functions
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
        if points:
            result += "\n".join(points) + "\n"
        else:
            result += "* (No items found)\n"

    return result

# ----------- Fetch Notes (ContentNotes) from Salesforce -----------
linked_docs = sf.query(f"""
    SELECT ContentDocumentId 
    FROM ContentDocumentLink 
    WHERE LinkedEntityId = '{accountid}'
""")['records']

print(f"🔍 Found {len(linked_docs)} linked ContentDocument(s) for Account {accountid} ({account_name})")

doc_ids = [d['ContentDocumentId'] for d in linked_docs]
note_texts = []

for doc_id in doc_ids:
    versions = sf.query(f"""
        SELECT Title, VersionData, Id 
        FROM ContentVersion 
        WHERE ContentDocumentId = '{doc_id}'
        ORDER BY CreatedDate DESC
        LIMIT 1
    """)['records']
    
    for version in versions:
        version_id = version['Id']
        url = f"https://{instance}/services/data/v62.0/sobjects/ContentVersion/{version_id}/VersionData"
        headers = {'Authorization': f'Bearer {session_id}'}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            try:
                content = response.content.decode('utf-8')
                if content.strip():
                    note_texts.append(content)
            except UnicodeDecodeError:
                pass

# ----------- Fetch Tasks from BigQuery -----------
task_query = f"""
SELECT Subject, Description, ActivityDate 
FROM `southern-coda-233109.estuary_flow_salesforce.task` 
WHERE AccountId = '{accountid}'
ORDER BY ActivityDate ASC
"""
#tasks = [dict(row) for row in bq_client.query(task_query).result()]
tasks = [dict(row) for row in bq_client.query(task_query).result()]
print(f"✅ Retrieved {len(tasks)} task(s) from BigQuery for Account {accountid}")

# ----------- Fetch Events from BigQuery -----------
event_query = f"""
SELECT Subject, Description, StartDateTime 
FROM `southern-coda-233109.estuary_flow_salesforce.event` 
WHERE AccountId = '{accountid}'
ORDER BY StartDateTime DESC
"""
#events = [dict(row) for row in bq_client.query(event_query).result()]
events = [dict(row) for row in bq_client.query(event_query).result()]
print(f"✅ Retrieved {len(events)} event(s) from BigQuery for Account {accountid}")

# ----------- Prepare Combined Text -----------
def summarize_records(records, label):
    lines = [f"{label}:\n"]
    for r in records:
        desc = r.get('Description') or ''
        subj = r.get('Subject') or ''
        if desc:
            lines.append(f"• {subj}: {desc}")
    return "\n".join(lines)

combined_text = (
    f"### Notes ({len(note_texts)} total):\n" + "\n".join(note_texts) + "\n\n" +
    summarize_records(tasks, f"### Tasks ({len(tasks)} total)") + "\n\n" +
    summarize_records(events, f"### Events ({len(events)} total)")
)

# ----------- Gemini Summarization -----------
prompt = f"""
You are a helpful assistant for Salesforce sales reps.

Generate a concise and well-structured summary of the account history for **{account_name}**, based on the provided Notes, Tasks, and Events.

Please follow this EXACT format:

# {account_name} Summary

### 1. Key Themes
*  **Subheader 1:** Brief explanation or related points.
*  **Subheader 2:** Brief explanation or related points.

### 2. Customer Concerns
*  **Subheader 1:** Brief explanation or related points.
*  **Subheader 2:** Brief explanation or related points.

### 3. Prior Interactions
*  **Subheader 1:** Brief explanation or related points.
*  **Subheader 2:** Brief explanation or related points.

### 4. Open Issues / Opportunities
*  **Subheader 1:** Brief explanation or related points.
*  **Subheader 2:** Brief explanation or related points.

Rules:
- Use only "###" for section headings.
- Use "**Subheader:** Description" format for each bullet point (bold subheaders followed by colon and explanation).
- Subheaders should be short, descriptive labels (e.g., Budget Issues, Product Questions).
- Use "*" for bullet points only.
- No markdown beyond ### and * (no bolding in output, just structure).
- Avoid repeating information across sections.
- Keep each bullet point concise.
- If no relevant info exists for a section, leave it blank but keep the heading.
- Input text may be in **German**, **English**, or a mix of both. Always output the summary in **English**.
- Output the summary in **English**, regardless of the language of the input text.
- Always include all four headings (1. Key Themes, 2. Customer Concerns, 3. Prior Interactions, 4. Open Issues / Opportunities), even if there's no content.
- If there is no relevant content for a section, write "(No items found)" under the heading.



--- START OF ACCOUNT DATA ---

{combined_text}

--- END OF ACCOUNT DATA ---
"""

response = client.models.generate_content(
    model="gemini-2.0-flash",
    contents=[
        {
            "role": "user",
            "parts": [
                {
                    "text": prompt
                }
            ]
        }
    ]
)

summary = response.candidates[0].content.parts[0].text
enforced_summary = enforce_summary_structure(summary)

try:
    sf.Account.update(accountid, {
        'AI_Summary__c': enforced_summary
    })
    print(f"✅ Successfully updated AI_Summary__c field on Account {accountid}")
except Exception as e:
    print(f"❌ Failed to update AI_Summary__c field: {e}")