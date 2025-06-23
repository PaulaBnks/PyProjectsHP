# doc_writer.py

from googleapiclient.discovery import build
from google.oauth2 import service_account

def get_google_docs_service():
    """Authenticate and return Google Docs service"""
    from config import SERVICE_ACCOUNT_FILE, SCOPES
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES
    )
    service = build('docs', 'v1', credentials=creds)
    print("✅ Connected to Google Docs API")
    return service

def clear_and_update_google_doc(service, document_id, content):
    """
    Clears the current content of a Google Doc (excluding the title) and inserts new content.
    """
    from config import DOCUMENT_ID
    try:
        # Get the full document to determine actual content range
        doc = service.documents().get(documentId=document_id).execute()
        body_content = doc.get('body', {}).get('content', [])
        if not body_content:
            print("⚠️ Document body is empty. Skipping deletion.")
            end_index = 1
        else:
            end_index = body_content[-1].get('endIndex', 1)
            if end_index > 1:
                end_index -= 1  # Avoid deleting the final newline
        requests = []
        if end_index > 1:
            requests.append({
                'deleteContentRange': {
                    'range': {
                        'startIndex': 1,
                        'endIndex': end_index
                    }
                }
            })
        requests.append({
            'insertText': {
                'location': {'index': 1},
                'text': content
            }
        })
        result = service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()
        print("✅ Customer summary successfully updated in Google Doc.")
    except Exception as e:
        print(f"❌ Failed to update Google Doc: {e}")