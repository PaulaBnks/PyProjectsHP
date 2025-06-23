# note_extractor.py

import requests
import base64
import re
from docx import Document
from io import BytesIO

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
            #print(f"⚠️ Unsupported file type: {filetype}")
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