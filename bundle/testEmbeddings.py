from google.cloud import bigquery
from google.oauth2 import service_account
import google.generativeai as genai
import numpy as np
from datetime import datetime
import os
import json
import re
from bs4 import BeautifulSoup

# --- CONFIGURATION ---
ACCOUNT_ID = "00109000013HoXkAAK"  # Replace with your test account ID
PROJECT_ID = "southern-coda-233109"
DATASET_ID = "import"
TABLE_ID = "salesforce_embeddings"

# --- AUTHENTICATION ---
print("🔐 Loading credentials...")
credentials = service_account.Credentials.from_service_account_file(
    r"F:\salesforce\tools\python-project-403413-480829366e0a.json"
)
bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
print("✅ BigQuery login successful.")

# Authenticate Gemini
print("🔐 Loading Gemini API key...")
with open(r"F:\salesforce\tools\gemini_prod_key.txt", "r") as file:
    api_key = file.read().strip()
genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-2.0-flash")
print("✅ Gemini model initialized.")


# --- HELPER FUNCTIONS ---

def strip_html(text):
    """Remove HTML tags and decode HTML entities."""
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    clean_text = soup.get_text(separator=" ")
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    return clean_text


def get_embeddings_for_account(account_id):
    """Fetch all embeddings and raw text for an account"""
    query = f"""
        SELECT id, raw_text, embedding 
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE account_id = '{account_id}'
    """
    rows = bq_client.query(query).result()
    return [(row.id, strip_html(row.raw_text), row.embedding) for row in rows]


def get_query_embedding(question):
    """Generate embedding for a question/query"""
    result = genai.embed_content(
        model="models/embedding-001",
        content=question,
        task_type="retrieval_query"
    )
    return result["embedding"]


def cosine_similarity(vec1, vec2):
    """Compute cosine similarity between two vectors"""
    v1 = np.array(vec1)
    v2 = np.array(vec2)
    return np.dot(v1, v2) / (np.linalg.norm(v1) * np.linalg.norm(v2))


def find_most_relevant_note(query_embedding, embeddings_data):
    """Find top 3 most similar notes based on cosine similarity"""
    similarities = []
    for version_id, raw_text, embedding in embeddings_data:
        if len(embedding) != len(query_embedding):
            continue  # Skip mismatched dimensions
        sim = cosine_similarity(query_embedding, embedding)
        similarities.append((sim, raw_text[:1000]))  # Limit context size
    similarities.sort(reverse=True, key=lambda x: x[0])
    return similarities[:3]  # Top 3 matches


def ask_question_with_context(question, relevant_notes):
    """Ask Gemini a question using only the most relevant note snippets"""
    context_block = "\n".join([f"{text}" for _, text in relevant_notes])

    full_prompt = f"""
        Du bist ein hilfreicher Assistent für Salesforce-Vertriebsmitarbeiter bei Cosuno.
        Deine Aufgabe ist es, die folgende Frage basierend auf den bereitgestellten Meetingnotizen zu beantworten.

        ### Frage:
        {question}

        ### Relevanteste Notizen:
        {context_block}

        ### Anweisungen:
        - Beantworte die Frage nur basierend auf den obigen Notizen.
        - Falls keine Note die Antwort enthält, antworte mit: "(Keine relevante Info gefunden)"
        - Halte deine Antwort prägnant und direkt.

        ### Antwort:
    """

    try:
        response = model.generate_content(full_prompt)
        return response.candidates[0].content.parts[0].text.strip()
    except Exception as e:
        return f"[Error]: {e}"


# --- MAIN TESTING LOGIC ---

print(f"\n🧠 Fetching embeddings for Account: {ACCOUNT_ID}")
embeddings_data = get_embeddings_for_account(ACCOUNT_ID)

if not embeddings_data:
    print("⚠️ No embeddings found for this account.")
else:
    print(f"✅ Found {len(embeddings_data)} embedded notes.\n")

    # Define test questions
    questions = {
        "Was sind die wichtigsten Erkenntnisse?": "1. Key Takeaways",
        "Welche nächsten Schritte wurden besprochen?": "2. Action Items",
        "Wer sind die Entscheidungsträger?": "6. Decision Makers",
        "Welche Probleme wurden genannt?": "4. Pain Points",
        "Wie läuft der Ausschreibungsprozess aktuell ab?": "Current Tendering Process",
        "Wie sieht der Entscheidungsprozess aus?": "5. Decision Process",
        "Wie lautet das Unternehmensprofil?": "3. Background",
    }

    # Ask each question and show results
    for question, section in questions.items():
        print(f"\n{'-'*50}\n")
        query_emb = get_query_embedding(question)
        relevant_notes = find_most_relevant_note(query_emb, embeddings_data)
        
        print(f"📌 Section: {section}")
        print(f"❓ Frage: {question}")
        
        answer = ask_question_with_context(question, relevant_notes)
        print(f"🧠 Gemini Antwort: {answer}")

        print("\n📎 Verwendete Notizen:")
        for idx, (sim, text) in enumerate(relevant_notes):
            print(f"\nNote {idx+1} (Ähnlichkeit: {sim:.4f})\n{text[:200]}...")
