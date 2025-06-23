from google.cloud import bigquery
from google.oauth2 import service_account
import google.generativeai as genai
from datetime import datetime, timedelta
import numpy as np
from bs4 import BeautifulSoup
import re


##############################################
## Generate Summaries from Notes Embeddings###
##############################################

# --- CONFIGURATION ---
ACCOUNT_ID = "00109000013HoXkAAK"  
PROJECT_ID = "southern-coda-233109"
DATASET_ID = "import"
TABLE_ID = "salesforce_embeddings"

# --- AUTHENTICATION ---
print("🔐 Loading credentials...")
credentials = service_account.Credentials.from_service_account_file(
    r"E:\Software\python-project-403413-480829366e0a.json"
)
bq_client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
print("✅ BigQuery login successful.")

# Authenticate Gemini
print("🔐 Loading Gemini API key...")
with open(r"E:\Software\gemini_prod_key.txt", "r") as file:
    api_key = file.read().strip()
genai.configure(api_key=api_key)
model = genai.GenerativeModel("gemini-2.0-flash")
print("✅ Gemini model initialized.")


# --- COSUNO CONTEXT ---
COSUNO_CONTEXT = """
Cosuno ist eine cloudbasierte B2B SaaS-Plattform, die den Beschaffungsprozess für die Baubranche digitalisiert und automatisiert.
Entworfen für Generalunternehmer, Immobilienentwickler, Architekten und Planer, vereinfacht Cosuno den traditionell manuellen,
papierbasierten Einkaufsprozess durch nahtlose Zusammenarbeit, automatisierte Ausschreibungen und intelligente Datenanalyse.

Durch die Vernetzung von nachfragenden Unternehmen mit einem wachsenden Netzwerk von über 80.000 Subunternehmen in ganz Europa
reduziert Cosuno die Dauer des Beschaffungsprozesses um bis zu 60 %, erhöht das Angebotsvolumen und verbessert die Entscheidungsfindung
durch Echtzeit-Einblicke. Die Plattform hilft Bauunternehmen dabei, Zeit zu sparen, Kosten zu senken und Gewinne zu maximieren – 
gleichzeitig schafft sie Transparenz und Effizienz in einer der am wenigsten digitalisierten Branchen weltweit.

Mit ihrer intuitiven Oberfläche und dem Zwei-Seiten-Marktplatzmodell vereinfacht Cosuno nicht nur den Bieter- und Ausschreibungsprozess,
sondern hilft Nutzern auch dabei, langfristige Subunternehmernetzwerke aufzubauen – für intelligentere, schnellere und kooperativere Projektumsetzung.
"""

# --- HELPER FUNCTIONS ---

def strip_html(text):
    """Remove HTML tags and decode HTML entities."""
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    clean_text = soup.get_text(separator=" ")
    clean_text = re.sub(r'\s+', ' ', clean_text).strip()
    return clean_text


def get_embeddings_for_account(account_id, days=730):  # Default: last 2 years
    """Fetch all note embeddings + raw text + created_date for an account"""
    cutoff_date = datetime.now() - timedelta(days=days)
    cutoff_str = cutoff_date.strftime("%Y-%m-%d")

    query = f"""
        SELECT id, raw_text, embedding, created_date 
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE account_id = '{account_id}'
          AND created_date >= '{cutoff_str}'
        ORDER BY created_date DESC
    """
    rows = bq_client.query(query).result()
    print(f"🧠 Loaded {rows.total_rows} notes for account {account_id}")
    return [(row.id, strip_html(row.raw_text), row.embedding, row.created_date) for row in rows]


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


def find_most_relevant_note(query_embedding, embeddings_data, decay_factor=0.95):
    """Find top 3 most similar notes based on hybrid score (semantic + recency)"""
    similarities = []
    now = datetime.now()

    for version_id, raw_text, embedding, created_date in embeddings_data:
        if len(embedding) != len(query_embedding):
            continue

        sim = cosine_similarity(query_embedding, embedding)

        # Apply time-based weighting
        try:
            note_date = datetime.strptime(created_date, "%Y-%m-%d %H:%M:%S")
            days_old = (now - note_date).days
            time_weight = max(0.1, decay_factor ** days_old)
            weighted_score = sim * time_weight
        except Exception:
            weighted_score = sim

        similarities.append((weighted_score, raw_text[:1000]))  # Limit context size

    similarities.sort(reverse=True, key=lambda x: x[0])
    return similarities[:3]  # Top 3 matches


def ask_question_with_context(question, relevant_notes):
    """Ask Gemini a question using only the most relevant note snippets"""

    context_block = "\n".join([f"{text}" for _, text in relevant_notes])

    full_prompt = f"""
{COSUNO_CONTEXT}

Du bist ein hilfreicher Assistent für Salesforce-Vertriebsmitarbeiter bei Cosuno. Deine Aufgabe ist es, basierend auf den bereitgestellten Meetingnotizen, prägnante und gut strukturierte Antworten zu liefern.

### WICHTIGE REGELN FÜR DIE ANTWORTERSTELLUNG:
- Wenn im Text ein vollständiges Datum oder eine genaue Uhrzeit erwähnt wird, gib diese immer im Format "[DD.MM.YYYY, HH:MM]" an.
- Vermeide relative Formulierungen wie "nächsten Montag" oder "Mittwoch um 9:30", wenn das vollständige Datum bekannt ist.
- Falls kein vollständiges Datum vorliegt, antworte mit "(Datum unbekannt)".
- Falls keine Note die Antwort enthält, antworte mit: "(Keine relevante Info gefunden)"
- Halte deine Antwort direkt, präzise und beruflich angemessen.

### Frage:
{question}

### Relevanteste Notizen:
{context_block}

### Antwort im gewünschten Format:
Beginne immer mit:
#### 🔍 Gemini Antwort:
Danach gib deine Antwort aus.

Beispiel:
#### 🔍 Gemini Antwort:
Die Hauptentscheidungsträger sind Stefan Meier (CFO) und Anna Schmidt (Leiterin Einkauf).
"""

    try:
        response = model.generate_content(full_prompt)
        candidate = response.candidates[0].content.parts[0].text.strip()

        # Extract just the part after "#### 🔍 Gemini Antwort:"
        match = re.search(r"#### 🔍 Gemini Antwort:\s*(.*)", candidate, re.DOTALL)
        return match.group(1).strip() if match else candidate

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
        "Welche Probleme wurden erwähnt?": "4. Pain Points",
        "Wie sieht der Ausschreibungsprozess aus?": "Current Tendering Process",
        "Wie läuft der Entscheidungsprozess ab?": "5. Decision Process",
        "Was ist das Unternehmensprofil?": "3. Background"
    }

    # Ask each question and show results
    for question, section in questions.items():
        print(f"\n{'-'*50}\n")
        print(f"📌 Section: {section}")
        print(f"❓ Frage: {question}")

        query_emb = get_query_embedding(question)
        relevant_notes = find_most_relevant_note(query_emb, embeddings_data)

        answer = ask_question_with_context(question, relevant_notes)
        print(f"🧠 Gemini Antwort: {answer}")

        # Optional: Print used notes
        # print("\n📎 Verwendete Notizen:")
        # for idx, (sim, text) in enumerate(relevant_notes):
        #     print(f"\nNote {idx+1} (Ähnlichkeit: {sim:.4f})\n{text[:200]}...")