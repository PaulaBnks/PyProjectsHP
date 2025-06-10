import re

def preprocess_note_content(text):
    """
    Removes unhelpful lines like Avoma URLs and structures the text.
    """
    lines = text.splitlines()
    cleaned_lines = []

    exclude_patterns = [
        r"^Avoma Meeting\s*$",
        r"https?://",
        r"^You need to enable JavaScript to run this app.$"
    ]

    for line in lines:
        line_clean = line.strip()
        if not line_clean:
            continue
        excluded = any(re.search(pattern, line_clean) for pattern in exclude_patterns)
        if not excluded:
            cleaned_lines.append(line)

    return "\n".join(cleaned_lines)

# Sample note content
sample_note = """Avoma Meeting (3mins): - https://app.avoma.com/meetings/e02b124c-15c9-4446-a2ab-8b0a3abcb446/notes   

Participants
Cosuno: Lisian Kastrati
Josef-Hebel: Graf Stefan
Key Takeaways
Es wurde besprochen, dass fünf Nutzer hinzugefügt werden sollen.
Die Teilnehmer warten auf eine Rückmeldung, um Preise und Pakete zu besprechen und das passende Angebot auszuwählen.
Ein Treffen wurde für Montag um 15 Uhr vereinbart.
Herr Kuhnert wird ebenfalls zu dem Treffen eingeladen.
Eine Stunde wurde für das Treffen reserviert, um genügend Zeit für die Besprechung zu haben.
Nutzeranfragen
Es wurde nachgefragt, wie viele Nutzer hinzugefügt werden sollen.
Es wurde festgelegt, dass fünf Nutzer hinzugefügt werden sollen.
Paketoptionen
Die Diskussion über passende Pakete und Preise wurde angesprochen.
Terminplanung
Ein Termin um 14 Uhr wurde vereinbart.
Der Termin ist für Montag, den 17., um 15 Uhr festgelegt.
Einladungen
Einladungen werden an Josef Hebel und Herrn Kuhnert geschickt.
Herr Kuhnert soll ebenfalls eingeladen werden.
Follow-up Meeting
Samstag, 17. Februar 2025, 15:00 Uhr +01:00"""

# Run preprocessing
cleaned_note = preprocess_note_content(sample_note)

# Print original and cleaned versions
# print("Original Note:\n" + "-"*40)
# print(sample_note)
print("\n\nCleaned Note:\n" + "-"*40)
print(cleaned_note)