# config.py

RUN_ON_GIT_ACTIONS = 'No'  # Change to 'Yes' when running in CI/CD
MAX_NOTE_LENGTH = 50000     # Maximum length of notes to process

# Google Docs Integration
SERVICE_ACCOUNT_FILE = r'E:\Software\quotefiles-f047a4ff4bdd.json'
SCOPES = ['https://www.googleapis.com/auth/documents'] 
DOCUMENT_ID = '1hISTyvQ_r-DVI3n3kfRQ9WGQiHcBvLMgh48M4zIuhkc'

# Prompt directory
PROMPT_DIR = r"F:\salesforce\Cosuno AI\prompts"