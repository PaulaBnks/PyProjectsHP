import os
import re
import google.generativeai as genai
import uuid

# Define directories
english_prompt_dir = 'prompts'
german_prompt_dir = 'prompts german'

# Create the German prompt directory if it doesn't exist
os.makedirs(german_prompt_dir, exist_ok=True)

def setup_gemini():
    #print("🔐 Loading Gemini API key...")
    with open(r"E:\Software\gemini_prod_key.txt", "r") as file:
        gemini_api_key = file.read().strip()
    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel("gemini-2.0-flash")
    print("✅ Gemini model initialized..")
    return model

# Initialize the Gemini model
gemini_model = setup_gemini()

# Function to translate text using Gemini while preserving placeholders
def translate_to_german_preserve_vars(model, text):
    # Find all placeholders like {variable_name}
    placeholder_pattern = re.compile(r'\{.*?\}')
    placeholders = placeholder_pattern.findall(text)

    # Replace placeholders with unique temporary keys
    temp_map = {}
    temp_text = text
    for placeholder in placeholders:
        temp_key = str(uuid.uuid4())
        temp_map[temp_key] = placeholder
        temp_text = temp_text.replace(placeholder, temp_key, 1)

    # Now translate the cleaned text
    prompt = f"""
Please translate the following text *literally* into German. 
Do NOT add any explanation, response, or example. 
Keep all placeholders like {{variable_name}} unchanged.

Text to translate:
{temp_text}
"""

    response = model.generate_content(prompt.strip())
    translated_text = response.text

    # Restore placeholders
    for temp_key, original_placeholder in temp_map.items():
        translated_text = translated_text.replace(temp_key, original_placeholder)

    return translated_text.strip()

# Loop through all .txt files in the prompts directory
for filename in os.listdir(english_prompt_dir):
    if filename.endswith('.txt'):
        file_path = os.path.join(english_prompt_dir, filename)
        
        # Read English prompt
        with open(file_path, 'r', encoding='utf-8') as file:
            english_text = file.read()
        
        # Translate to German while preserving variable names
        print(f"🔄 Translating {filename}...")
        german_text = translate_to_german_preserve_vars(gemini_model, english_text)
        
        # Save German version with the same filename
        german_file_path = os.path.join(german_prompt_dir, filename)
        with open(german_file_path, 'w', encoding='utf-8') as file:
            file.write(german_text)

print("🎉 All translations completed and saved in 'prompts german' folder.")