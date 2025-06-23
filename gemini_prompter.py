# gemini_prompter.py

import google.generativeai as genai
import os
import json

# --- Load Gemini API Key ---
def setup_gemini():
    #print("🔐 Loading Gemini API key...")
    with open(r"E:\Software\gemini_prod_key.txt", "r") as file:
        gemini_api_key = file.read().strip()
    genai.configure(api_key=gemini_api_key)
    model = genai.GenerativeModel("gemini-2.0-flash")
    print("✅ Gemini model initialized..")
    return model


# --- Prompt Template Loader ---
def load_prompt_template(filename):
    """
    Loads a prompt template from the 'prompts' folder.
    """
    prompt_dir = os.path.dirname(__file__)
    path = os.path.join(prompt_dir, "prompts", filename)
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"Prompt template '{filename}' not found at {path}")
        
    with open(path, "r", encoding="utf-8") as f:
        template = f.read()
    return template


# --- Prompt Builders ---
def create_prompt_question_1(account_name, acquired_licenses):
    """
    Builds the prompt for Question 1: Licence Count
    """
    template = load_prompt_template("q1_licence_count.txt")

    filled_prompt = template.format(
        account_name=account_name,
        acquired_licenses=acquired_licenses        
    )
    return filled_prompt

def create_prompt_question_2(account_name, acquired_licenses,active_users):
    """
    Builds the prompt for Question 2: Usage vs Purchase Comparison
    """
    template = load_prompt_template("q2_usage_vs_purchased.txt")

    filled_prompt2 = template.format(
        account_name=account_name,
        acquired_licenses=acquired_licenses ,
        active_users = active_users       
    )
    return filled_prompt2

def create_prompt_question_3(account_name, acquired_licenses,active_users, users_at_75_activity):
    """
    Builds the prompt for Question 3: 75% Usage Threshold
    """
    template = load_prompt_template("q3_75percent_usage.txt")

    filled_prompt3 = template.format(
        account_name=account_name,
        acquired_licenses=acquired_licenses ,
        active_users = active_users,
        users_at_75_activity = users_at_75_activity       
    )
    return filled_prompt3

def create_prompt_question_4(account_name, acquired_licenses,active_users, users_at_75_activity, meeting_notes):
    """
    Builds the prompt for Question 4: Below expectations usage reasons
    """
    template = load_prompt_template("q4_below expectations_reasons.txt")

    filled_prompt4 = template.format(
        account_name=account_name,
        acquired_licenses=acquired_licenses ,
        active_users = active_users,
        users_at_75_activity = users_at_75_activity,
        meeting_notes = meeting_notes       
    )
    return filled_prompt4

def create_prompt_question_5(account_name, acquired_tendering_volume, tendering_last_year, meeting_notes):
    """
    Builds the prompt for Question 5: Acquired Tendering Volume
    """
    template = load_prompt_template("q5_purchased_tendering_volume.txt")

    filled_prompt5 = template.format(
        account_name=account_name,
        acquired_tendering_volume=acquired_tendering_volume,
        tendering_last_year=tendering_last_year,
        meeting_notes=meeting_notes
    )
    return filled_prompt5

def create_prompt_question_6(account_name, meeting_notes):
    """
    Builds the prompt for Question 6: Customer Sentiment
    """
    template = load_prompt_template("q6_customer_sentiment.txt")

    filled_prompt6 = template.format(
        account_name=account_name,       
        meeting_notes=meeting_notes
    )
    return filled_prompt6