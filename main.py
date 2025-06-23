# main.py

import os
from salesforce_auth import login_to_salesforce
from gemini_prompter import setup_gemini, create_prompt_question_1, create_prompt_question_2, create_prompt_question_3, create_prompt_question_4, create_prompt_question_5, create_prompt_question_6
from note_extractor import extract_note_content
from doc_writer import get_google_docs_service, clear_and_update_google_doc
from utils import chunked, is_recent

DOCUMENT_ID = '1hISTyvQ_r-DVI3n3kfRQ9WGQiHcBvLMgh48M4zIuhkc'
MAX_NOTE_LENGTH = 50000    # Maximum length of notes to process

def format_tendering_volume(value):
    if value is None:
        return "Not specified"
    if isinstance(value, str) and value.lower() == 'unlimited':
        return "Unlimited"
    try:
        # Convert number to "X million"
        return f"{float(value):,.0f} million"
    except ValueError:
        return "Unknown"

def main():
    try:
        sf, session_id, instance = login_to_salesforce()
        model = setup_gemini()

        print("📡 Query Salesforce for target account...")
        query = """
            SELECT Id, Name, Acquired_Licenses__c, Active_Users__c, Number_of_User_with_75_Activity_Score__c, Acquired_Tendering_volume__c, Bid_Packages_Tot_last_365_Days__c 
            FROM Account 
            WHERE Id = '00109000013HnuHAAS'
        """
        response = sf.query(query)
        accounts = response['records']

        for acc in accounts:
            accountid = acc['Id']
            account_name = acc.get('Name', 'Unknown')
            acquired_licenses = acc.get("Acquired_Licenses__c")
            active_users = acc.get("Active_Users__c")
            users_at_75_activity = acc.get("Number_of_User_with_75_Activity_Score__c")
            acquired_tendering_volume = acc.get("Acquired_Tendering_volume__c")
            tendering_last_year = acc.get("Bid_Packages_Tot_last_365_Days__c")

            # Format tendering volume
            formatted_acquired_tend_volume = format_tendering_volume(acquired_tendering_volume)

             # Initialize full_summary here
            full_summary = ""

            # Fetch Notes
            linked_docs = sf.query(f"SELECT ContentDocumentId FROM ContentDocumentLink WHERE LinkedEntityId = '{accountid}'")['records']
            doc_ids = [d['ContentDocumentId'] for d in linked_docs]
            all_notes = []
            for doc_id in doc_ids:
                versions = sf.query(f"SELECT Id, FileType, CreatedDate FROM ContentVersion WHERE ContentDocumentId = '{doc_id}' ORDER BY CreatedDate DESC LIMIT 1")['records']
                for version in versions:
                    content, created_date = extract_note_content(version, session_id, instance)
                    if content:
                        all_notes.append({
                            'text': content,
                            'created_date': created_date,
                            'is_recent': is_recent(created_date)
                        })

            # Sort by recency
            all_notes.sort(key=lambda x: x['created_date'] or '', reverse=True)
            recent_notes = [n['text'] for n in all_notes if n['is_recent']]
            older_notes = [n['text'] for n in all_notes if not n['is_recent']]
            combined_text = "\n".join(recent_notes * 2 + older_notes)
            meeting_notes = combined_text[:MAX_NOTE_LENGTH]  

            # Build Prompt for Question 1
            prompt1 = create_prompt_question_1(
                account_name=account_name,
                acquired_licenses=acquired_licenses                
            )

            # Ask Gemini Question 1
            try:
                response1 = model.generate_content(prompt1)
                answer1 = response1.candidates[0].content.parts[0].text.strip()

                full_summary += f"""
                # Executive Summary for {account_name}

                ## 1. Licences Purchased

                ### Question: How many licences have been purchased?
                {answer1}
                \n\n"""

               
            except Exception as e:
                print(f"❌ Gemini error 1: {e}")
                candidate = "(Error generating summary)"
            
             # Build Prompt for Question 2
            prompt2 = create_prompt_question_2(
                account_name=account_name,
                acquired_licenses=acquired_licenses,
                active_users = active_users                       
            )
            
            # Ask Gemini Question 2
            try:
                response2 = model.generate_content(prompt2)
                answer2 = response2.candidates[0].content.parts[0].text.strip()
                
            except Exception as e:
                print(f"❌ Gemini error 2: {e}")
                candidate = "(Error generating summary)"

            
            # Combine Results
            full_summary += f"""
            ## 2. Usage vs. Purchase Comparison

            ### Question: How does the usage of these licences compare? (Active Users vs Total Licences)
            {answer2}
            \n\n"""



            # Build Prompt for Question 3
            prompt3 = create_prompt_question_3(
                account_name=account_name,
                acquired_licenses=acquired_licenses,
                active_users = active_users,
                users_at_75_activity = users_at_75_activity                     
            )

             # Ask Gemini Question 3
            try:
                response3 = model.generate_content(prompt3)
                answer3 = response3.candidates[0].content.parts[0].text.strip()
                
            except Exception as e:
                print(f"❌ Gemini error 3: {e}")
                candidate = "(Error generating summary)"

            
            # Combine Results
            full_summary += f"""
            ## 3. 75% Usage Threshold

            ### Question: Are all licences used at a 75% activity rate or higher?
            {answer3}
            \n\n"""


            # Build Prompt for Question 4
            prompt4 = create_prompt_question_4(
                account_name=account_name,
                acquired_licenses=acquired_licenses,
                active_users = active_users,
                users_at_75_activity = users_at_75_activity,
                meeting_notes = meeting_notes                   
            )

             # Ask Gemini Question 4
            try:
                response4 = model.generate_content(prompt4)
                answer4 = response4.candidates[0].content.parts[0].text.strip()
                
            except Exception as e:
                print(f"❌ Gemini error 4: {e}")
                candidate = "(Error generating summary)"

            
            # Combine Results
            full_summary += f"""
            ## 4. Reasons for Below Expectations Usage

            ### Question: If the usage is below expectations, has the reason been explored or discussed?
            {answer4}
            \n\n"""



          # Build Prompt for Question 5
            prompt5 = create_prompt_question_5(
                account_name=account_name,
                acquired_tendering_volume = acquired_tendering_volume,
                tendering_last_year = tendering_last_year,
                meeting_notes = meeting_notes                  
            )

             # Ask Gemini Question 5
            try:
                response5 = model.generate_content(prompt5)
                answer5 = response5.candidates[0].content.parts[0].text.strip()
                
            except Exception as e:
                print(f"❌ Gemini error 5: {e}")
                candidate = "(Error generating summary)"

            
            # Combine Results
            full_summary += f"""
            ## 5. Acquired Tendering Volume vs Used Tendering Volume

            ### Question: What is the tendering volume that has been purchased by the customer and how does the tendering volume in the last 365 days compare to this purchase volume? 
            If the volume is below expectations has the reason been explored / discussed?
            \n\n
            {answer5}
            \n\n"""


            # Build Prompt for Question 6
            prompt6 = create_prompt_question_6(
                account_name=account_name,                
                meeting_notes = meeting_notes                  
            )

             # Ask Gemini Question 5
            try:
                response6 = model.generate_content(prompt6)
                answer6 = response6.candidates[0].content.parts[0].text.strip()
                
            except Exception as e:
                print(f"❌ Gemini error 6: {e}")
                candidate = "(Error generating summary)"

            
            # Combine Results
            full_summary += f"""
            ## 6. Customer Sentiment

            ### Question: Is the customer positive or negative based on the sentiment of the last meeting?
            \n\n
            {answer6}
            \n\n"""


            ###################################################################
            # Save to Google Doc
            docs_service = get_google_docs_service()
            clear_and_update_google_doc(docs_service, DOCUMENT_ID, full_summary)

    except Exception as e:
        print(f"❌ Critical error in main: {e}")

if __name__ == "__main__":
    main()