# main.py

import os
from salesforce_auth import login_to_salesforce
from gemini_prompter import (
    setup_gemini, create_prompt_question_1, create_prompt_question_2, create_prompt_question_3,
    create_prompt_question_4, create_prompt_question_5, create_prompt_question_6, create_prompt_question_7, create_prompt_question_8,
    create_prompt_question_9, create_prompt_question_10, create_prompt_question_11, create_prompt_question_12
)

from note_extractor import extract_note_content
from doc_writer import get_google_docs_service, clear_and_update_google_doc
from utils import chunked, is_recent
from datetime import datetime, timedelta

DOCUMENT_ID = '1hISTyvQ_r-DVI3n3kfRQ9WGQiHcBvLMgh48M4zIuhkc'
MAX_NOTE_LENGTH = 50000    # Maximum length of notes to process


def get_potential_users(sf, account_id):
    """
    Calculates metrics for Salesforce account:
    - Active users
    - Potential inactive users (with job titles common across org)
    - Total addressable users (active + potential inactive)

    # Step 1: Get list of active contact job titles across the entire org
    """

    active_titles_query = """
    SELECT Job_Title__c 
    FROM Contact 
    WHERE Active__c = TRUE 
      AND Job_Title__c != NULL 
    GROUP BY Job_Title__c
    """

    active_titles_result = sf.query(active_titles_query)
    active_titles = [record['Job_Title__c'] for record in active_titles_result.get('records', [])]

    if not active_titles:
        return {
            'active_users': 0,
            'potential_inactive_users': 0,
            'total_addressable': 0
        }

    # Step 2: Count active users in the target account
    active_users_query = f"""
    SELECT Id 
    FROM Contact 
    WHERE AccountId = '{account_id}' 
      AND Active__c = TRUE
    """

    active_users_result = sf.query(active_users_query)
    active_users_count = len(active_users_result.get('records', []))

    # Step 3: Find inactive users in the target account with matching job titles
    formatted_titles = "', '".join(active_titles)
    inactive_users_query = f"""
    SELECT Id 
    FROM Contact 
    WHERE AccountId = '{account_id}' 
      AND Active__c = FALSE 
      AND Job_Title__c IN ('{formatted_titles}')
    """

    inactive_users_result = sf.query(inactive_users_query)
    potential_inactive_count = len(inactive_users_result.get('records', []))

    # Step 4: Calculate total addressable users
    total_addressable = active_users_count + potential_inactive_count
    #print(f"Active Users: {active_users_count}, Potential Inactive Users: {potential_inactive_count}, Total Addressable: {total_addressable}")
    return total_addressable


def format_tendering_volume(value):
    if value is None:
        return "Not specified"
    
    if isinstance(value, str) and value.lower() == 'unlimitiert':
        return "Unlimitiert"
    
    try:
        # Convert number to: "X million €"
        formatted_value = f"€{float(value):,.0f} million"
        return formatted_value
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
            potential_users_count = get_potential_users(sf, accountid)

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
                        try:
                            date_clean = created_date.split("T")[0]
                            parsed_date = datetime.strptime(date_clean, "%Y-%m-%d")
                        except Exception:
                            parsed_date = datetime.min  # Push invalids to the end
                        all_notes.append({
                            'text': content,
                            'created_date': created_date,
                            'parsed_date': parsed_date,
                            'is_recent': is_recent(created_date)
                        })

            # Now sort by parsed datetime
            all_notes.sort(key=lambda x: x['parsed_date'], reverse=True)
            recent_notes = [n for n in all_notes if n['is_recent']]
            older_notes = [n['text'] for n in all_notes if not n['is_recent']]
            combined_text = "\n".join([note['text'] for note in recent_notes] * 2 + older_notes)
            meeting_notes = combined_text[:MAX_NOTE_LENGTH] 
            latest_note = recent_notes[0] if recent_notes else None
            latest_meeting_date = latest_note['created_date'] if latest_note else "Unknown" 

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
                \n
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
            \n
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
            \n
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
            \n
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
            \n
            {answer5}
            \n\n"""


             # Build Prompt for Question 6
            prompt6 = create_prompt_question_6(
                account_name=account_name,                
                acquired_tendering_volume=formatted_acquired_tend_volume,
                meeting_notes=meeting_notes                  
            )

             # Ask Gemini Question 6   
            try:
                print("\n📌 Sending to Gemini (Question 6):")                
                response6 = model.generate_content(prompt6)
                answer6 = response6.candidates[0].content.parts[0].text.strip()
            except Exception as e:
                print(f"❌ Gemini error 6: {e}")
                candidate = "(Error generating summary)"
                
            # Combine Results
            full_summary += f"""
            ## 6. Actual vs Potential Tendering Volume
            ### Question:  How does the tendering volume compare to the possible tendering volume outlined by the customer?

            \n   
            {answer6}
            \n\n"""
            

             # Build Prompt for Question 7
            prompt7 = create_prompt_question_7(
                account_name=account_name,                
                active_users=active_users,
                potential_users_count=potential_users_count               
            )

             # Ask Gemini Question 7
            try:
                print("\n📌 Sending to Gemini (Question 7):")                
                response7 = model.generate_content(prompt7)
                answer7 = response7.candidates[0].content.parts[0].text.strip()
                
            except Exception as e:
                print(f"❌ Gemini error 7: {e}")
                candidate = "(Error generating summary)"

            
            # Combine Results
            full_summary += f"""
            ## 7. Active Users vs Potential Users

            ### Question: How do the number of used licences compare with possible users (Summe von Einkauf, Kalkulation, Arbeitsvorbereitung)?
            \n
            {answer7}
            \n\n"""


            #Build Prompt for Question 8
            prompt8 = create_prompt_question_8(
                account_name=account_name,                
                meeting_notes = meeting_notes,
                latest_meeting_date=latest_meeting_date                  
            )

             # Ask Gemini Question 8
            try:
                print("\n📌 Sending to Gemini (Question 8):")
                print(f"Latest Meeting Date: {latest_meeting_date}")
                #print(f"Meeting Notes (first 500 chars):\n{meeting_notes[:500]}...")
                response8 = model.generate_content(prompt8)
                answer8 = response8.candidates[0].content.parts[0].text.strip()
                
            except Exception as e:
                print(f"❌ Gemini error 8: {e}")
                candidate = "(Error generating summary)"

            
            # Combine Results
            full_summary += f"""
            ## 8. Customer Sentiment

            ### Question: Is the customer positive or negative based on the sentiment of the last meeting?
            \n
            {answer8}
            \n\n"""


            # Build Prompt for Question 9
            prompt9 = create_prompt_question_9(
                account_name=account_name,                
                meeting_notes = meeting_notes,
                latest_meeting_date=latest_meeting_date                  
            )

             # Ask Gemini Question 9            
            try:
                print("\n📌 Sending to Gemini (Question 9):")
                response9 = model.generate_content(prompt9)
                answer9 = response9.candidates[0].content.parts[0].text.strip()
            except Exception as e:
                print(f"❌ Gemini error 9: {e}")
                candidate = "(Error generating summary)"        

            # Combine Results
            full_summary += f""" 
            # Current Next Steps:   
            ## 9. Next Meeting Date 
            ### Question: What is the date & time of the next meeting with the customer?            
            Next Meeting Date: {answer9}
            \n\n"""


            # Build Prompt for Question 10
            prompt10 = create_prompt_question_10(
                account_name=account_name,                
                meeting_notes = meeting_notes,
                latest_meeting_date=latest_meeting_date                  
            )

            # Ask Gemini Question 10       
            try:
                print("\n📌 Sending to Gemini (Question 10):")
                response10 = model.generate_content(prompt10)
                answer10 = response10.candidates[0].content.parts[0].text.strip()
            except Exception as e:
                print(f"❌ Gemini error 10: {e}")
                candidate = "(Error generating summary)"
            # Combine Results
            full_summary += f"""    
            ## 10. Next Steps
            ### Question: What are the next steps that were agreed upon in the last meeting?
            \n
            {answer10}
            \n\n"""


            # Build Prompt for Question 11
            prompt11 = create_prompt_question_11(
                account_name=account_name,                
                meeting_notes = meeting_notes,
                latest_meeting_date=latest_meeting_date                  
            )

            # Ask Gemini Question 11      
            try:
                print("\n📌 Sending to Gemini (Question 11):")
                response11 = model.generate_content(prompt11)
                answer11 = response11.candidates[0].content.parts[0].text.strip()
            except Exception as e:
                print(f"❌ Gemini error 11: {e}")
                candidate = "(Error generating summary)"
            # Combine Results
            full_summary += f"""    
            ## 11. CSM Next Steps
            ### Question: What are the next steps defined by the customer success manager?
            \n
            {answer11}
            \n\n"""


            # Build Prompt for Question 12
            prompt12 = create_prompt_question_12(
                account_name=account_name,                
                meeting_notes = meeting_notes,
                latest_meeting_date=latest_meeting_date                  
            )

            # Ask Gemini Question 12      
            try:
                print("\n📌 Sending to Gemini (Question 12):")
                response12 = model.generate_content(prompt12)
                answer12 = response11.candidates[0].content.parts[0].text.strip()
            except Exception as e:
                print(f"❌ Gemini error 12: {e}")
                candidate = "(Error generating summary)"
            # Combine Results
            full_summary += f"""    
            ## 12. Open Tasks
            ### Question: Are there open tasks?
            \n
            {answer12}
            \n\n"""



            ###################################################################
            # Save to Google Doc
            docs_service = get_google_docs_service()
            clear_and_update_google_doc(docs_service, DOCUMENT_ID, full_summary)

    except Exception as e:
        print(f"❌ Critical error in main: {e}")


if __name__ == "__main__":
    main()