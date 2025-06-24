from salesforce_auth import login_to_salesforce
import os
from utils import chunked, is_recent
from datetime import datetime, timedelta


def get_top_user_usage_metrics(sf, account_id):
    """
    For a given Salesforce Account ID, retrieves active contacts and ranks them
    by usage metrics (activity days and bid packages). Returns a list of top 5 users
    with their name, activity days, and bid packages.
        
    :return: List of dictionaries:
        [
            {
                'name': str,
                'activity_days': int,
                'bid_packages': int
            },
            ...
        ]
    """

    # Step 1: Query active contacts in the account with relevant usage fields
    query = f"""
    SELECT 
        Name,
        Days_with_activity_Last_90_Days__c,
        Bid_Packages_Published_L90D__c
    FROM Contact
    WHERE AccountId = '{account_id}'
      AND Active__c = TRUE
    ORDER BY 
        Bid_Packages_Published_L90D__c DESC NULLS LAST,
        Days_with_activity_Last_90_Days__c DESC NULLS LAST
    LIMIT 5
    """

    result = sf.query(query)
    records = result.get('records', [])

    # Step 2: Format results
    top_users = []
    for record in records:
        top_users.append({
            'name': record['Name'],
            'activity_days': record['Days_with_activity_Last_90_Days__c'] or 0,
            'bid_packages': record['Bid_Packages_Published_L90D__c'] or 0
        })

    return top_users

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