from salesforce_auth import login_to_salesforce
import os
from utils import chunked, is_recent
from datetime import datetime, timedelta

def get_contract_data(account_id):
    """
    Fetches ServiceContract and related ContractLineItem data for a given Account ID.
    
    Returns:
        dict: Dictionary containing structured contract and account data.
    """

    # Initialize result dictionary with default values
    result = {
        "account_name": "",
        "number_of_employees": None,
        "contract": {
            "start_date": None,
            "end_date": None,
            "last_possible_termination_date": None,
            "packages_purchased": [],
            "user_limit": 0,
            "tendering_volume_limit": 0,
            "project_limit": 0,
            "arr_before_discount_net": 0,
            "current_discount_rate": 0,
            "arr_after_discount_net": 0
        }
    }

    # Query Account Info
    account_query = f"SELECT Name FROM Account WHERE Id = '{account_id}'"
    account_result = sf.query(account_query)

    if account_result['totalSize'] == 0:
        raise ValueError(f"No Account found with ID {account_id}")

    account = account_result['records'][0]
    result["account_name"] = account['Name']

    # Query ServiceContract and ContractLineItems
    contract_query = f"""
        SELECT Id, StartDate, EndDate, Last_possible_termination_date__c, 
               ARR_before_discount_net__c, Discount_Rate__c, ARR_after_discount_net__c,
               (SELECT Product_or_Package__c, Number_of_user_licenses_new__c, 
                       Tendering_Volume_new__c, Max_number_of_projects_new__c
                FROM ContractLineItems)
        FROM ServiceContract
        WHERE AccountId = '{account_id}' and Status = 'Active' order by Arr_after_discount_net__c DESC LiMIT 1
    """

    contract_result = sf.query(contract_query)

    if contract_result['totalSize'] > 0:
        service_contract = contract_result['records'][0]

        # Map contract-level fields
        result["contract"]["start_date"] = service_contract.get("StartDate")
        result["contract"]["end_date"] = service_contract.get("EndDate")
        result["contract"]["last_possible_termination_date"] = service_contract.get("Last_possible_termination_date__c")
        result["contract"]["arr_before_discount_net"] = service_contract.get("ARR_before_discount_net__c", 0)
        result["contract"]["current_discount_rate"] = service_contract.get("Discount_Rate__c", 0)
        result["contract"]["arr_after_discount_net"] = service_contract.get("ARR_after_discount_net__c", 0)

        # Process Contract Line Items
        line_items = service_contract.get("ContractLineItems", {}).get("records", [])
        packages = []
        user_limit = 0
        tendering_volume = 0
        project_limit = 0

        for item in line_items:
            product_name = item.get("Product_or_Package__c", "")
            if product_name and "paket" in product_name.lower():
                packages.append(product_name)

            user_limit += item.get("Number_of_user_licenses_new__c", 0) or 0
            tendering_volume += item.get("Tendering_Volume_new__c", 0) or 0
            project_limit += item.get("Max_number_of_projects_new__c", 0) or 0

        result["contract"]["packages_purchased"] = list(set(packages)) or ["None"]
        result["contract"]["user_limit"] = user_limit
        result["contract"]["tendering_volume_limit"] = tendering_volume
        result["contract"]["project_limit"] = project_limit

    else:
        # No ServiceContract found for this account
        result["contract"]["packages_purchased"] = ["None"]

    return result



def get_share_of_wallet_data(sf, account_id):
    """
    Fetches Share_of_Wallet__c data for a given Account ID.
    Returns structured data for use with Gemini prompts.
    """

    # Query Share_of_Wallet__c records related to the Account
    query = f"""
    SELECT 
        Id,
        Cosuno_estimate_projects_2025__c,
        Cosuno_estimate_tendering_volume_2025__c,
        CS_estimate_Share_of_Wallet__c,
        Share_of_Wallet_consol_projects_2025__c,
        Actuals_projects_2025__c
    FROM Share_of_Wallet__c
    WHERE Account__c = '{account_id}'
    LIMIT 1
    """

    result = sf.query(query)
    records = result.get('records', [])

    if not records:
        return {
            'error': 'No Share_of_Wallet__c record found for this account'
        }

    record = records[0]

    # Format values safely
    estimate_projects = record.get('Cosuno_estimate_projects_2025__c')
    estimate_tendering_volume = record.get('Cosuno_estimate_tendering_volume_2025__c')  
    actual_projects = record.get('Actuals_projects_2025__c')

    # Determine whether actuals match estimates
    actuals_provided = actual_projects is not None
    

    return {
        'estimate_projects': int(estimate_projects) if estimate_projects is not None else None,
        'estimate_tendering_volume': float(estimate_tendering_volume) if estimate_tendering_volume is not None else None,
        'actuals_provided': actuals_provided,
    }


def get_account_organization_insights(sf, account_id):
    """
    Returns organization insights about a Salesforce Account for use with Gemini prompts.
    """

    def count_contacts_by_department(possible_departments, head_keywords):
        """
        Inner helper function scoped to this method.
        """
        contact_query = f"""
        SELECT Id, Department, Job_Title__c 
        FROM Contact 
        WHERE AccountId = '{account_id}' AND Active__c = TRUE
        """
        contact_result = sf.query(contact_query)
        contacts = contact_result.get('records', [])

        department_contacts = [
            c for c in contacts
            if c['Department'] and any(dep.lower() in c['Department'].lower() for dep in possible_departments)
        ]

        count = len(department_contacts)

        has_head = False
        if head_keywords and department_contacts:
            for keyword in head_keywords:
                if any(
                    c['Job_Title__c'] and keyword.lower() in c['Job_Title__c'].lower()
                    for c in department_contacts
                ):
                    has_head = True
                    break

        return {
            'exists': count > 0,
            'count': count,
            'has_head': has_head
        }

    # Get Account info including NumberOfEmployees
    account_query = f"SELECT Id, Name, NumberOfEmployees FROM Account WHERE Id = '{account_id}'"
    account_result = sf.query(account_query)
    if not account_result['records']:
        return {"error": "Account not found"}
    
    account = account_result['records'][0]

    # Get all active contacts in the account
    contact_query = f"""
    SELECT Id, Department, Job_Title__c 
    FROM Contact 
    WHERE AccountId = '{account_id}' AND Active__c = TRUE
    """
    contact_result = sf.query(contact_query)
    contacts = contact_result.get('records', [])

    # Einkauf / Purchasing
    einkauf = count_contacts_by_department(["Einkauf", "Purchasing"], ["Leiter Einkauf", "Head of Procurement", "Einkaufsleiter", "Einkäufer"])

    # Kalkulation / Calculation
    kalkulation = count_contacts_by_department(["Kalkulation", "Calculation"], ["Leiter Kalkulation", "Head of Calculation", "Calculation Manager"])

    # Arbeitsvorbereitung
    arbeitsvorbereitung = count_contacts_by_department(["Arbeitsvorbereitung", "Preparation"], ["Leiter Arbeitsvorbereitung", "Head of Preparation", "AV Manager"])

    # Departments to check for Cosuno relevance
    cosuno_departments = ["Bauleitung", "Projektmanagement", "Construction Management", "Digitalisierungsabteilung"]

    relevant_cosuno_departments = []
    for dept in cosuno_departments:
        dept_contacts = [c for c in contacts if c['Department'] == dept]
        if dept_contacts:
            relevant_cosuno_departments.append({
                'department': dept,
                'count': len(dept_contacts)
            })

    # Optional: collect all other departments not already checked
    all_departments = set(c['Department'] for c in contacts if c['Department'])
    other_departments = [
        d for d in all_departments
        if d and d not in ['Einkauf', 'Kalkulation', 'Arbeitsvorbereitung'] +
           [item['department'] for item in relevant_cosuno_departments]
    ]

    return {
        'account_name': account['Name'],
        'number_of_employees': account['NumberOfEmployees'] or 0,

        'einkauf_exists': einkauf['exists'],
        'einkauf_count': einkauf['count'],
        'einkauf_has_head': einkauf['has_head'],

        'kalkulation_exists': kalkulation['exists'],
        'kalkulation_count': kalkulation['count'],
        'kalkulation_has_head': kalkulation['has_head'],

        'arbeitsvorbereitung_exists': arbeitsvorbereitung['exists'],
        'arbeitsvorbereitung_count': arbeitsvorbereitung['count'],
        'arbeitsvorbereitung_has_head': arbeitsvorbereitung['has_head'],

        'cosuno_relevant_departments': relevant_cosuno_departments,
        'other_departments': list(other_departments)
    }
    
    # Einkauf / Purchasing
    einkauf = count_contacts_by_department("Einkauf", ["Leiter Einkauf", "Head of Purchasing", "Purchasing Manager"])

    # Kalkulation / Calculation
    kalkulation = count_contacts_by_department("Kalkulation", ["Leiter Kalkulation", "Head of Calculation", "Calculation Manager"])

    # Arbeitsvorbereitung
    arbeitsvorbereitung = count_contacts_by_department("Arbeitsvorbereitung", ["Leiter Arbeitsvorbereitung", "Head of Preparation", "AV Manager"])

    # Departments to check for Cosuno relevance
    cosuno_departments = ["Bauleitung", "Projektmanagement", "Construction Management", "Digitalisierungsabteilung"]

    relevant_cosuno_departments = []
    for dept in cosuno_departments:
        dept_contacts = [c for c in contacts if c['Department'] == dept]
        if dept_contacts:
            relevant_cosuno_departments.append({
                'department': dept,
                'count': len(dept_contacts)
            })

    # Optional: collect all other departments not already checked
    all_departments = set(c['Department'] for c in contacts if c['Department'])
    other_departments = [
        d for d in all_departments
        if d and d not in ['Einkauf', 'Kalkulation', 'Arbeitsvorbereitung'] +
           [item['department'] for item in relevant_cosuno_departments]
    ]

    return {
        'account_name': account['Name'],
        'number_of_employees': account['NumberOfEmployees'] or 0,

        'einkauf_exists': einkauf['exists'],
        'einkauf_count': einkauf['count'],
        'einkauf_has_head': einkauf['has_head'],

        'kalkulation_exists': kalkulation['exists'],
        'kalkulation_count': kalkulation['count'],
        'kalkulation_has_head': kalkulation['has_head'],

        'arbeitsvorbereitung_exists': arbeitsvorbereitung['exists'],
        'arbeitsvorbereitung_count': arbeitsvorbereitung['count'],
        'arbeitsvorbereitung_has_head': arbeitsvorbereitung['has_head'],

        'cosuno_relevant_departments': relevant_cosuno_departments,
        'other_departments': list(other_departments)
    }



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