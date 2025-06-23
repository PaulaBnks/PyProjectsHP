import json
from simple_salesforce import Salesforce, SalesforceLogin

# Load login credentials
loginInfo = json.load(open(r"E:\Software\loginpd.json"))
username = loginInfo['username']
password = loginInfo['password']
security_token = loginInfo['security_token']
domain = 'login'

# Log in to Salesforce
session_id, instance = SalesforceLogin(
    username=username,
    password=password,
    security_token=security_token,
    domain=domain
)
sf = Salesforce(instance=instance, session_id=session_id)

# Step 1: Get accounts with Account_Status__c = Customer or Subsidiary
accounts_query = """
    SELECT Id, Name 
    FROM Account 
    WHERE Account_Status__c IN ('Customer', 'Subsidiary')
"""
accounts = sf.query_all(accounts_query)['records']

# Step 2: Process each account and update
print("Updating Accounts with Number_of_User_with_75_Activity_Score__c ...")
for acc in accounts:
    acc_id = acc['Id']
    acc_name = acc['Name']

    contacts_query = f"""
        SELECT Id, Name, Days_with_activity_Last_90_Days__c, Expected_Number_of_Days_with_Activity__c 
        FROM Contact 
        WHERE AccountId = '{acc_id}'
        AND Active__c = TRUE
        AND User_Type__c != 'Guest'
    """
    contacts = sf.query_all(contacts_query)['records']

    count_qualified = 0
    for con in contacts:
        days_active = con.get('Days_with_activity_Last_90_Days__c') or 0
        expected_days = con.get('Expected_Number_of_Days_with_Activity__c') or 0

        if expected_days == 0:
            continue  # Avoid division by zero

        ratio = days_active / expected_days
        if ratio > 0.75:
            count_qualified += 1

    # Update the account field
    sf.Account.update(acc_id, {
        'Number_of_User_with_75_Activity_Score__c': count_qualified
    })

    print(f"{acc_name} ({acc_id}) updated with count: {count_qualified}")

print("✅ All qualifying accounts have been updated.")
