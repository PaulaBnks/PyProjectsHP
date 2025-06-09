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
print(f"Successfully connected to Salesforce: {sf.base_url}")

# Query accounts with potentially non-empty AI_Summary__c (limited fields due to long text)
print("Querying accounts...")
account_ids_to_update = []
query = "SELECT Id, AI_Summary__c FROM Account WHERE Account_Status__c = 'Prospect' AND CompanyType__c != 'Subcontractor'"
result = sf.query_all(query)

# Filter records in Python
for record in result['records']:
    if record.get('AI_Summary__c'):  # non-empty string
        account_ids_to_update.append({'Id': record['Id'], 'AI_Summary__c': ''})

print(f"Found {len(account_ids_to_update)} accounts to clear AI_Summary__c.")

batch_size = 200
updated_count = 0
failed_updates = []

print(f"Starting batch updates with a batch size of {batch_size}...")

for i in range(0, len(account_ids_to_update), batch_size):
    batch = account_ids_to_update[i:i + batch_size]
    try:
        job_result = sf.bulk.Account.update(batch)

        for result_item in job_result:
            if not result_item['success']:
                failed_updates.append(result_item)
            else:
                updated_count += 1

        print(f"Processed batch {i // batch_size + 1}/{(len(account_ids_to_update) + batch_size - 1) // batch_size}. Updated so far: {updated_count}")

    except Exception as e:
        print(f"An error occurred during batch update: {e}")
        for record in batch:
            failed_updates.append({'Id': record['Id'], 'error': str(e)})

# Summary
print("\nUpdate process complete.")
print(f"Total accounts processed: {len(account_ids_to_update)}")
print(f"Successfully updated: {updated_count}")
print(f"Failed to update: {len(failed_updates)}")

if failed_updates:
    print("\nDetails of failed updates:")
    for failure in failed_updates:
        print(f"  Account ID: {failure.get('Id')}, Error: {failure.get('errors', failure.get('error', 'Unknown error'))}")
