import json
from simple_salesforce import Salesforce, SalesforceLogin

# ------------------ Step 1: Salesforce Login ------------------
loginInfo = json.load(open('E:\\Software\\loginpd.json'))
username = loginInfo['username']
password = loginInfo['password']
security_token = loginInfo['security_token']
domain = 'login'

session_id, instance = SalesforceLogin(
    username=username,
    password=password,
    security_token=security_token,
    domain=domain
)
sf = Salesforce(instance=instance, session_id=session_id)
print("Logged in to Salesforce")

# ------------------ Step 2: Query all accounts ------------------
query = """
SELECT Id, AI_Summary__c
FROM Account
LIMIT 5000
"""
records = sf.query_all(query)['records']
print(f"Total accounts retrieved: {len(records)}")

# ------------------ Step 3: Filter and prepare updates ------------------
to_update = []

for rec in records:
    summary = rec.get('AI_Summary__c')
    if summary and 'No items found' in summary:
        to_update.append({
            'Id': rec['Id'],
            'AI_Summary__c': None,
            'Temp__c': False
        })

print(f"Accounts to update: {len(to_update)}")

# ------------------ Step 4: Bulk update ------------------
BATCH_SIZE = 200
for i in range(0, len(to_update), BATCH_SIZE):
    chunk = to_update[i:i + BATCH_SIZE]
    sf.bulk.Account.update(chunk)
    print(f"Updated batch {i // BATCH_SIZE + 1}: {len(chunk)} records")

print("Done.")
