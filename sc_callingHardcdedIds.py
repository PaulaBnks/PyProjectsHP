from simple_salesforce import Salesforce, SalesforceLogin
import pandas as pd
from google.cloud import bigquery
import json
import codecs
from clrprint import clrprint
import pygsheets
import numpy as np
from google.oauth2 import service_account

# Google Sheets authorization
#gc = pygsheets.authorize(service_file='E:\Software\python-project-403413-480829366e0a.json')
gc = pygsheets.authorize(service_file=r'E:\Software\python-project-403413-480829366e0a.json')



# Authenticate to Salesforce
loginInfo = json.load(open(r'E:\Software\loginpd.json'))
username = loginInfo['username']
password = loginInfo['password']
security_token = loginInfo['security_token']

session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token)
sf = Salesforce(instance=instance, session_id=session_id)


# Authenticate to BigQuery
bqscopes = ["https://www.googleapis.com/auth/bigquery"]
bqcredentials = service_account.Credentials.from_service_account_file(
     r"E:\Software\python-project-403413-480829366e0a.json",
    scopes=bqscopes    
)
project_id = "southern-coda-233109"
location = "EU"

# Create the BigQuery client with the specified project ID and location
client = bigquery.Client(credentials=bqcredentials, project=project_id, location=location)

# Read SQL file and execute query
sql_pocs_abgabetermin_CWNW_due = codecs.open('hardcoded_ids.sql', mode='r', encoding='utf-8-sig').read()
df_sql_pocs_abgabetermin_CWNW_due = client.query(sql_pocs_abgabetermin_CWNW_due).to_dataframe()

# Ensure 'sc_sf_account_id' is treated as a string and handle missing values
df_sql_pocs_abgabetermin_CWNW_due.loc[:, 'sc_sf_account_id'] = df_sql_pocs_abgabetermin_CWNW_due['sc_sf_account_id'].astype(str).replace(['None', 'nan', 'NaN', '', '<NA>'], np.nan)

# Drop rows where 'sc_sf_account_id' is NaN or empty
df_filtered = df_sql_pocs_abgabetermin_CWNW_due.dropna(subset=['sc_sf_account_id'])
df_filtered_unique = df_filtered.drop_duplicates(subset=['sc_sf_account_id'])

# Update Google Sheets with filtered data
spreadsheet = gc.open('This week\'s SC calls')
try:
    worksheet = spreadsheet.worksheet('title', 'Sheet1')  
except pygsheets.WorksheetNotFound:
    worksheet = spreadsheet.add_worksheet('Sheet1')  
worksheet.clear()
worksheet.set_dataframe(df_filtered, (1, 1))
print('worksheet updated successfully!')

# Create a list to hold the new work items
new_work_items = []

# Iterate through each unique account in the filtered DataFrame
for index, row in df_filtered_unique.iterrows():
    account_id = row['sc_sf_account_id']
    agent_company_id = row['agent_companyId']
    account_status = row['account_status']

    owner_id = '00GOj00000BHTP7MAP'

    # # Assign OwnerId based on the conditions for account status and agent company ID
    # if account_status != 'Customer' and pd.notnull(agent_company_id):
    #     owner_id = '00GOj000009DhObMAK'  # SDR Queue
    # else:
    #     owner_id = '00GOj00000BHTP7MAP'  # CSM Queue

    # Create a new work item dictionary
    new_work_item = {
        'Account__c': account_id,
        'Priority__c': '1',
        'Type__c': 'SC Calling',
        'OwnerId': owner_id,
    }

    new_work_items.append(new_work_item)
    #print('Work item created:', new_work_item)

# Insert new work items if any
if new_work_items:
    #sf.bulk.Account_Work_Item__c.insert(new_work_items)
    print(f"Created {len(new_work_items)} new Account_Work_Item__c records.")
else:
    print("No new Account_Work_Item__c records needed to be created.")

#######################
# Create Bid Requests ##
#######################

# Extract unique bid_request_id values
unique_bid_requests = df_sql_pocs_abgabetermin_CWNW_due[['bid_request_id', 'bid_package_name', 'sc_sf_account_id', 'gc_sf_account_id', 'project_name', 'bid_package_due_date', 'bid_request_first_invite_date', 'bid_request_status']].drop_duplicates()

account_ids = unique_bid_requests['sc_sf_account_id'].dropna().unique().tolist()
accounts_with_work_items = {}

# Query existing Account_Work_Item__c records for all account_ids
ids_str = ', '.join(f"'{id}'" for id in account_ids)
work_item_query = f"SELECT Id, Account__c FROM Account_Work_Item__c WHERE Account__c IN ({ids_str})"
work_item_results = sf.query_all(work_item_query)

# Populate the dictionary with work items
for record in work_item_results['records']:
    accounts_with_work_items[record['Account__c']] = record['Id']

# Create Bid_Request__c records
new_bid_requests = []
for _, row in unique_bid_requests.iterrows():
    account_id = row['sc_sf_account_id']
    gc_account_id = row['gc_sf_account_id']
    project_name = row['project_name']
    bid_package_name = row['bid_package_name']
    bid_request_id = row['bid_request_id']
    bid_package_due_date = row['bid_package_due_date']
    bid_request_first_invite_date = row['bid_request_first_invite_date']
    bid_request_status = row['bid_request_status']

    if account_id in accounts_with_work_items:
        account_work_item_id = accounts_with_work_items[account_id]
        new_bid_request = {
            'Account_Work_Item__c': account_work_item_id,
            'Bid_Package_Name__c': bid_package_name,
            'Bid_Request_Id__c': bid_request_id,
            'Subcontractor__c': account_id,
            'General_Contractor__c': gc_account_id,
            'Project_Name__c': project_name,
            'Bid_Package_Due_Date__c': bid_package_due_date,
            'Bid_Invite_Date__c': bid_request_first_invite_date,
            'Status_of_bid_invite__c': bid_request_status,
        }
        new_bid_requests.append(new_bid_request)
        print(new_bid_request)
# Insert new Bid_Request__c records in bulk
if new_bid_requests:
    #sf.bulk.Bid_Request__c.insert(new_bid_requests)
    print(f"Created {len(new_bid_requests)} new Bid_Request__c records.")
else:
    print("No new Bid_Request__c records needed to be created.")

