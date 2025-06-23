import json
from simple_salesforce import Salesforce, SalesforceLogin

# ------------------ Step 1: Salesforce Login ------------------
loginInfo = json.load(open('E:\Software\loginpd.json'))
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
print(sf)