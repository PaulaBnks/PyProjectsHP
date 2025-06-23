# salesforce_auth.py

from simple_salesforce import Salesforce, SalesforceLogin
import json
import os

def login_to_salesforce():
    #print("🔐 Logging into Salesforce...")
    if os.getenv('RUN_ON_GIT_ACTIONS') == 'Yes':
        sf = Salesforce(
            username=os.environ["username"],
            password=os.environ["pwd"],
            security_token=os.environ["token"],
            client_id='python'
        )
        session_id, instance = SalesforceLogin(
            username=os.environ["username"],
            password=os.environ["pwd"],
            security_token=os.environ["token"]
        )
    else:
        login_info = json.load(open(r"E:\Software\loginpd.json"))
        username = login_info['username']
        password = login_info['password']
        security_token = login_info['security_token']
        session_id, instance = SalesforceLogin(username=username, password=password, security_token=security_token)
        sf = Salesforce(instance=instance, session_id=session_id)
    print("✅ Salesforce login successful.")
    return sf, session_id, instance