WITH
SCs_to_be_called as (
Select *
FROM
(
  SELECT
    sf_accounts.account_id AS gc_sf_account_id,
    sf_accounts.account_type AS sf_account_type,
    sf_accounts.account_name AS sf_account_name,
    sf_accounts_sc.Account_Status__c AS account_status,
    bid_packages.id AS bid_package_id,
    bid_packages.bid_package_name AS bid_package_name,
    bid_requests.id AS bid_request_id,
    bid_packages.bid_request_count AS bid_request_count,
    sf_accounts_sc.id AS sc_sf_account_id,
    sf_accounts_sc.Agent_Company_Id__c AS agent_companyId,
    
    bid_requests.subcontractor_name AS subcontractor_name,
    bid_requests.bid_request_status AS bid_request_status,
    bid_requests.has_bid AS has_bid,
    sf_accounts.billing_country_code AS sf_billing_country_code,
    bid_requests.subcontractor_type AS subcontractor_type,
    bid_requests.project_name AS project_name,

    DATE(TIMESTAMP(FORMAT_TIMESTAMP('%F %T', bid_packages.published_at, 'Europe/Berlin'))) AS bid_package_published_date,   
    DATE(TIMESTAMP(FORMAT_TIMESTAMP('%F %T', bid_packages.bids_due_at, 'Europe/Berlin'))) AS bid_package_due_date,   
    DATE(TIMESTAMP(FORMAT_TIMESTAMP('%F %T', bid_requests.bids_due_at, 'Europe/Berlin'))) AS bid_request_bid_due_date,
    DATE(TIMESTAMP(FORMAT_TIMESTAMP('%F %T', bid_requests.first_invite_at, 'Europe/Berlin'))) AS bid_request_first_invite_date,

    CASE
    WHEN LENGTH(bid_requests.subcontractor_phone_numbers) > 16 THEN regexp_extract (bid_requests.subcontractor_phone_numbers,'[^,]*')
    ELSE bid_requests.subcontractor_phone_numbers
    END AS sc_phone_final,
    CASE
    WHEN LENGTH(bid_requests.subcontractor_emails) > 20 THEN regexp_extract (bid_requests.subcontractor_emails,'[^,]*')
    ELSE bid_requests.subcontractor_emails
    END AS sc_email_final,
    CASE
    WHEN bid_requests.subcontractor_id IS NOT NULL THEN (bid_packages.company_submitted_bid_request_count+bid_packages.network_submitted_bid_request_count)
    END AS recieved_bids,
    CONCAT(
      "SC Calling: Project name",bid_requests.project_name," ","Bid Package name;"," ",bid_requests.bid_package_name)
    AS subject,
    CASE
    WHEN sf_accounts.account_type = "GC New Business POC" THEN "Hoch"
    WHEN sf_accounts.account_type = "POC / Enterprise" THEN "Mittel"
    WHEN sf_accounts.account_type = "POC" THEN "Mittel"
    WHEN sf_accounts.account_type = "Freemium POC" THEN "Niedrig"
    ELSE "Niedrig"
    END AS priority,
  FROM southern-coda-233109.dbt_production_core.f_cosuno_bid_packages AS bid_packages
  LEFT JOIN `southern-coda-233109.dbt_production_core.f_cosuno_companies` AS cosuno_companies
  ON bid_packages.company_id = cosuno_companies.id
  Left JOIN `southern-coda-233109.dbt_production_sales.d_salesforce_accounts` AS sf_accounts
  ON cosuno_companies.salesforce_account_id = sf_accounts.account_id
  LEFT JOIN `southern-coda-233109.dbt_production_core.f_cosuno_bid_requests` AS bid_requests
  ON bid_packages.id = bid_requests.bid_package_id
  LEFT JOIN `southern-coda-233109.estuary_salesforce.account` AS sf_accounts_sc
  ON bid_requests.subcontractor_id = sf_accounts_sc.Subcontractor_ID__c
  WHERE cosuno_companies.deleted_at IS NULL
  AND bid_packages.deleted_at IS NULL
  AND bid_requests.deleted_at IS NULL
)as X
)
SELECT
SCs_to_be_called.bid_package_id AS bid_package_id,
SCs_to_be_called.sc_sf_account_id AS sc_sf_account_id,
SCs_to_be_called.agent_companyId,
SCs_to_be_called.account_status,
SCs_to_be_called.sf_account_name AS sf_account_name,
SCs_to_be_called.sf_account_type AS sf_account_type,
SCs_to_be_called.sc_email_final AS sc_email,
SCs_to_be_called.sc_phone_final AS sc_phone,
SCs_to_be_called.subcontractor_name AS sc_name,
FORMAT_DATE("%F",SCs_to_be_called.bid_package_due_date) AS bid_package_due_date,
FORMAT_DATE("%F",SCs_to_be_called.bid_request_bid_due_date) AS bid_request_bid_due_date,
FORMAT_DATE("%F",SCs_to_be_called.bid_request_first_invite_date) AS bid_request_first_invite_date,
FORMAT_DATE("%F",SCs_to_be_called.bid_package_published_date) AS bid_package_published_date,
SCs_to_be_called.subject AS subject,
SCs_to_be_called.bid_package_name AS bid_package_name,
SCs_to_be_called.gc_sf_account_id AS gc_sf_account_id,
SCs_to_be_called.project_name AS project_name,
SCs_to_be_called.bid_request_status AS bid_request_status,
SCs_to_be_called.priority AS priority,
SCs_to_be_called.bid_request_id AS bid_request_id,
CAST (SCs_to_be_called.bid_request_count AS string) AS bid_request_count,
FROM SCs_to_be_called
Where SCs_to_be_called.sf_billing_country_code IN ("DE","AT")
--AND SCs_to_be_called.sf_account_type IN ("POC","POC / Enterprise","Freemium POC","GC New Business POC")
AND SCs_to_be_called.sc_phone_final IS NOT NULL
--AND SCs_to_be_called.bid_request_status IN ("opened","viewed","invited","accepted","watched")
AND SCs_to_be_called.bid_request_status IN ("opened","viewed","invited","watched")
AND SCs_to_be_called.bid_package_id IN ('a4129ad9-ec16-4c17-93e2-79b5634d8eab')

AND SCs_to_be_called.recieved_bids <= 2
AND SCs_to_be_called.subcontractor_type = "network"
AND SCs_to_be_called.has_bid = false
-- AND EXTRACT(YEAR FROM bid_request_bid_due_date) = Extract(Year From Current_date())
-- --AND bid_request_bid_due_date <> Current_date()
-- AND (Extract(WEEK FROM bid_request_bid_due_date) = Extract(week From Current_date())
--  or Extract(WEEK FROM bid_request_bid_due_date) = EXTRACT(WEEK FROM CURRENT_DATE() + INTERVAL 1 WEEK))
--AND SCs_to_be_called.gc_sf_account_id = '00109000013HoaoAAC'