from google.cloud import bigquery
from google.oauth2 import service_account
import os

KEY = 'credentials/key.json'
CREDENTIALS = service_account.Credentials.from_service_account_file(KEY)

bigquery_client = bigquery.Client(credentials=CREDENTIALS)
  
stmt = f"""
SELECT
  IF (DATETIME_DIFF(CURRENT_DATETIME('America/Sao_Paulo'), start_time, SECOND) > 900, TRUE, FALSE) AS blocked
FROM `telefonica-digitalsales.coe_datalake.sap_commerce_rt_logs`
WHERE
  report LIKE '%Realtime%'
ORDER BY 
  start_time
DESC 
LIMIT 1
"""
query_job = bigquery_client.query(stmt)

# Se existir uma execução rodando mais de 15 minutos, mata ela
response = query_job.to_dataframe().iloc[0, 0]
if response:
  os.system("pkill -f 'python3 sap_rt_bq.py'")
