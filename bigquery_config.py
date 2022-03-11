from google.cloud import bigquery
from google.oauth2 import service_account

key_path = "/Users/reyna.feng/Downloads/unity-other-learn-prd-5e8340861a8c.json"

credentials = service_account.Credentials.from_service_account_file(
    key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

sql = """
SELECT *
FROM `unity-other-learn-prd.reynafeng.egl_installs` 
LIMIT 10
"""
df = client.query(sql).to_dataframe()
print(df)
