from google.cloud import bigquery
from google.oauth2 import service_account

# TODO(developer): Set key_path to the path to the service account key
#                  file.
key_path = "/Users/reyna.feng/Downloads/unity-other-learn-prd-7c5a7f03e9bc.json"

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
