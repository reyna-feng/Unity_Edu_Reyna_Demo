from google_auth_oauthlib import flow
launch_browser = True
appflow = flow.InstalledAppFlow.from_client_secrets_file(
    "/Users/reyna.feng/Downloads/client_secret_255942804857-sf5h8ptnitg5kto0g00jin9td0pt9e8r.apps.googleusercontent.com.json", scopes=["https://www.googleapis.com/auth/bigquery"]
)

if launch_browser:
    appflow.run_local_server()
else:
    appflow.run_console()

credentials = appflow.credentials

from google.cloud import bigquery
project = 'unity-other-learn-prd'
client = bigquery.Client(project=project, credentials=credentials)

import sqlite3
from sqlite3 import OperationalError
fd = open('./egl_academic_kpi.sql', 'r')
sqlFile = fd.read()
fd.close()

# all SQL commands (split on ';')
sqlCommands = sqlFile.split(';')

# Execute every command from the input file
for command in sqlCommands:
    # This will skip and report errors
    # For example, if the tables do not yet exist, this will skip over
    # the DROP TABLE commands
    df = client.query(command).to_dataframe()
    print(df)