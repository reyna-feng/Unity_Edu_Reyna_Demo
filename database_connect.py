#from google.cloud import bigquery
sql = """
SELECT *
FROM `unity-other-learn-prd.reynafeng.egl_installs` 
LIMIT 10
"""
df = client.query(sql).to_dataframe()