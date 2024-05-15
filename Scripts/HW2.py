import subprocess
import sys

# Install the necessary package
subprocess.check_call([sys.executable, "-m", "pip", "install", "db-dtypes"])

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Credentials deteled for security reasons 
credentials_info = {

}
credentials = service_account.Credentials.from_service_account_info(credentials_info)
bq_client = bigquery.Client(credentials=credentials, project=credentials_info['project_id'])

# Function to clean and load data into refined tables
def clean_and_load_data(source_table, refined_table):
    # Fetch data from source table
    query = f'SELECT * FROM `{credentials_info["project_id"]}.cis4400.{source_table}`'
    df = bq_client.query(query).to_dataframe()

    # Replace all string null values with "Data not found"
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].fillna('Data not found')

    # Delete existing records in refined table
    bq_client.query(f'DELETE FROM `{credentials_info["project_id"]}.cis4400.{refined_table}` WHERE TRUE').result()
    print(f"Deleted existing records from {refined_table}.")

    # Load the refined data into the refined table
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
    table_ref = bq_client.dataset('cis4400').table(refined_table)
    job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    print(f"Loaded {job.output_rows} rows into {refined_table}.")

# Perform ETL for DateDimension and DateDimensionRefined
clean_and_load_data('DateDimension', 'DateDimensionRefined')

# Perform ETL for IncidentsFact and IncidentsFactRefined
clean_and_load_data('IncidentsFact', 'IncidentsFactRefined')
