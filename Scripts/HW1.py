import requests
import pandas as pd
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from io import StringIO
import datetime

# Function to fetch data in batches
def fetch_data(base_url, total_rows, batch_size):
    data = []
    for offset in range(0, total_rows, batch_size):
        response = requests.get(f"{base_url}?$limit={batch_size}&$offset={offset}")
        if response.status_code == 200:
            batch_data = pd.DataFrame(response.json())
            data.append(batch_data)
        else:
            print(f"Failed to fetch data: {response.status_code}")
            break
    return pd.concat(data, ignore_index=True)

# Hardcoded service account credentials. Will be externalized in actual deployment
credentials_info = {
  }

# Initialize GCS and BigQuery client with hardcoded credentials
credentials = service_account.Credentials.from_service_account_info(credentials_info)
gcs_client = storage.Client(credentials=credentials)
bq_client = bigquery.Client(credentials=credentials)

# URL of the API and parameters for fetching data
url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
total_rows = 8500
batch_size = 1000
data = fetch_data(url, total_rows, batch_size)

# Save data to CSV in GCS
bucket_name = 'kfhhw1-2'
bucket = gcs_client.bucket(bucket_name)
now = datetime.datetime.now()
formatted_date_time = now.strftime("%Y%m%d-%H%M%S")
csv_filename = f'nyc_data_{formatted_date_time}.csv'
csv_in_memory = StringIO()
data.to_csv(csv_in_memory, index=False)
csv_in_memory.seek(0)
blob = bucket.blob(csv_filename)
blob.upload_from_string(csv_in_memory.getvalue(), content_type='text/csv')
print(f"Data has been successfully saved to GCS as {csv_filename}.")

# Download the file from GCS, transform, and load to BigQuery
blob = bucket.blob(csv_filename)
csv_in_memory = StringIO(blob.download_as_text())
data = pd.read_csv(csv_in_memory)

# Load data to DateDimension table
dataset_id = 'cis4400'
table_id = 'DateDimension'
table_ref = bq_client.dataset(dataset_id).table(table_id)
job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
job = bq_client.load_table_from_dataframe(data, table_ref, job_config=job_config)
job.result()  # Wait for the job to complete
print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

# Load data to IncidentsFact table
table_id = 'IncidentsFact'
table_ref = bq_client.dataset(dataset_id).table(table_id)
job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
job = bq_client.load_table_from_dataframe(data, table_ref, job_config=job_config)
job.result()  # Wait for the job to complete
print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")
