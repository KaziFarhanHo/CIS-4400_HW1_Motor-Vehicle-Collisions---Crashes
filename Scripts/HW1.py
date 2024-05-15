import requests
import pandas as pd
from google.cloud import storage, bigquery
from google.oauth2 import service_account
from io import StringIO
import datetime

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

# Credentials info deleted for security reasons
credentials_info = {

}
credentials = service_account.Credentials.from_service_account_info(credentials_info)
gcs_client = storage.Client(credentials=credentials)
bq_client = bigquery.Client(credentials=credentials)

url = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"
total_rows = 8500
batch_size = 1000
data = fetch_data(url, total_rows, batch_size)

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

blob = bucket.blob(csv_filename)
csv_in_memory = StringIO(blob.download_as_text())
data = pd.read_csv(csv_in_memory)

# Ensure the 'Date' column is in datetime format
data['Date'] = pd.to_datetime(data['crash_date']).dt.date  # Adjust the column name if needed

# Transformations to fit the DateDimension table
data['DateKey'] = pd.util.hash_pandas_object(data['Date'], index=False).astype(str)  # Create unique identifiers
data['Year'] = pd.to_datetime(data['Date']).dt.year  # Convert Date again if necessary
data['Quarter'] = pd.to_datetime(data['Date']).dt.quarter
data['Month'] = pd.to_datetime(data['Date']).dt.month
data['Day'] = pd.to_datetime(data['Date']).dt.day
data['WeekdayName'] = pd.to_datetime(data['Date']).dt.strftime('%A')

# Delete existing records in the DateDimension table
bq_client.query('DELETE FROM `my-cis4400-homework.cis4400.DateDimension` WHERE TRUE').result()
print("Deleted existing records from DateDimension.")

# Load to DateDimension
job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND)
table_ref = bq_client.dataset('cis4400').table('DateDimension')
job = bq_client.load_table_from_dataframe(data[['DateKey', 'Date', 'Year', 'Quarter', 'Month', 'Day', 'WeekdayName']], table_ref, job_config=job_config)
job.result()
print(f"Loaded {job.output_rows} rows into DateDimension.")

# Assuming you have similar transformation needs for IncidentsFact
data['IncidentKey'] = pd.util.hash_pandas_object(data.index).astype(str)  # Create unique identifiers for incidents
data['SomeMeasure'] = 100  # Placeholder
data['OtherMeasure'] = 200  # Placeholder

# Delete existing records in the IncidentsFact table
bq_client.query('DELETE FROM `my-cis4400-homework.cis4400.IncidentsFact` WHERE TRUE').result()
print("Deleted existing records from IncidentsFact.")

# Load to IncidentsFact
table_ref = bq_client.dataset('cis4400').table('IncidentsFact')
job = bq_client.load_table_from_dataframe(data[['IncidentKey', 'DateKey', 'SomeMeasure', 'OtherMeasure']], table_ref, job_config=job_config)
job.result()
print(f"Loaded {job.output_rows} rows into IncidentsFact.")

