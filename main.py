from airflow import DAG
from airflow.operators.python import PythonOperator
from azure.storage.blob import BlobServiceClient
from bronze.azure import fetchAndStore
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

load_dotenv()

account_url = os.getenv("STORAGE_ACCOUNT_URL", 'https://dataengineering2025sa.blob.core.windows.net')
account_key = os.getenv("STORAGE_ACCOUNT_KEY", 'lddwRosEqMuLpfYf7YwkRheeGNatH/cWrg+g6hjvrGSJ6SUIjJ/K+jA4nP//wWGGnyARcDssSPKu+AStRU3kfQ==')
source_container_name = "flight"

def fetchAndStoreFlights():
    blob_service_client = BlobServiceClient(account_url, credential=account_key)

    folder_prefix = "flight/"
    fetchAndStore(blob_service_client, source_container_name, folder_prefix)

def fetchAndStoreCheckIn():
    blob_service_client = BlobServiceClient(account_url, credential=account_key)

    folder_prefix = "checkin/"
    fetchAndStore(blob_service_client, source_container_name, folder_prefix)

def fetchAndStoreFlightBookings():
    blob_service_client = BlobServiceClient(account_url, credential=account_key)

    folder_prefix = "flight_bookings/"
    fetchAndStore(blob_service_client, source_container_name, folder_prefix)

def fetchAndStorePayMe():
    blob_service_client = BlobServiceClient(account_url, credential=account_key)

    folder_prefix = "payme/"
    fetchAndStore(blob_service_client, source_container_name, folder_prefix)

with DAG(
    dag_id="fetchData",
    start_date=datetime(2016, 1, 1),
    schedule_interval=None,
    default_args={"retries": 2},
    catchup=False,
    max_active_runs=1
) as flight:
    fetchAndStoreFlights1 = PythonOperator(task_id='flights',
                                           python_callable=fetchAndStoreFlights)
    fetchAndStoreCheckIn1 = PythonOperator(task_id='check-in',
                                          python_callable=fetchAndStoreCheckIn)
    fetchAndStoreFlightBookings1 = PythonOperator(task_id='flight-bookings',
                                           python_callable=fetchAndStoreFlightBookings)
    fetchAndStorePayMe1 = PythonOperator(task_id='pay-me',
                                           python_callable=fetchAndStorePayMe)


    [fetchAndStoreCheckIn1, fetchAndStoreFlights1, fetchAndStoreFlightBookings1, fetchAndStorePayMe1]