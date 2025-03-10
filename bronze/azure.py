from azure.storage.blob import BlobServiceClient
import csv
import io
import os


def list_dynamic_blobs(container_name, prefix=''):
    connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    container_client = blob_service_client.get_container_client(container_name)

    blob_list = container_client.list_blobs(name_starts_with=prefix)

    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob)

        process_blob(blob_client)


def process_blob(blob_client):
    try:
        blob_data = blob_client.download_blob()
        raw_data = blob_data.readall().decode('utf-8')

        # upload_blob_data(blob_client, raw_data, blob_data['name'])

        raw_json_data = []
        if blob_data.name.endswith('csv'):
            raw_json_data = csv_to_json_from_string(raw_data)
        else:
            print('yes')

        # push_to_kafka(raw_json_data) #DOD

    except Exception as e:
        print(f"Failed to download blob {blob_client.blob_name}: {str(e)}")


def csv_to_json_from_string(csv_content):
    csv_file = io.StringIO(csv_content)
    data = []

    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        data.append(row)

    return data


