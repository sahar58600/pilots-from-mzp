from azure.storage.blob import BlobServiceClient
import csv
import io
import os




def list_dynamic_blobs(container_name, prefix=''):
    CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")

    blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)

    container_client = blob_service_client.get_container_client(container_name)

    blob_list = container_client.list_blobs(name_starts_with=prefix)

    for blob in blob_list:
        blob_client = container_client.get_blob_client(blob)

        download_blob(blob_client)


def download_blob(blob_client):
    try:
        blob_data = blob_client.download_blob()
        raw_json_data = []
        if blob_data.name.endswith('csv'):
            csv_content = blob_data.readall().decode('utf-8')
            raw_json_data = csv_to_json_from_string(csv_content)
        else:
            print('yes')



        print(raw_json_data)

    except Exception as e:
        print(f"Failed to download blob {blob_client.blob_name}: {str(e)}")


def csv_to_json_from_string(csv_content):
    csv_file = io.StringIO(csv_content)
    data = []

    csv_reader = csv.DictReader(csv_file)

    for row in csv_reader:
        data.append(row)

    return data