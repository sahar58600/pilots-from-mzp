import csv
import io
from pandas import pd

destination_container_name = 'pilots-from-mazpen'

def fetchAndStore(blob_service_client, source_container_name, prefix=''):
    source_container_client = blob_service_client.get_container_client(source_container_name)
    destination_container_client = blob_service_client.get_container_client(destination_container_name)

    source_blob_list = source_container_client.list_blobs(name_starts_with=prefix)

    for source_blob in source_blob_list:
        source_blob_client = source_container_client.get_blob_client(source_blob)
        source_blob_data_content = source_blob_client.download_blob().readall()

        dataframe = pd.read_csv(io.StringIO(source_blob_data_content))
        parquet_buffer = io.BytesIO()
        dataframe.to_parquet(parquet_buffer, engine='pyarrow', index=False)
        parquet_buffer.seek(0)

        destination_container_client.upload_blob(name=source_blob.name,
                                                 data=parquet_buffer,
                                                 overwrite=True)

        # process_blob(blob_client)


def process_blob(blob_client):
    try:
        blob_data = blob_client.download_blob()
        raw_data = blob_data.readall().decode('utf-8')

        # upload_blob_data(blob_client, raw_data, blob_data['name'])

        raw_json_data = []
        if blob_data.name.endswith('csv'):
            raw_json_data = csv_to_json(raw_data)
        else:
            print('yes')

        # push_to_kafka(raw_json_data) #DOD

    except Exception as e:
        print(f"Failed to download blob {blob_client.blob_name}: {str(e)}")

def csv_to_json(csv_content):
    csv_file = io.StringIO(csv_content)
    data = []

    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        data.append(row)

    return data