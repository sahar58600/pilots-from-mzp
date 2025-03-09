from bronze.azure import list_dynamic_blobs
from dotenv import load_dotenv

load_dotenv()

print('started')

if __name__ == "__main__":
    container_name = "flight"  # Replace with your container name
    prefix = "flight/2024-03-09_00-00-00/"  # Replace with desired blob prefix or leave as an empty string

    list_dynamic_blobs(container_name, prefix)