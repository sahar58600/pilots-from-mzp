from bronze.azure import list_dynamic_blobs
from dotenv import load_dotenv

from bronze.bank import fetch_exchange_rates

load_dotenv()

print('started')

if __name__ == "__main__":
    container_name = "flight"
    prefix = "flight/2024-03-09_00-00-00/"
    fetch_exchange_rates()

    list_dynamic_blobs(container_name, prefix)