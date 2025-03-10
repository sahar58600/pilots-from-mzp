import requests
import os

def fetch_exchange_rates():
    bank_israel_url = os.getenv('BANK_ISRAEL_EXCHANGE_URL', 'https://boi.org.il/PublicApi/GetExchangeRates?asXml=false')

    try:
        response = requests.get(bank_israel_url)
        response.raise_for_status()

        data = response.json()
        exchange_rates = data.get('exchangeRates', [])

        return exchange_rates
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")