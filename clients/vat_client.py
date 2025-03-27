import requests

class CurrencyAPIClient:
    BASE_URL = "https://api.vatcomply.com"

    def __init__(self):
        self.session = requests.Session()

    def fetch_currencies(self):

        url = f"{self.BASE_URL}/currencies"
        response = self.session.get(url)

        return response.json()

    def fetch_base_rates(self, base_currency="NOK"):

        url = f"{self.BASE_URL}/rates?base={base_currency}"
        response = self.session.get(url)

        if response.status_code == 200:
            return response.json()['rates']
        else:
            raise Exception(f"Error fetching rates for {base_currency}: {response.status_code}")
