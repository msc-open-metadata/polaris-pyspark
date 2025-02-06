import requests

class PolarisXRestClient:
    #TODO: add support for refresh token
    def __init__(self, api_endpoint: str, credentials: str):
        """
        REST client to interact with Polaris (extended) APIs.
        """
        self.api_endpoint = api_endpoint
        self.credentials = credentials
        self.auth_token = self.get_polaris_oauth_token(credentials)

    def post(self, endpoint: str, json_payload: dict):
        """
        Sends a POST request to a Polaris API endpoint.
        """
        #in our case it is the api url + the /functions endpoint for post
        url = f"{self.api_endpoint}{endpoint}"
        print(url)
        response = requests.post(url, json=json_payload, headers={"Authorization": f"Bearer {self.auth_token}"})
        response.raise_for_status()  # Raise an error for non-2xx status codes
        return response.json()

    def get(self, endpoint: str):
        """
        Sends a GET request to a Polaris API endpoint.
        """
        url = f"{self.api_endpoint}{endpoint}"
        response = requests.get(url, headers={"Authorization": f"Bearer {self.auth_token}"})
        response.raise_for_status() 
        return response.json()

    def get_polaris_oauth_token(self, credentials:str):
        client_id = credentials.split(":")[0]
        client_secret= credentials.split(":")[1]

        url = f"{self.api_endpoint}/catalog/v1/oauth/tokens"
        data = {
            "grant_type": "client_credentials",
            "client_id": f"{client_id}",
            "client_secret": f"{client_secret}",
            "scope": "PRINCIPAL_ROLE:ALL"
        }

        return requests.post(url, data=data).json()["access_token"]