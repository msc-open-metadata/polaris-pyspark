import pytest
import requests
from unittest.mock import patch
from polarisx.rest_client import PolarisXRestClient


# Just test client correctly sends HTTP requests and handles responses.. it is very simply, so it doesnt really give us much
# But, we do instantiate the client with the api url, and then we call the post function with a path and a payload, and we check if the response is as expected

MOCK_API_URL = "http://mock-polarisx-api.com"


@patch("requests.post")
def test_post_function(mock_post : requests.post):
    """Test if PolarisXRestClient correctly sends a POST request."""
    
    # Create an instance of PolarisXRestClient
    client = PolarisXRestClient(MOCK_API_URL, "s:s")

    # Fake the API response on the mock object (the requests.post function)
    mock_post.return_value.status_code = 200
    mock_post.return_value.json.return_value = {"success": True}

    # Call the actual function (this normally calls requests.post - which is replaced with mock_post here)
    response = client.post("/functions", {"name": "test_function"})

    # Check if we got the expected response
    assert response == {"success": True}

    # Verify that requests.post was actually called with the right URL & data
    mock_post.assert_called_with(
        f"{MOCK_API_URL}/functions",
        json={"name": "test_function"},
        headers={"Authorization": "Bearer s:s"}
    )
