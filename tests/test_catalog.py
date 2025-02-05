import pytest
from unittest.mock import MagicMock, patch
from polarisx.catalog import PolarisXCatalog

MOCK_API_URL = "http://mock-polarisx-api.com"


@pytest.fixture
def mock_spark():
    """Creates a mock SparkSession."""
    return MagicMock()


@pytest.fixture
def catalog(mock_spark):
    """Creates an instance of PolarisXCatalog with mock Spark."""
    return PolarisXCatalog(mock_spark, MOCK_API_URL)


# ---- Test create_function ----

@patch("polarisx.rest_client.PolarisXRestClient.post")
def test_create_function_api_call(mock_post, catalog):
    """Test if create_function sends the correct POST request to the API."""
    # Sim a success API response
    mock_post.return_value = {"success": True}

    # Call create_function
    response = catalog.create_function("my_func", "return x + 1")

    # Assert the POST request was sent with the correct payload
    mock_post.assert_called_once_with(
        "/functions",
        {"name": "my_func", "body": "return x + 1"}
    )

    # Assert the response is returned correctly
    assert response == {"success": True}


# ---- Test show_functions ----

@patch("polarisx.rest_client.PolarisXRestClient.get")
def test_show_functions_api_call(mock_get, catalog):
    """Test if show_functions sends the correct GET request to the API."""
    # Simulate a successful API response
    mock_get.return_value = [{"name": "func1"}, {"name": "func2"}]

    # Call show_functions
    response = catalog.show_functions()

    # Assert the GET request was sent
    mock_get.assert_called_once_with("/functions")

    # Assert the response is returned correctly
    assert response == [{"name": "func1"}, {"name": "func2"}]


# ---- Test sql interception ----

@patch("polarisx.rest_client.PolarisXRestClient.post")
def test_sql_intercepts_create_function(mock_post, catalog):
    """Test if sql() correctly intercepts CREATE FUNCTION queries."""
    # Simulate a successful API response
    mock_post.return_value = {"success": True}

    # Call the intercepted query
    response = catalog.sql("CREATE FUNCTION my_func AS 'return x + 1' USING PolarisX")

    # Assert that create_function was called via the API
    mock_post.assert_called_once_with(
        "/functions",
        {"name": "my_func", "body": "'return x + 1'"}
    )

    # Assert the response is handled correctly
    assert response == f"Function my_func created successfully in Polaris: {mock_post.return_value}"


@patch("polarisx.rest_client.PolarisXRestClient.get")
def test_sql_intercepts_show_functions(mock_get, catalog):
    """Test if sql() correctly intercepts SHOW FUNCTIONS queries."""
    # Simulate a successful API response
    mock_get.return_value = [{"name": "func1"}]

    # Call the intercepted query
    response = catalog.sql("SHOW FUNCTIONS USING PolarisX")

    # Assert that show_functions was called via the API
    mock_get.assert_called_once_with("/functions")

    # Assert the response is handled correctly
    assert response == f"Functions retrieved successfully from Polaris: {mock_get.return_value}"


# ---- Test SQL fallback ----

def test_sql_fallback_to_spark(mock_spark, catalog):
    """Test if sql() passes non-PolarisX queries to Spark unchanged."""
    # Mock Spark's response
    mock_spark.sql.return_value = "MockSpark executed: SELECT * FROM table"

    # Call a non-PolarisX query
    result = catalog.sql("SELECT * FROM table")

    # Assert Spark's sql() was called
    mock_spark.sql.assert_called_once_with("SELECT * FROM table")

    # Assert the response matches Spark's response
    assert result == "MockSpark executed: SELECT * FROM table"
