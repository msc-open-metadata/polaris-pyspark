from pyspark.sql.catalog import Catalog
from pyspark.sql import SparkSession
from polarisx.rest_client import PolarisXRestClient
import re
import requests

class PolarisXCatalog(Catalog):
    def __init__(self, spark: SparkSession, api_endpoint: str):
        """
        Custom Catalog for PolarisX that manages functions via PolarisXRestClient.
        """
        self.spark = spark
        creds = spark.conf.get("spark.sql.catalog.polaris.credential")
        self.client = PolarisXRestClient(api_endpoint, creds)

    def create_function(self, function_name: str, function_body: str):
        """
        Sends a CREATE FUNCTION request to Polaris API via PolarisXRestClient.
        Returns the API response.
        """
        payload = {"name": function_name, "body": function_body}
        try:
            return self.client.post("/management/v1/functions", payload)
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

    def show_functions(self):
        """
        Fetches all functions stored in Polaris via PolarisXRestClient.
        Returns the API response.
        """
        try:
            return self.client.get("/management/v1/functions")
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

    def sql(self, query: str):
        """
        Intercepts SQL queries related to FUNCTIONS and calls the Polaris API.
        """
        query_cleaned = query.strip()

        # Unified regex pattern to parse CREATE FUNCTION and SHOW FUNCTIONS
        create_function_pattern = r"create function\s+(\w+)\s+as\s+(.*?)\s+using polarisx"
        show_functions_pattern = r"show functions\s+using polarisx"

        # Match CREATE FUNCTION
        create_match = re.match(create_function_pattern, query_cleaned, re.IGNORECASE)
        if create_match:
            function_name = create_match.group(1)  # Extract the function name
            function_body = create_match.group(2)  # Extract the function body

            # Call the PolarisX API to create the function
            response = self.create_function(function_name, function_body)
            if "error" in response:
                return f"Error creating function {function_name}: {response['error']}"
            return f"Function {function_name} created successfully in Polaris: {response}"

        # Match SHOW FUNCTIONS
        show_match = re.match(show_functions_pattern, query_cleaned, re.IGNORECASE)
        if show_match:
            functions = self.show_functions()
            if "error" in functions:
                return f"Error fetching functions: {functions['error']}"
            return f"Functions retrieved successfully from Polaris: {functions}"

        # Fallback to Spark for all other queries
        return self.spark.sql(query_cleaned)