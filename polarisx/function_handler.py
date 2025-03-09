import re
import requests
from pyspark.sql import SparkSession


class FunctionHandler:
    def __init__(self, client):
         """
            Handles functions in Polaris via PolarisXRestClient. (add for seperation of concerns)
            Methods:
            - create_function: Creates a function in Polaris.
            - show_functions: Fetches all functions stored in Polaris.
         """
         self.client = client

    def create_function(self, match: re.Match, spark_session: SparkSession):
        """
            Reads code from FILE resources, if any, supporting local and remote file URIs. 
            Creates a function in Polaris via PolarisXRestClient. 
        """
        
        # Parse 
        function_name, class_name, resource_location = self.parse_create_details(match)

        payload = {
            "name": function_name,
            "arguments": {
                "class_name": class_name,
            }
        }

        if resource_location:
            resource_type = resource_location["type"]
            file_uri = resource_location["uri"]

            #TODO: should we add support for JAR and ARCHIVE? -> currently it makes more sense to only support FILE, e.g. .py, .sql etc.
            #TODO: one more thing, should we include support for inline functions? That is not made with SQL? probably out of scope..
            if resource_type.strip().upper() == "FILE":
                try:
                    file_content = self.fetch_file_contents(spark_session, file_uri)
                    payload["arguments"]["code"] = file_content
                except IOError as e:
                    return {"error reading file content": str(e)}
            else:
                return {"error": f"Unsupported resource type: {resource_type}. Only FILE is supported."}

        try:
            return self.client.post("/management/v1/functions", payload)
        except requests.exceptions.RequestException as e:
            return {"error sending request": str(e)}
                 

    def show_functions(self):
        """
        Fetches all functions stored in Polaris via PolarisXRestClient.
        Returns the API response.
        """
        try:
            return self.client.get("/management/v1/functions")
        except requests.exceptions.RequestException as e:
            return {"error fetching functions": str(e)}


    # Helper method to parse function details from CREATE FUNCTION query
    def parse_create_details(self, match: re.Match):
        function_name = match.group(1)  # Extract function name
        class_name = match.group(2)  # Extract class name
        resource_string = match.group(3)  # Extract raw resource locations (if present)

        # --------------------------------------------------------
        # Parser: Extract the single resource location from USING clause
        # --------------------------------------------------------
        # - Captures exactly one resource type (JAR, FILE, ARCHIVE) and its URI
        #
        # Explanation:
        # - (JAR|FILE|ARCHIVE)  # Captures one of these three as group 1
        # - \s+'([^']+)'        # Captures the URI inside single quotes as group 2
        #
        resource_pattern = r"(JAR|FILE|ARCHIVE)\s+'([^']+)'"

        resource_location = None  # Default to None
        if resource_string:
            resource_match = re.match(resource_pattern, resource_string, re.IGNORECASE)
            if resource_match:
                resource_location = {"type": resource_match.group(1), "uri": resource_match.group(2)}
        return function_name, class_name, resource_location

    # Read the contents of FILE resource URIs using Spark
    # This helper method is an alternative to standard Python file I/O, allowing Spark to read from HDFS, S3, etc.
    def fetch_file_contents(self, spark: SparkSession, file_uri: str) -> str:
        """
        Reads file contents from local or remote file URIs (HDFS, S3, etc.) using Spark.
        """
        try:
            df = spark.read.text(file_uri)
            return "\n".join(row.value for row in df.collect())
        except Exception as e:
            raise IOError(f"Failed to read file from {file_uri}: {str(e)}")