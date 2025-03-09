from pyspark.sql.catalog import Catalog
from pyspark.sql import SparkSession
from polarisx.function_handler import FunctionHandler
from polarisx.rest_client import PolarisXRestClient
import re
import requests


'''
PolarisXCatalog: A Custom PySpark Catalog

By default, `spark.catalog` allows users to:
- List databases, tables, and functions.
- Run `SHOW FUNCTIONS;` to get built-in Spark functions.
- Execute queries using `spark.sql(...)`.

Normally, Spark processes SQL queries using its built-in parser.  
However, `PolarisXCatalog` intercepts queries before Spark runs them:
1. If the query includes `USING PolarisX`, it is sent to the PolarisX API.
2. Otherwise, Spark processes the query as usual.

Example:
```python
spark.catalog.sql("CREATE FUNCTION my_func AS 'return x + 1' USING PolarisX;")

obs. and atm we assume that the Polaris instance contains the object and not just the metadata,
such that we dont have to reconstruct anything.
'''

class PolarisXCatalog(Catalog):
    def __init__(self, spark: SparkSession, api_endpoint: str):
        """
        Custom Catalog for PolarisX that manages functions via PolarisXRestClient.
        """
        self.spark = spark
        creds = spark.conf.get("spark.sql.catalog.polaris.credential")
        
        self.function_handler = FunctionHandler(api_endpoint, creds)
        self.client = PolarisXRestClient(api_endpoint, creds)
        

    def sql(self, query: str):
        """
        Intercepts SQL queries related to FUNCTIONS and calls the Polaris API.
        """
        query_cleaned = query.strip()

        # --------------------------------------------------------
        # Parser: CREATE OPEN FUNCTION
        # --------------------------------------------------------
        # Regex that tolerates (non-capturing) the optional keywords,
        # then captures:
        #  (1) function_name
        #  (2) class_name
        #  (3) the optional resource_locations
        #
        # Explanation:
        # - ^create                 # must start with "create" 
        # - (?:\s+or\s+replace)?     # optional non-capturing group for "or replace"
        # - (?:\s+temporary)?        # optional non-capturing group for "temporary"
        # - \s+function              # literal "function"
        # - (?:\s+if\s+not\s+exists)? # optional non-capturing group for "if not exists"
        # - \s+([\w.]+)              # capture group 1: function_name (allows optional dot for db.func)
        # - \s+as\s+'([^']+)'        # " as " followed by quoted class_name (capture group 2)
        # - (?:\s+using\s+(.+))?     # optional group capturing resource_locations (capture group 3)
        # - $                        # end of string
        #
        create_function_pattern = (
            r"^create"
            r"(?:\s+or\s+replace)?"
            r"(?:\s+temporary)?"
            r"\s+open\s+function" # changed to open function, instead of USING POLARISX
            r"(?:\s+if\s+not\s+exists)?"
            r"\s+([\w.]+)"
            r"\s+as\s+'([^']+)'"
            r"(?:\s+using\s+(.+))?$"
        )

        # --------------------------------------------------------
        # Parser: SHOW OPEN FUNCTIONS
        # --------------------------------------------------------
        # - Matches "SHOW OPEN FUNCTIONS"
        # - Ensures exact syntax match
        #
        show_functions_pattern = r"show\s+open\s+functions"

        # --------------------------------------------------------
        # Extract function details from CREATE FUNCTION query
        # --------------------------------------------------------
        create_match = re.match(create_function_pattern, query_cleaned, re.IGNORECASE)
        if create_match:
            return self.function_handler.create_function(create_match, self.spark)
            
        # --------------------------------------------------------
        # Extract function details from SHOW FUNCTIONS query
        # --------------------------------------------------------
        show_match = re.match(show_functions_pattern, query_cleaned, re.IGNORECASE)
        if show_match:
            return self.function_handler.show_functions()

        # --------------------------------------------------------
        # Fallback: If not CREATE or SHOW, send query to Spark
        # --------------------------------------------------------
        return self.spark.sql(query_cleaned)