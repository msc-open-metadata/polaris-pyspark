from pyspark.sql import SparkSession
from polarisx.catalog import PolarisXCatalog

# Configure a mock Polaris API endpoint
MOCK_API_URL = "http://localhost:5000"

def main():
    # Create a Spark session
    spark = SparkSession.builder \
        .appName("PolarisXCatalogTest") \
        .master("local[*]") \
        .getOrCreate()

    # Initialize the PolarisXCatalog
    catalog = PolarisXCatalog(spark, MOCK_API_URL)

    # Test CREATE FUNCTION SQL command
    print("Testing CREATE FUNCTION...")
    create_response = catalog.sql("CREATE FUNCTION test_func AS 'return x + 1' USING PolarisX")
    print(create_response)

    # Test SHOW FUNCTIONS SQL command
    print("\nTesting SHOW FUNCTIONS...")
    show_response = catalog.sql("SHOW FUNCTIONS USING PolarisX")
    print(show_response)

    # Test fallback to Spark
    print("\nTesting fallback SQL query...")
    fallback_response = catalog.sql("SELECT 'Hello, Polaris!' AS message")
    print(fallback_response.show())

if __name__ == "__main__":
    main()
