import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    """Creates a single, local Spark session for all tests."""
    spark_session = (
        SparkSession.builder
        .appName("SwiftRoute-Testing")
        .master("local[1]") # Use a single thread for predictable testing
        .getOrCreate()
    )
    # Reduce logging noise during tests
    spark_session.sparkContext.setLogLevel("ERROR")
    
    yield spark_session
    
    spark_session.stop()