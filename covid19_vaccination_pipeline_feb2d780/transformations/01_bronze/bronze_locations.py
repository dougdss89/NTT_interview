"""
Bronze Layer - Locations
Raw ingestion of locations.csv containing country vaccine information
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

@dp.materialized_view(
    comment="Bronze table: Raw locations data from CSV with country vaccine information"
)
def bronze_locations() -> DataFrame:
    """
    Ingest raw locations CSV file.
    
    Schema:
    - location: country name
    - iso_code: 3-letter country code
    - vaccines: comma-separated list of vaccine names
    - last_observation_date: last date of data observation
    - source_name: data source name
    - source_website: data source URL
    """
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/workspace/default/covid_vaccination_sources/locations.csv")
    )
