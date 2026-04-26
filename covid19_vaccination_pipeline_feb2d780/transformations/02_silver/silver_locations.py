"""
Silver Layer - Locations
Cleaned and enriched locations data with vaccine array and counts
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, split, trim, size, when

@dp.materialized_view(
    comment="Silver table: Cleaned locations with vaccines split into array and vaccine count"
)
@dp.expect_all({
    "valid_iso_code": "iso_code IS NOT NULL",
    "valid_location": "location IS NOT NULL"
})
def silver_locations() -> DataFrame:
    """
    Transform bronze locations data:
    - Split comma-separated vaccines into array
    - Trim whitespace from vaccine names
    - Calculate vaccine count per country
    - Filter out any invalid records
    
    Schema:
    - location: country name
    - iso_code: 3-letter country code
    - vaccines: original comma-separated string
    - vaccines_array: array of individual vaccine names
    - vaccine_count: number of distinct vaccines used
    - last_observation_date: last date of data observation
    - source_name: data source name
    - source_website: data source URL
    """
    return (
        spark.read.table("bronze_locations")
        .select(
            col("location"),
            col("iso_code"),
            col("vaccines"),
            # Split vaccines by comma and trim whitespace from each element
            split(col("vaccines"), ",\\s*").alias("vaccines_array"),
            col("last_observation_date"),
            col("source_name"),
            col("source_website")
        )
        # Calculate vaccine count from array size
        .withColumn(
            "vaccine_count",
            when(col("vaccines").isNotNull(), size(col("vaccines_array"))).otherwise(0)
        )
        # Filter out records with no location or iso_code
        .filter(
            col("location").isNotNull() & 
            col("iso_code").isNotNull()
        )
    )
