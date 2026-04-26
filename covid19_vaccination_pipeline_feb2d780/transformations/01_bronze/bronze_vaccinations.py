"""
Bronze Layer - Vaccinations
Raw ingestion of vaccinations.json with nested daily vaccination records
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import explode, col

@dp.materialized_view(
    comment="Bronze table: Raw vaccinations data from JSON with exploded daily records"
)
def bronze_vaccinations() -> DataFrame:
    """
    Ingest raw vaccinations JSON file and explode nested data array.
    
    Source JSON structure:
    [
      {
        "country": "Afghanistan",
        "iso_code": "AFG",
        "data": [
          {"date": "2021-02-22", "total_vaccinations": 0, ...},
          {"date": "2021-02-23", "daily_vaccinations": 1367, ...}
        ]
      }
    ]
    
    Output flattened schema:
    - country: country name
    - iso_code: 3-letter country code
    - date: vaccination date
    - total_vaccinations: cumulative vaccinations
    - people_vaccinated: number of people vaccinated
    - daily_vaccinations: daily vaccination count
    - various per capita metrics
    """
    # Read JSON array
    df = (
        spark.read
        .format("json")
        .option("multiLine", "true")
        .load("/Volumes/workspace/default/covid_vaccination_sources/vaccinations.json")
    )
    
    # Explode the nested data array to flatten daily records
    return (
        df
        .select(
            col("country"),
            col("iso_code"),
            explode(col("data")).alias("record")
        )
        .select(
            col("country"),
            col("iso_code"),
            col("record.*")  # Expand all fields from the nested record
        )
    )
