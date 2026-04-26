"""
Gold Layer - Countries by Vaccine Types
Business Question: What country(s) use more kind of vaccines?

Answers which countries have the highest diversity of vaccine types.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col

@dp.materialized_view(
    comment="Gold table: Countries ranked by number of distinct vaccine types used"
)
def gold_countries_vaccine_types() -> DataFrame:
    """
    Rank countries by the number of distinct vaccine types they use.
    
    Business Question: What country(s) use more kind of vaccines?
    
    Returns countries ordered by vaccine_count (descending), showing which
    countries have the highest vaccine diversity.
    
    Schema:
    - location: country name
    - iso_code: 3-letter country code
    - vaccine_count: number of distinct vaccines used
    - vaccines: original comma-separated vaccine list
    - vaccines_array: array of individual vaccine names
    """
    return (
        spark.read.table("silver_locations")
        .select(
            col("location"),
            col("iso_code"),
            col("vaccine_count"),
            col("vaccines"),
            col("vaccines_array")
        )
        # Order by vaccine count descending to show countries with most variety
        .orderBy(col("vaccine_count").desc())
    )
