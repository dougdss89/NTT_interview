"""
Silver Layer - Vaccinations
Cleaned and enriched vaccination data with date parsing and derived columns
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, to_date, year, month, coalesce, lit

@dp.materialized_view(
    comment="Silver table: Cleaned vaccinations with parsed dates, year/month extraction, and null handling"
)
@dp.expect_all({
    "valid_country": "country IS NOT NULL",
    "valid_iso_code": "iso_code IS NOT NULL",
    "valid_date": "date IS NOT NULL"
})
def silver_vaccinations() -> DataFrame:
    """
    Transform bronze vaccinations data:
    - Parse date string to date type
    - Extract year and month for aggregations
    - Coalesce null vaccination counts to 0
    - Filter out invalid records
    
    Schema:
    - country: country name
    - iso_code: 3-letter country code
    - date: vaccination date (parsed to date type)
    - year: extracted year
    - month: extracted month
    - total_vaccinations: cumulative vaccinations (nulls → 0)
    - people_vaccinated: number of people vaccinated
    - daily_vaccinations: daily vaccination count
    - various per capita metrics
    """
    return (
        spark.read.table("bronze_vaccinations")
        .select(
            col("country"),
            col("iso_code"),
            # Parse date string to date type
            to_date(col("date"), "yyyy-MM-dd").alias("date"),
            # Extract year and month for aggregations
            year(to_date(col("date"), "yyyy-MM-dd")).alias("year"),
            month(to_date(col("date"), "yyyy-MM-dd")).alias("month"),
            # Handle nulls in vaccination counts
            coalesce(col("total_vaccinations"), lit(0)).cast("long").alias("total_vaccinations"),
            coalesce(col("people_vaccinated"), lit(0)).cast("long").alias("people_vaccinated"),
            coalesce(col("daily_vaccinations"), lit(0)).cast("long").alias("daily_vaccinations"),
            col("total_vaccinations_per_hundred"),
            col("people_vaccinated_per_hundred"),
            col("daily_vaccinations_per_million"),
            col("daily_people_vaccinated"),
            col("daily_people_vaccinated_per_hundred")
        )
        # Filter out records with missing critical fields
        .filter(
            col("country").isNotNull() & 
            col("iso_code").isNotNull() & 
            col("date").isNotNull()
        )
    )
