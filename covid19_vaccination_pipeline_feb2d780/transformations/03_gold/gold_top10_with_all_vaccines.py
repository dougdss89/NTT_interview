"""
Gold Layer - Top 10 Countries with All Vaccines Per Year
Business Question: Top 10 countries per year with all vaccines used, 
ordered by vaccine count and total vaccinations

Combines vaccination totals with vaccine diversity data.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max as max_, row_number, explode
from pyspark.sql.window import Window

@dp.materialized_view(
    comment="Gold table: Top 10 countries per year by vaccine diversity and total vaccinations"
)
def gold_top10_with_all_vaccines() -> DataFrame:
    """
    Calculate top 10 countries per year based on:
    1. Number of vaccine types used (vaccine_count)
    2. Total vaccinations administered
    
    Business Question: Top 10 countries per year with all vaccines used,
    ordered by vaccine count and total vaccinations
    
    Joins vaccination data with location data to include vaccine information,
    then ranks countries within each year.
    
    Schema:
    - country: country name
    - iso_code: 3-letter country code
    - year: year of vaccination
    - vaccine_count: number of distinct vaccine types used
    - vaccines: comma-separated list of all vaccine names
    - total_vaccinations_in_year: maximum total vaccinations for that year
    - rank: ranking within the year (1-10)
    """
    # Get maximum total vaccinations per country per year
    yearly_totals = (
        spark.read.table("silver_vaccinations")
        .groupBy("country", "iso_code", "year")
        .agg(
            max_("total_vaccinations").alias("total_vaccinations_in_year")
        )
    )
    
    # Join with locations to get vaccine information
    combined = (
        yearly_totals
        .join(
            spark.read.table("silver_locations"),
            on="iso_code",
            how="inner"
        )
        .select(
            col("country"),
            yearly_totals["iso_code"],
            col("year"),
            col("vaccine_count"),
            col("vaccines"),
            col("vaccines_array"),
            col("total_vaccinations_in_year")
        )
    )
    
    # Create window partitioned by year, ordered by vaccine_count desc, then total_vaccinations desc
    window_spec = Window.partitionBy("year").orderBy(
        col("vaccine_count").desc(),
        col("total_vaccinations_in_year").desc()
    )
    
    # Rank countries within each year and filter top 10
    return (
        combined
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") <= 10)
        .select(
            col("country"),
            col("iso_code"),
            col("year"),
            col("vaccine_count"),
            col("vaccines"),
            col("vaccines_array"),
            col("total_vaccinations_in_year"),
            col("rank")
        )
        .orderBy("year", "rank")
    )
