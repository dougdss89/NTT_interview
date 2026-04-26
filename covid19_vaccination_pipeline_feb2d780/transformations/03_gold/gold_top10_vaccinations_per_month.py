"""
Gold Layer - Top 10 Vaccinations Per Month
Business Question: Top 10 countries that had more vaccinations per month and year

Shows monthly vaccination rankings by country.
"""
from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, max as max_, sum as sum_, row_number
from pyspark.sql.window import Window

@dp.materialized_view(
    comment="Gold table: Top 10 countries by total vaccinations per month and year"
)
def gold_top10_vaccinations_per_month() -> DataFrame:
    """
    Calculate top 10 countries by total vaccinations for each month/year.
    
    Business Question: Top 10 countries that had more vaccinations per month and year
    
    Aggregates vaccinations by country, year, and month, then ranks countries
    within each month to identify the top 10.
    
    Schema:
    - country: country name
    - iso_code: 3-letter country code
    - year: year of vaccination
    - month: month of vaccination
    - total_vaccinations_in_month: maximum total_vaccinations for that month
    - rank: ranking within the month (1-10)
    """
    # Aggregate by country, year, and month to get max total vaccinations
    monthly_totals = (
        spark.read.table("silver_vaccinations")
        .groupBy("country", "iso_code", "year", "month")
        .agg(
            # Use max to get the latest cumulative total for the month
            max_("total_vaccinations").alias("total_vaccinations_in_month")
        )
    )
    
    # Create window partitioned by year and month, ordered by vaccinations desc
    window_spec = Window.partitionBy("year", "month").orderBy(col("total_vaccinations_in_month").desc())
    
    # Rank countries within each month and filter top 10
    return (
        monthly_totals
        .withColumn("rank", row_number().over(window_spec))
        .filter(col("rank") <= 10)
        .select(
            col("country"),
            col("iso_code"),
            col("year"),
            col("month"),
            col("total_vaccinations_in_month"),
            col("rank")
        )
        .orderBy("year", "month", "rank")
    )
