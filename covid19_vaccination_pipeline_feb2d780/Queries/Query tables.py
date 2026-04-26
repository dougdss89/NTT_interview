# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `workspace`.`default`.`gold_countries_vaccine_types`
# MAGIC ORDER BY vaccine_count DESC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM workspace.default.gold_top10_vaccinations_per_month
# MAGIC LIMIT 10

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,otal_
# MAGIC %sql
# MAGIC SELECT 
# MAGIC   country
# MAGIC   , iso_code
# MAGIC   , year
# MAGIC   , vaccine_count
# MAGIC   , total_vaccinations_in_year
# MAGIC   , rank
# MAGIC   , vaccines
# MAGIC
# MAGIC FROM workspace.default.gold_top10_with_all_vaccines
# MAGIC LIMIT 10
