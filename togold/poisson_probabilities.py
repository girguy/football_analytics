# Databricks notebook source
import pandas as pd
from pyspark.sql.functions import col
from PoissonDistribution import PoissonDistribution

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/schemas"

# COMMAND ----------

fixtures_df = spark.read \
    .format("delta") \
    .load(f"{SILVER_CONTAINER_PATH}/epl/fixtures/") \
    .toPandas()

game_statistics_df = spark.read \
    .format("delta") \
    .load(f"{SILVER_CONTAINER_PATH}/epl/game_statistics/") \
    .filter((col("season") == 2023) | (col("season") == 2024)) \
    .toPandas()

team_games_df = spark.read \
    .format("delta") \
    .load(f"{SILVER_CONTAINER_PATH}/epl/team_games/") \
    .filter((col("season") == 2023) | (col("season") == 2024)) \
    .toPandas()

# COMMAND ----------

poisson_prob = PoissonDistribution(game_statistics_df, team_games_df, fixtures_df, 2023, 2024, 10)
results = poisson_prob.get_poisson_probabilities()

# COMMAND ----------

poisson_probabilities_df = spark.createDataFrame(
    results, schema=POISSON_PROBABILITIES_SCHEMA
    )

# COMMAND ----------

poisson_probabilities_df = poisson_probabilities_df \
    .withColumnRenamed('Win', 'win') \
    .withColumnRenamed('Loose', 'loose') \
    .withColumnRenamed('Draw', 'draw') \
    .withColumnRenamed('BothScore', 'bothScore') \
    .withColumnRenamed('Season', 'season')

# COMMAND ----------

poisson_probabilities_df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVE DATASET AND EPL_GAMES

# COMMAND ----------

db_name = "gold_db"
table_name = "poisson_probabilities"
path_folder = f"{GOLD_CONTAINER_PATH}/epl/{table_name}"

poisson_probabilities_df.write \
    .mode("overwrite") \
    .option("path", path_folder) \
    .format("delta") \
    .saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL queries to verify the data in the silver container.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_db.poisson_probabilities
