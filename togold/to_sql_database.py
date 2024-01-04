# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/schemas"

# COMMAND ----------

dbs_user = dbutils.secrets.get(scope = "footanalysis", key = "dbs-user")
dbs_password = dbutils.secrets.get(scope = "footanalysis", key = "dbs-password")
dbs_url = dbutils.secrets.get(scope = "footanalysis", key = "dbs-url")

# COMMAND ----------

game_statistics_df = spark.read \
    .format("delta") \
    .load(f"{GOLD_CONTAINER_PATH}/epl/game_statistics/")

team_games_df = spark.read \
    .format("delta") \
    .load(f"{GOLD_CONTAINER_PATH}/epl/team_games/")

fixtures_df = spark.read \
    .format("delta") \
    .load(f"{GOLD_CONTAINER_PATH}/epl/fixtures/")

probabilities_df = spark.read \
    .format("delta") \
    .load(f"{GOLD_CONTAINER_PATH}/epl/poisson_probabilities/")

# COMMAND ----------

game_statistics_df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", dbs_url) \
    .option("dbtable", "game_statistics") \
    .option("user", dbs_user) \
    .option("password", dbs_password) \
    .option("spark.connection.mode", "databricks") \
    .option("upsert", "true") \
    .save()

# COMMAND ----------

team_games_df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", dbs_url) \
    .option("dbtable", "team_games") \
    .option("user", dbs_user) \
    .option("password", dbs_password) \
    .option("spark.connection.mode", "databricks") \
    .option("upsert", "true") \
    .save()

# COMMAND ----------

fixtures_df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", dbs_url) \
    .option("dbtable", "fixtues") \
    .option("user", dbs_user) \
    .option("password", dbs_password) \
    .option("spark.connection.mode", "databricks") \
    .option("upsert", "true") \
    .save()

# COMMAND ----------

probabilities_df.write \
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", dbs_url) \
    .option("dbtable", "poisson_probabilities") \
    .option("user", dbs_user) \
    .option("password", dbs_password) \
    .option("spark.connection.mode", "databricks") \
    .option("upsert", "true") \
    .save()
