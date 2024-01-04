# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/schemas"

# COMMAND ----------

game_statistics_df = spark.read \
    .format("delta") \
    .load(f"{SILVER_CONTAINER_PATH}/epl/game_statistics/")

team_games_df = spark.read \
    .format("delta") \
    .load(f"{SILVER_CONTAINER_PATH}/epl/team_games/")

fixtures_df = spark.read \
    .format("delta") \
    .load(f"{SILVER_CONTAINER_PATH}/epl/fixtures/")

# COMMAND ----------

# MAGIC %md
# MAGIC #### DROP COLUMNS OF GAME_STATISTICS

# COMMAND ----------

columns_game_statistics_to_drop = [
  'HST', 'AST', 'HC', 'AC', 'B365H', 'B365D', 'B365A', 'B365>2.5', 'B365<2.5',
  'HGD', 'HS', 'HWS', 'HF', 'AGD', 'AS', 'AWS', 'AF', 'PKSTD', 'PKGD', 'PKCD',
  'GDD', 'SD', 'WSD', 'FD', 'avgGoalSH', 'avgGoalSA', 'HR', '>1.5', '>2.5'
  ]
game_statistics_df = game_statistics_df.drop(*columns_game_statistics_to_drop)

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVE DATA TO GOLD CONTAINER

# COMMAND ----------

db_name = "gold_db"
table_name = "team_games"
path_folder = f"{GOLD_CONTAINER_PATH}/epl/{table_name}"

team_games_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", path_folder) \
    .format("delta") \
    .saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

db_name = "gold_db"
table_name = "game_statistics"
path_folder = f"{GOLD_CONTAINER_PATH}/epl/{table_name}"

game_statistics_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", path_folder) \
    .format("delta") \
    .saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

db_name = "gold_db"
table_name = "fixtures"
path_folder = f"{GOLD_CONTAINER_PATH}/epl/{table_name}"

fixtures_df.write \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .option("path", path_folder) \
    .format("delta") \
    .saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL queries to verify the data in the silver container.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_db.fixtures

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_db.game_statistics

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_db.team_games

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_db.poisson_probabilities
