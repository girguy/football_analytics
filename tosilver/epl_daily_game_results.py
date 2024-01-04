# Databricks notebook source
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, to_date
from pyspark.sql.functions import regexp_replace
from PremierLeague import PremierLeague
import pandas as pd
from datetime import datetime

# COMMAND ----------

# Function to check the date format
def check_date_format(date_str):
    try:
        datetime.strptime(date_str, '%d/%m/%y')
        return True
    except ValueError:
        return False

def transform_date_column(df):
    # Check if all dates are in 'dd/MM/yy' format
    if df['Date'].apply(check_date_format).all():
        df['Date'] = pd.to_datetime(
            df['Date'], format='%d/%m/%y').dt.strftime('%d/%m/%Y')
    else:
        print("Not all dates are in the 'dd/MM/yy' format")
    return df

# COMMAND ----------

# Set up Databricks widgets to input parameters
dbutils.widgets.text("season", "")
SEASON = dbutils.widgets.get("season")

PAST_GAMES = 4
ID_FIRST_GAME = 1

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/schemas"

# COMMAND ----------

# MAGIC %md
# MAGIC ### LOAD EPL GAMES FROM BRONZE CONTAINER

# COMMAND ----------

# Load English Premier League games data from the
# bronze container for the specified season.
tableName =  f"epl_{SEASON}"
try:
    epl_games = spark.read \
        .option("header", True) \
        .csv(f"{BRONZE_CONTAINER_PATH}/epl/{SEASON}/{tableName}.csv")
    
    if "_c0" in epl_games.columns:
        epl_games = epl_games.drop("_c0")
    
    col_to_delete = []
    for i in range(len(epl_games.columns)):
        col_name = epl_games.columns[i]
        if col_name in DAILY_GAME_SCHEMA.names:
            data_type = DAILY_GAME_SCHEMA[i].dataType
            epl_games = epl_games.withColumn(col_name, col(col_name).cast(data_type))
        else: 
            col_to_delete.append(col_name) 
    epl_games = epl_games.drop(*col_to_delete)
    epl_games = epl_games.toPandas()

    # transform date format if necessary
    epl_games = transform_date_column(epl_games)
    
    # Adjust types in Pandas if necessary
    epl_games['FTHG'] = epl_games['FTHG'].astype('int32')
    epl_games['FTAG'] = epl_games['FTAG'].astype('int32')
except Exception as e:
    print(f"Error reading data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### CREATE 'GAME_STATISTICS' AND 'TEAM_GAMES' DATAFRAMES

# COMMAND ----------

# Initialize PremierLeague class with past games, season,
# and first game ID, then create datasets.
premier_league = PremierLeague(PAST_GAMES, SEASON, ID_FIRST_GAME)

premier_league.create_dataset(epl_games)
game_statistics_pd = premier_league._dataset

premier_league.create_teams_dataset(SEASON)
team_games_pd = premier_league._teams_dataset

# COMMAND ----------

# MAGIC %md
# MAGIC #### TRANSFORM INTO SPARK DATAFRAME

# COMMAND ----------

# Convert Pandas DataFrames to Spark DataFrames with the defined schemas.
game_statistics_df = spark.createDataFrame(
    game_statistics_pd, schema=GAME_STATISTICS_SCHEMA
    )
team_games_df = spark.createDataFrame(
    team_games_pd, schema=TEAM_GAMES_SCHEMA
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### PROCESS DATE COLUMN

# COMMAND ----------

date_format = "dd/MM/yyyy"  # format of the date before the transformation

# Process date columns in the DataFrames to ensure correct format.
team_games_df = team_games_df.withColumn(
    "date", to_date(col("date"), date_format)
    )
game_statistics_df = game_statistics_df.withColumn(
    "date", to_date(col("date"), date_format)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC #### PROCESS TEAM NAMES

# COMMAND ----------

team_games_df = team_games_df \
    .withColumn("team", regexp_replace("team", "\'", "")) \
    .withColumn("team", regexp_replace("team", "Man Utd", "Man United")) \
    .withColumn("team", regexp_replace("team", "Nott'm Forest", "Nottingham Forest")) \
    .withColumn("team", regexp_replace("team", "Nottm Forest", "Nottingham Forest"))
team_games_df = team_games_df \
    .withColumn("team", regexp_replace("team", "\'", "")) \
    .withColumn("team", regexp_replace("team", "Man Utd", "Man United")) \
    .withColumn("team", regexp_replace("team", "Nott'm Forest", "Nottingham Forest")) \
    .withColumn("team", regexp_replace("team", "Nottm Forest", "Nottingham Forest"))

game_statistics_df = game_statistics_df \
    .withColumn("homeTeam", regexp_replace("homeTeam", "\'", "")) \
    .withColumn("homeTeam", regexp_replace("homeTeam", "Man Utd", "Man United")) \
    .withColumn("homeTeam", regexp_replace("homeTeam", "Nott'm Forest", "Nottingham Forest")) \
    .withColumn("homeTeam", regexp_replace("homeTeam", "Nottm Forest", "Nottingham Forest"))
game_statistics_df = game_statistics_df \
    .withColumn("awayTeam", regexp_replace("awayTeam", "\'", "")) \
    .withColumn("awayTeam", regexp_replace("awayTeam", "Man Utd", "Man United")) \
    .withColumn("awayTeam", regexp_replace("awayTeam", "Nott'm Forest", "Nottingham Forest")) \
    .withColumn("awayTeam", regexp_replace("awayTeam", "Nottm Forest", "Nottingham Forest"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### SAVE DATASET AND EPL_GAMES

# COMMAND ----------

# Save processed datasets into the silver container
# with appropriate merge conditions and partitioning.
partition_column = "season"
db_name = "silver_db"
merge_condition = "tgt.matchDay = src.matchDay AND tgt.season = src.season AND tgt.team = src.team"
table_name = "team_games"
path_folder = f"{SILVER_CONTAINER_PATH}/epl/{table_name}"
merge_and_save_delta_table(team_games_df, db_name, table_name,
                           path_folder, merge_condition, partition_column
                           )

# COMMAND ----------

merge_condition = "tgt.gameNumber = src.gameNumber AND tgt.season = src.season"
table_name = "game_statistics"
path_folder = f"{SILVER_CONTAINER_PATH}/epl/{table_name}"
merge_and_save_delta_table(game_statistics_df, db_name, table_name,
                           path_folder, merge_condition, partition_column
                           )

# COMMAND ----------

# MAGIC %md
# MAGIC #### SQL queries to verify the data in the silver container.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_db.team_games

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_db.game_statistics
