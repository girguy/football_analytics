# Databricks notebook source
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, DoubleType
from pyspark.sql.functions import to_date, date_format
from pyspark.sql.functions import col, when , regexp_replace

# COMMAND ----------

# Set up Databricks widgets to input parameters
dbutils.widgets.text("season", "")
SEASON = dbutils.widgets.get("season")

DATE_FORMAT = "dd/MM/yyyy"  # format of the date before the transformation

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

# Load English Premier League games fixtures
# data from the bronze container for the specified season.

tableName =  f"epl_fixtures_{SEASON}"
try:
    epl_fixtures = spark.read \
        .option("header", True) \
        .csv(f"{BRONZE_CONTAINER_PATH}/epl/{SEASON}/{tableName}.csv")
    
    if "_c0" in epl_fixtures.columns:
        epl_fixtures = epl_fixtures.drop("_c0")
    
    for i in range(len(epl_fixtures.columns)):
        col_name = epl_fixtures.columns[i]
        if col_name in FIXTURES_SCHEMA.names:
            data_type = FIXTURES_SCHEMA[i].dataType
            epl_fixtures = epl_fixtures.withColumn(col_name, col(col_name).cast(data_type))

except Exception as e:
    print(f"Error reading data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### TRANSFORM DATAFRAME

# COMMAND ----------

# drop columns

epl_fixtures = epl_fixtures.drop("round number", "Match Number")

# COMMAND ----------

# change name of the columns

epl_fixtures = epl_fixtures \
    .withColumnRenamed("Home Team", "homeTeam") \
    .withColumnRenamed("Away Team", "awayTeam") \
    .withColumnRenamed("Date", "date") \
    .withColumnRenamed("Location", "location") \
    .withColumnRenamed("Result", "played")

# COMMAND ----------

# Transform column "Played"

epl_fixtures = epl_fixtures.withColumn("played", when(col("played").isNull(), False)
                                                .otherwise(True))

# COMMAND ----------

# change date format

initial_date_format = "dd/MM/yyyy HH:mm"  # format of the date before the transformation
# Convert string to timestamp
epl_fixtures = epl_fixtures \
    .withColumn("date", to_date(col("date"), initial_date_format))

# Format timestamp to desired format
epl_fixtures = epl_fixtures \
    .withColumn("date", date_format("date", "EEEE dd MMMM yyyy"))

# COMMAND ----------

epl_fixtures = epl_fixtures \
    .withColumn("homeTeam", regexp_replace("homeTeam", "\'", "")) \
    .withColumn("homeTeam", regexp_replace("homeTeam", "Man Utd", "Man United")) \
    .withColumn("homeTeam", regexp_replace("homeTeam", "Nott'm Forest", "Nottingham Forest")) \
    .withColumn("homeTeam", regexp_replace("homeTeam", "Nottm Forest", "Nottingham Forest")) \
    .withColumn("homeTeam", regexp_replace("homeTeam", "Spurs", "Tottenham")) \
    .withColumn("homeTeam", regexp_replace("homeTeam", "Sheffield Utd", "Sheffield United"))

epl_fixtures = epl_fixtures \
    .withColumn("awayTeam", regexp_replace("awayTeam", "\'", "")) \
    .withColumn("awayTeam", regexp_replace("awayTeam", "Man Utd", "Man United")) \
    .withColumn("awayTeam", regexp_replace("awayTeam", "Nott'm Forest", "Nottingham Forest")) \
    .withColumn("awayTeam", regexp_replace("awayTeam", "Nottm Forest", "Nottingham Forest")) \
    .withColumn("awayTeam", regexp_replace("awayTeam", "Spurs", "Tottenham")) \
    .withColumn("awayTeam", regexp_replace("awayTeam", "Sheffield Utd", "Sheffield United"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### SAVE DATAFRAME

# COMMAND ----------

db_name = "silver_db"
table_name = "fixtures"
path_folder = f"{SILVER_CONTAINER_PATH}/epl/{table_name}"

epl_fixtures.write \
    .mode("overwrite") \
    .option("path", path_folder) \
    .format("delta") \
    .saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### SQL queries to verify the data in the silver container.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_db.fixtures
