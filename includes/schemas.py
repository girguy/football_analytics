# Databricks notebook source
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import IntegerType, StringType, DoubleType

# COMMAND ----------

# Define the schema for the daily game data. This schema
# will be used to read and validate the CSV data.
# The fields are defined with their respective data
# types and nullability.
DAILY_GAME_SCHEMA = StructType(
    fields=[StructField("Div", StringType(), False),
            StructField("Date", StringType(), True),
            StructField("Time", StringType(), True),
            StructField("HomeTeam", StringType(), True),
            StructField("AwayTeam", StringType(), True),
            StructField("FTHG", IntegerType(), True),
            StructField("FTAG", IntegerType(), True),
            StructField("FTR", StringType(), True),
            StructField("HTHG", StringType(), True),
            StructField("HTAG", IntegerType(), True),
            StructField("HTR", StringType(), True),
            StructField("Referee", StringType(), True),
            StructField("HS", IntegerType(), True),
            StructField("AS", IntegerType(), True),
            StructField("HST", IntegerType(), True),
            StructField("AST", IntegerType(), True),
            StructField("HF", IntegerType(), True),
            StructField("AF", IntegerType(), True),
            StructField("HC", IntegerType(), True),
            StructField("AC", IntegerType(), True)
            ])

# COMMAND ----------

# Define the schema for team games. This schema represents
# the structure of team game data.
TEAM_GAMES_SCHEMA = StructType(
    fields=[StructField("date", StringType(), False),
            StructField("season", StringType(), False),
            StructField("gameNumber", IntegerType(), False),
            StructField("matchDay", IntegerType(), True),
            StructField("RES", IntegerType(), True),
            StructField("PTS", IntegerType(), True),
            StructField("GS", IntegerType(), True),
            StructField("GSCUM", IntegerType(), True),
            StructField("GC", IntegerType(), True),
            StructField("GCCUM", IntegerType(), True),
            StructField("GD", IntegerType(), True),
            StructField("ST", IntegerType(), True),
            StructField("C", IntegerType(), True),
            StructField("WP", StringType(), True),
            StructField("FORM", IntegerType(), True),
            StructField("team", StringType(), True)
            ])

# COMMAND ----------

# Define the schema for game statistics.
# This schema represents detailed statistics of games.
GAME_STATISTICS_SCHEMA = StructType(
    fields=[StructField("date", StringType(), False),
            StructField("season", StringType(), False),
            StructField("gameNumber", IntegerType(), False),
            StructField("homeTeam", StringType(), True),
            StructField("awayTeam", StringType(), True),
            StructField("FTHG", IntegerType(), True),
            StructField("FTAG", IntegerType(), True),
            StructField("HST", IntegerType(), True),
            StructField("AST", IntegerType(), True),
            StructField("HC", IntegerType(), True),
            StructField("AC", IntegerType(), True),
            StructField("HPKST", DoubleType(), True),
            StructField("HPKGS", DoubleType(), True),
            StructField("HPKGC", DoubleType(), True),
            StructField("HPKC", DoubleType(), True),
            StructField("HGD", DoubleType(), True),
            StructField("HS", DoubleType(), True),
            StructField("HWS", DoubleType(), True),
            StructField("HF", DoubleType(), True),
            StructField("APKST", DoubleType(), True),
            StructField("APKGS", DoubleType(), True),
            StructField("APKGC", DoubleType(), True),
            StructField("APKC", DoubleType(), True),
            StructField("AGD", DoubleType(), True),
            StructField("AS", DoubleType(), True),
            StructField("AWS", DoubleType(), True),
            StructField("AF", DoubleType(), True),
            StructField("PKSTD", DoubleType(), True),
            StructField("PKGD", DoubleType(), True),
            StructField("PKCD", DoubleType(), True),
            StructField("GDD", DoubleType(), True),
            StructField("SD", DoubleType(), True),
            StructField("WSD", DoubleType(), True),
            StructField("FD", DoubleType(), True),
            StructField("avgGoalSH", DoubleType(), True),
            StructField("avgGoalSA", DoubleType(), True),
            StructField("avgGoalSHH", DoubleType(), True),
            StructField("avgGoalCHH", DoubleType(), True),
            StructField("avgGoalSAA", DoubleType(), True),
            StructField("avgGoalCAA", DoubleType(), True),
            StructField("HR", DoubleType(), True),
            StructField(">1.5", DoubleType(), True),
            StructField(">2.5", DoubleType(), True)
            ])

# COMMAND ----------

# Define the schema for the daily game data. This schema
# will be used to read and validate the CSV data.
# The fields are defined with their respective data
# types and nullability.
FIXTURES_SCHEMA = StructType(
    fields=[StructField("Match Number", IntegerType(), False),
            StructField("Round Number", IntegerType(), True),
            StructField("Date", StringType(), True),
            StructField("Location", StringType(), True),
            StructField("Home Team", StringType(), True),
            StructField("Away Team", StringType(), True),
            StructField("Result", StringType(), True)
            ])

# COMMAND ----------

POISSON_PROBABILITIES_SCHEMA = StructType(
    fields=[StructField("homeTeam", StringType(), False),
            StructField("awayTeam", StringType(), True),
            StructField("Win", DoubleType(), True),
            StructField("Loose", DoubleType(), True),
            StructField("Draw", DoubleType(), True),
            StructField("BothScore", DoubleType(), True),
            StructField(">1.5", DoubleType(), True),
            StructField(">2.5", DoubleType(), True),
            StructField(">3.5", DoubleType(), True),
            StructField("Season", StringType(), True),
            StructField("hTAttStrength", DoubleType(), True),
            StructField("hTDefStrength", DoubleType(), True),
            StructField("aTAttStrength", DoubleType(), True),
            StructField("aTDefStrength", DoubleType(), True)
            ])
