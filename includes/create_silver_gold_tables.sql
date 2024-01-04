-- Databricks notebook source
DROP DATABASE IF EXISTS silver_db CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS silver_db
LOCATION "/mnt/storagefootanalysis/silver";

-- COMMAND ----------

DROP DATABASE IF EXISTS gold_db CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS gold_db
LOCATION "/mnt/storagefootanalysis/gold";
