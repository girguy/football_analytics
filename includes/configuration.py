# Databricks notebook source
dbutils.widgets.text("storage_account", "storagefootanalysis")
STORAGE_ACCOUT_NAME = dbutils.widgets.get("storage_account")

# COMMAND ----------

BRONZE_CONTAINER_PATH = f"/mnt/{STORAGE_ACCOUT_NAME}/bronze"
SILVER_CONTAINER_PATH = f"/mnt/{STORAGE_ACCOUT_NAME}/silver"
GOLD_CONTAINER_PATH = f"/mnt/{STORAGE_ACCOUT_NAME}/gold"
