# Databricks notebook source
from delta.tables import DeltaTable

def overwrite_table(df, path_folder, partition_column, db_name, table_name):
    df.write \
        .mode("overwrite") \
        .option("path", path_folder) \
        .partitionBy(partition_column) \
        .format("delta") \
        .saveAsTable(f"{db_name}.{table_name}")
    
def merge_and_save_delta_table(df, db_name, table_name, path_folder, merge_condition, partition_column):
    """
    Merges a DataFrame into an existing Delta table or creates a new Delta table if it doesn't exist.

    This function first checks if the specified Delta table exists. If it does, it performs a merge 
    operation using the given conditions. If the table doesn't exist, it creates a new Delta table 
    with the DataFrame.

    Args:
        df (DataFrame): The DataFrame to be merged or written to the Delta table.
        db_name (str): The name of the database where the Delta table is located or will be created.
        table_name (str): The name of the Delta table.
        path_folder (str): The path where the Delta table is stored.
        merge_condition (str): The condition used to match records for the merge operation.
        partition_column (str): The column used to partition the Delta table.

    Note:
        This function uses Spark's dynamic partition pruning feature for efficient data processing.
    """
    spark.conf.set("spark.databricks.optimizer.dynamicPartitionPruning", "true")
    # This configuration allows to dynamically look for a partition during a merge

    # Check if the table exists
    try:
        if (spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
            deltaTable = DeltaTable.forPath(spark, f"{path_folder}")
            deltaTable.alias("tgt").merge(
                df.alias("src"), merge_condition) \
                    .whenMatchedUpdateAll()\
                    .whenNotMatchedInsertAll()\
                    .execute()
        else:
            overwrite_table(df, path_folder, partition_column, db_name, table_name)
    except Exception as e:
        # If an error is thrown, the table likely doesn't exist
        print("Table does not exist. Error:", e)
        print("This table needs to be created")
        overwrite_table(df, path_folder, partition_column, db_name, table_name)
