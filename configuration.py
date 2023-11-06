# Databricks notebook source
raw_folder_path = '/mnt/instablobcart/raw'
processed_folder_path = '/mnt/instablobcart/processed'
presentation_folder_path = '/mnt/instablobcart/presentation'

# COMMAND ----------

#define common functions
from pyspark.sql.functions import current_timestamp

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date",current_timestamp())
    return output_df
