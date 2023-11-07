-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "/mnt/instablobcart/processed"

-- COMMAND ----------

DESC DATABASE f1_processed;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation
LOCATION "/mnt/instablobcart/presentation"

-- COMMAND ----------

DESC DATABASE f1_presentation;

-- COMMAND ----------


