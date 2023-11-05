# Databricks notebook source
#credentials
client_id = dbutils.secrets.get(scope = 'spclientid-scope',key = 'spclientid')
tenant_id = dbutils.secrets.get(scope = 'tenant-scope',key = 'sptenantid')
client_secret = dbutils.secrets.get(scope = 'clientsecret-scope',key = 'clientsecret')


spark.conf.set("fs.azure.account.auth.type.instablobcart.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.instablobcart.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.instablobcart.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.instablobcart.dfs.core.windows.net",client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.instablobcart.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls(f"abfss://demo@instablobcart.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@instablobcart.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------


