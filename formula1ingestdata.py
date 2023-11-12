# Databricks notebook source
# MAGIC %run "/Repos/d.saldanha3193@hotmail.com/formula1databricks/configuration&commondfunctions" 

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

###dbutils.widgets.remove("p_file_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

#function to mount and access storage accounts
def mount_adls(storage_account_name, container_name):
    #get secrets
    client_id = dbutils.secrets.get(scope = 'spclientid-scope',key = 'spclientid')
    tenant_id = dbutils.secrets.get(scope = 'tenant-scope',key = 'sptenantid')
    client_secret = dbutils.secrets.get(scope = 'clientsecret-scope',key = 'clientsecret')

    #set configs
    configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": client_id,
           "fs.azure.account.oauth2.client.secret": client_secret,
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #check to see if mount exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    #mount storage
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/"
    mount_point = f"/mnt/{storage_account_name}/{container_name}"
    dbutils.fs.mount(source = source,
    mount_point = mount_point,extra_configs = configs)


# COMMAND ----------

mount_adls('instablobcart','raw')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %fs 
# MAGIC ls /mnt/instablobcart/raw

# COMMAND ----------

# DBTITLE 1,Import CSV filetype -circuits
#specify the schema(optimization technique)
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

circuits_schema = StructType(fields = [StructField('circuitId', IntegerType(), False),
                                       StructField('circuitRef', StringType(), True),
                                       StructField('name', StringType(), True),
                                       StructField('location', StringType(), True),
                                       StructField('country', StringType(), True),
                                       StructField('lat', StringType(), True),
                                       StructField('lng',DoubleType(), True),
                                       StructField('alt',IntegerType(), True),
                                       StructField('url',StringType(),True)])
                                       
circuits_df = spark.read.csv(f"{raw_folder_path}/{v_file_date}/circuits.csv", header=True, schema=circuits_schema)
display(circuits_df)

# COMMAND ----------

#select required columns 
circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")
display(circuits_selected_df)

# COMMAND ----------

#using col
from pyspark.sql.functions import col,lit
circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))
display(circuits_selected_df)

# COMMAND ----------

#rename columns, add file date as column
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source", lit(v_data_source)) \
    .withColumn("file_date", lit(v_file_date))

display(circuits_renamed_df)

# COMMAND ----------

#add ingestion date
from pyspark.sql.functions import current_timestamp, lit
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

#write final df to blob storage in parquet format
#mount processed blob 
mount_adls('instablobcart','processed')

#write to data to datalake as parquet
circuits_final_df.write.mode('overwrite').format("parquet").saveAsTable("f1_processed.circuits")

#check
df = spark.read.parquet(f"{processed_folder_path}/circuits")
display(df)

# COMMAND ----------

# DBTITLE 1,Races file
from pyspark.sql.types import DateType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

#ingest races_file
races_schema = StructType(fields = [StructField('raceId', IntegerType(), False),
                                       StructField('year', IntegerType(), True),
                                       StructField('round', IntegerType(), True),
                                       StructField('circuitId', IntegerType(), True),
                                       StructField('name', StringType(), True),
                                       StructField('date', DateType(), True),
                                       StructField('time',StringType(), True),
                                       StructField('url',StringType(), True)])

races_df = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/{v_file_date}/races.csv")


# COMMAND ----------

#add ingestion date and race_timestamp to the df

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)

#select required columns
races_selected_df = races_with_ingestion_date_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

#write to data to datalake as parquet
races_selected_df.write.mode('overwrite').partitionBy('race_year').format("parquet").saveAsTable("f1_processed.races")

#check
df = spark.read.option("header",True).parquet(f"{processed_folder_path}/races")
display(df)

# COMMAND ----------

# DBTITLE 1,Working with JSON files - constructors
#using ddl type schema
constructors_schema = "constructorId INT, constructorRed STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read \
.schema(constructors_schema).json(f"{raw_folder_path}/{v_file_date}/constructors.json")

#drop columns
constructor_dropped_df = constructor_df.drop('url')


#rename columns and ingestion date column
constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("file_date", lit(v_file_date))

constructor_final_df = add_ingestion_date(constructor_renamed_df)

#write to data to datalake as parquet
constructor_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# DBTITLE 1,Drivers file
#process drivers file
#rename columns and add new columns

name_schema = StructType(fields  = [StructField("forename", StringType(), True),
                                    StructField("surname",StringType(), True)])

drivers_schema = StructType(fields= [StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(),True),
                                     StructField("number", IntegerType(), True),
                            StructField("code", StringType(),True),
                            StructField("name",name_schema),
                            StructField("dob",DateType(),True),
                            StructField("nationality",StringType(),True),
                            StructField("url",StringType(),True)])

#v_file_date
drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")



# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

#concat forename and surname and ingestion date column

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverId", "driver_id")\
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))


drivers_final_df = drivers_with_columns_df.drop(col("url"))

#write to data to datalake as parquet
drivers_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.drivers")


# COMMAND ----------

# DBTITLE 1,Results file
#change p_file_date widget to 2021-03-28, load data from 28 first
#incremental load data from from April 18th next

from pyspark.sql.types import StructType, StructField, IntegerType, StringType,FloatType
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                    StructField("raceId", IntegerType(), True),
                                    StructField("driverId", IntegerType(), True),
                                    StructField("constructorId", IntegerType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("grid", IntegerType(), True),
                                    StructField("position", IntegerType(), True),
                                    StructField("positionText", StringType(), True),
                                    StructField("positionOrder", IntegerType(), True),
                                    StructField("points", FloatType(), True),
                                    StructField("laps", IntegerType(), True),
                                    StructField("time", StringType(), True),
                                    StructField("milliseconds", IntegerType(), True),
                                    StructField("fastestLap", IntegerType(), True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", StringType(), True)])

#v_file_date
results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/{v_file_date}/results.json")

#rename columns and add new columns
results_with_columns_df = results_df.withColumnRenamed("resultId", "result_id") \
                                    .withColumnRenamed("raceId", "race_id") \
                                    .withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("constructorId", "constructor_id") \
                                    .withColumnRenamed("positionText", "position_text") \
                                    .withColumnRenamed("positionOrder", "position_order") \
                                    .withColumnRenamed("fastestLap", "fastest_lap") \
                                    .withColumnRenamed("fastestLapTime", "fastest_lap_time") \
                                    .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed") \
                                    .withColumn("data_source", lit(v_data_source)) \
                                    .withColumn("file_date", lit(v_file_date))

#from configuration notebook
results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))



# COMMAND ----------

#Method1
#loop through list and drop partition before loading data
#for race_id_list in results_final_df.select("race_id").distinct().collect():
#    if (spark._jsparkSession.catalog().tableExists("f1_processed.results")): #check to see if table exits before running below
#        spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")


#results_final_df.write.mode("append").partitionBy('race_id').format("parquet").saveAsTable("f1_processed.results")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Method 2

# COMMAND ----------

#functions defined in configuration file
output_df = re_arrange_partition_column(results_final_df, "race_id")
overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------


overwrite_partition(results_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

# DBTITLE 1,Pitstops file
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])


pit_stops_df = spark.read \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

pitstop_final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

#write to data to datalake as parquet
pitstop_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# DBTITLE 1,Ingest Multiple Files -laptimes
lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])


lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times")   

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))

#write to data to datalake as parquet
final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")


# COMMAND ----------

# DBTITLE 1,Qualifying Files
qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source)) \
.withColumn("file_date", lit(v_file_date))   


#write to data to datalake as parquet
results_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

# MAGIC %sql
# MAGIC --check
# MAGIC select * from f1_processed.qualifying;

# COMMAND ----------


