# Databricks notebook source
# MAGIC %run "/Repos/d.saldanha3193@hotmail.com/formula1databricks/configuration" 

# COMMAND ----------

dbutils.widgets.text("p_data_source","")

v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

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
                                       
circuits_df = spark.read.csv("dbfs:/mnt/instablobcart/raw/circuits.csv", header=True, schema=circuits_schema)
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

#rename columns
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId","circuit_id") \
    .withColumnRenamed("circuitRef","circuit_ref") \
    .withColumnRenamed("lat","latitude") \
    .withColumnRenamed("lng","longitude") \
    .withColumnRenamed("alt","altitude") \
    .withColumn("data_source", lit(v_data_source))

display(circuits_renamed_df)

# COMMAND ----------

#add ingestion date
from pyspark.sql.functions import current_timestamp, lit
circuits_final_df = add_ingestion_date(circuits_renamed_df)

# COMMAND ----------

#write final df to blob storage in parquet format
#mount processed blob 
mount_adls('instablobcart','processed')

circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

#check
df = spark.read.parquet(f"{processed_folder_path}/circuits")

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

races_df = spark.read.option("header",True).schema(races_schema).csv(f"{raw_folder_path}/races.csv")


# COMMAND ----------

#add ingestion date and race_timestamp to the df

races_with_timestamp_df = races_df.withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss')) \
.withColumn("data_source", lit(v_data_source))

races_with_ingestion_date_df = add_ingestion_date(races_with_timestamp_df)

#select required columns
races_selected_df = races_with_ingestion_date_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

#write to blob
races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f'{processed_folder_path}/races')

#check
df = spark.read.option("header",True).parquet(f"{processed_folder_path}/races")
display(df)

# COMMAND ----------

processed_folder_path

# COMMAND ----------

# DBTITLE 1,Working with JSON files - constructors
#using ddl type schema
constructors_schema = "constructorId INT, constructorRed STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read \
.schema(constructors_schema).json(f"{raw_folder_path}/constructors.json")

#drop columns
constructor_dropped_df = constructor_df.drop('url')


#rename columns and ingestion date column
constructor_renamed_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("data_source", lit(v_data_source))

constructor_final_df = add_ingestion_date(constructor_renamed_df)

#write data to parquet, save to blob
constructor_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/constructors")

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

drivers_df = spark.read.schema(drivers_schema).json(f"{raw_folder_path}/drivers.json")



# COMMAND ----------

drivers_df.printSchema()

# COMMAND ----------

#concat forename and surname and ingestion date column

drivers_with_ingestion_date_df = add_ingestion_date(drivers_df)

drivers_with_columns_df = drivers_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                    .withColumnRenamed("driverRef", "driver_ref") \
                                    .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                    .withColumn("data_source", lit(v_data_source))


drivers_final_df = drivers_with_ingestion_date_df.drop(col("url"))

drivers_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/drivers")

# COMMAND ----------

# DBTITLE 1,Results file
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

results_df = spark.read.schema(results_schema).json(f"{raw_folder_path}/results.json")

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
                                    .withColumn("data_source", lit(v_data_source))

results_with_ingestion_date_df = add_ingestion_date(results_with_columns_df)

results_final_df = results_with_ingestion_date_df.drop(col("statusId"))

results_final_df.write.mode("overwrite").partitionBy('race_id').parquet(f"{processed_folder_path}/results")

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
.json(f"{raw_folder_path}/pit_stops.json")

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/pit_stops")

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
.csv(f"{raw_folder_path}/lap_times")   

lap_times_with_ingestion_date_df = add_ingestion_date(lap_times_df)

final_df = lap_times_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))

final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/lap_times")

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
.json(f"{raw_folder_path}/qualifying")

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumnRenamed("constructorId", "constructor_id") \
.withColumn("ingestion_date", current_timestamp()) \
.withColumn("data_source", lit(v_data_source))   


final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/qualifying")

# COMMAND ----------


