# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.functions import sum, when

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://bronze@vehicletheftdatastorage.blob.core.windows.net/",
    mount_point = "/mnt/bronze",
    extra_configs = {
      "fs.azure.account.key.vehicletheftdatastorage.blob.core.windows.net": dbutils.secrets.get("dbscope","keyvaultforstorageaccount")
    }
)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### check whether mount point exist 

# COMMAND ----------

mount_point = "/mnt/bronze"

# Check if the mount point exists
if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
    print(f"Directory has been mounted: {mount_point}")

else:
    print(f"Directory not mounted: {mount_point}")

# COMMAND ----------

# MAGIC %md
# MAGIC ####### to unmount the mount_point: dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

dbutils.fs.unmount("/mnt/bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ######## Mounted the blob storage container named - bronze from storage acc to databricks file . path : /mnt/bronze

# COMMAND ----------

dbutils.fs.ls("/mnt/bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ###### so, files from bronze in storage container - mounted in bronze in databricks

# COMMAND ----------

# MAGIC %md
# MAGIC ######likewise moving silver and gold layer files to databricks

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://silver@vehicletheftdatastorage.blob.core.windows.net/",
    mount_point = "/mnt/silver",
    extra_configs = {
      "fs.azure.account.key.vehicletheftdatastorage.blob.core.windows.net": dbutils.secrets.get("dbscope","keyvaultforstorageaccount")
    }
)

# COMMAND ----------

dbutils.fs.mount(
    source = "wasbs://gold@vehicletheftdatastorage.blob.core.windows.net/",
    mount_point = "/mnt/gold",
    extra_configs = {
      "fs.azure.account.key.vehicletheftdatastorage.blob.core.windows.net": dbutils.secrets.get("dbscope","keyvaultforstorageaccount")
    }
)

# COMMAND ----------

dbutils.fs.ls("/mnt/silver")

# COMMAND ----------

dbutils.fs.ls("/mnt/gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### load data into dataframe

# COMMAND ----------

location_df = spark.read.format("csv").option("header","true").option("inferschema","true").load("/mnt/bronze/locations.csv")

# COMMAND ----------

make_details_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/bronze/make_details.csv")
stolen_vehicles_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/bronze/stolen_vehicles.csv")
data_dictionary_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/bronze/data_dictionary.csv")

# COMMAND ----------

location_df.show(5)

# COMMAND ----------

location_df.printSchema()

# COMMAND ----------

make_details_df.show(5)


# COMMAND ----------

make_details_df.printSchema()

# COMMAND ----------

stolen_vehicles_df.show(5)


# COMMAND ----------

stolen_vehicles_df.printSchema()

# COMMAND ----------

data_dictionary_df.show(5)

# COMMAND ----------

data_dictionary_df.printSchema()

# COMMAND ----------

location_df.show(1)

# COMMAND ----------

location_df.printSchema()

# COMMAND ----------

# DBTITLE 1,changing population column as integer by replacing "," by ""
location_df = location_df.withColumn("population",regexp_replace(location_df["population"],",","").cast("integer"))


# COMMAND ----------

location_df.show(1)
location_df.printSchema()

# COMMAND ----------

# DBTITLE 1,changing title in standard format
location_df = location_df.toDF(*[Column.lower().replace(" ", "_") for Column in location_df.columns])

# COMMAND ----------

location_df.show(1)
location_df.printSchema()

# COMMAND ----------

data_dictionary_df.show(1)

# COMMAND ----------

data_dictionary_df = data_dictionary_df.withColumnRenamed("Description","Description Data")


# COMMAND ----------

data_dictionary_df.show(1)

# COMMAND ----------

data_dictionary_df = data_dictionary_df.toDF(*[Column.lower().replace(" ","_") for Column in data_dictionary_df.columns])

# COMMAND ----------

data_dictionary_df.show(1)

# COMMAND ----------

# DBTITLE 1,writing in python code
# data_dictionary_df = data_dictionary_df.toDF(*[col.lower().replace(" ","_") for col in data_dictionary_df.columns])




column = data_dictionary_df.columns
new_column = []

for col in column:
    clean_column = col.replace(" ", "_").lower()
    new_column.append(clean_column)

data_dictionary_df = data_dictionary_df.toDF(*new_column)
data_dictionary_df.show(1)

# COMMAND ----------

make_details_df.show(5)

# COMMAND ----------

make_details_df = make_details_df.toDF(*[Column.lower().replace(" ", "_") for Column in make_details_df.columns])

# COMMAND ----------

make_details_df.show(1)

# COMMAND ----------

make_details_df.printSchema()

# COMMAND ----------

stolen_vehicles_df.show(2)

# COMMAND ----------

stolen_vehicles_df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/mnt/bronze/stolen_vehicles.csv")

# COMMAND ----------

stolen_vehicles_df = stolen_vehicles_df.toDF(*[Column.lower().replace(" ", "_") for Column in stolen_vehicles_df.columns])

# COMMAND ----------

stolen_vehicles_df = stolen_vehicles_df.withColumnRenamed("vhicle_id", "vehicle_id")

# COMMAND ----------

stolen_vehicles_df.show(1)

# COMMAND ----------

stolen_vehicles_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###### moving or writing files from bronze to silver layer, overwriting enabled

# COMMAND ----------

location_df.write.mode("overwrite").option("header", "true").option("inferschema","true").csv("/mnt/silver/location.csv")
make_details_df.write.mode("overwrite").option("header", "true").option("inferschema","true").csv("/mnt/silver/make_details.csv")
stolen_vehicles_df.write.mode("overwrite").option("header","true").option("inferschema","true").csv("/mnt/silver/stolen_vehicles.csv")
data_dictionary_df.write.option("header","true").option("inferschema","true").mode("overwrite").csv("/mnt/silver/data_dictionary.csv")

# COMMAND ----------

dbutils.fs.ls("/mnt/silver")

# COMMAND ----------

stolen_vehicles_silver = spark.read.format("csv").option("header","true").load("/mnt/silver/stolen_vehicles.csv")

# COMMAND ----------

stolen_vehicles_silver.show(1)

# COMMAND ----------

location_silver = spark.read.format("csv").option("header","true").load("/mnt/silver/location.csv")
make_details_silver = spark.read.format("csv").option("header","true").load("/mnt/silver/make_details.csv")
stolen_vehicles_silver = spark.read.format("csv").option("header","true").load("/mnt/silver/stolen_vehicles.csv")
data_dictionary_silver = spark.read.format("csv").option("header","true").load("/mnt/silver/data_dictionary.csv")


# COMMAND ----------

location_silver.show(10)

# COMMAND ----------

nullcount_location = location_silver.select([sum(when(col(column).isNull(), 1).otherwise(0)).alias(column) for column in location_silver.columns])

# COMMAND ----------

nullcount_location.show()

# COMMAND ----------

nullcount_makedetails = make_details_silver.select([sum(when(col(colk).isNull(), 1).otherwise (0)).alias (colk) for colk in make_details_silver.columns])
nullcount_stolen_vehicles = stolen_vehicles_silver.select([sum(when(col(colk).isNull(), 1).otherwise (0)).alias (colk) for colk in stolen_vehicles_silver.columns])
nullcount_data_dictionary = data_dictionary_silver.select([sum(when(col(colk).isNull(), 1).otherwise (0)).alias (colk) for colk in data_dictionary_silver.columns])

# COMMAND ----------

nullcount_makedetails.show()

# COMMAND ----------

nullcount_data_dictionary.show()

# COMMAND ----------

nullcount_stolen_vehicles.show()

# COMMAND ----------

stolen_vehicles_silver.printSchema()

# COMMAND ----------

stolen_vehicles_silver = stolen_vehicles_silver.fillna(
    {
        "vehicle_type": "no data",
        "make_id":0,
        "model_year":0,
        "vehicle_desc":"no data",
        "color":"no data"
    }
)

# COMMAND ----------

stolen_vehicles_silver.tail(10)

# COMMAND ----------

# DBTITLE 1,rerun isnull  -after filling null columns to get latest value
nullcount_stolen_vehicles = stolen_vehicles_silver.select([sum(when(col(colk).isNull(), 1).otherwise (0)).alias (colk) for colk in stolen_vehicles_silver.columns])
nullcount_stolen_vehicles.show()


# COMMAND ----------

dbutils.fs.ls('/mnt/silver/make_details.csv')

# COMMAND ----------

location_silver.write.mode("overwrite").option("header", "true").option("inferschema","true").csv("/mnt/gold/location.csv")
make_details_silver.write.mode("overwrite").option("header", "true").option("inferschema","true").csv("/mnt/gold/make_details.csv")
stolen_vehicles_silver.write.mode("overwrite").option("header","true").option("inferschema","true").csv("/mnt/gold/stolen_vehicles.csv")
data_dictionary_silver.write.option("header","true").option("inferschema","true").mode("overwrite").csv("/mnt/gold/data_dictionary.csv")

# COMMAND ----------

dbutils.fs.ls("/mnt/gold")

# COMMAND ----------

location_silver.createOrReplaceTempView("location")
make_details_silver.createOrReplaceTempView("make_details")
stolen_vehicles_silver.createOrReplaceTempView("stolen_vehicles")
data_dictionary_silver.createOrReplaceTempView("data_dictionary")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT model_year,count(*) AS num_of_vehicle_Stolen 
# MAGIC FROM stolen_vehicles
# MAGIC GROUP BY model_year
# MAGIC ORDER BY model_year DESC

# COMMAND ----------

location_silver.show()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT region, population FROM location
# MAGIC WHERE population>500000
# MAGIC ORDER BY region DESC
