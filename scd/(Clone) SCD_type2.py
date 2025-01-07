# Databricks notebook source
input_file = "customer_data_02.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC #spark session and cred

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder \
    .appName("SCD Type 2 Pipeline - ADLS to Azure SQL") \
    .getOrCreate()

adls_account_name = "septauto"
adls_container_name = "raw"
key = "6TR8QTDWIWj0EshX2YRzMln2dYylTAVUECMoLHE2JPo0SwXt9Kbybqpca96qNTnndDFGB/t4UbTo+AStbQROcg=="

# ADLS file path and credentials
adls_path = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/customer/{input_file}"
spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)


# Azure SQL Server JDBC configuration
jdbc_url = "jdbc:sqlserver://septauto.database.windows.net:1433;database=septbatch"
jdbc_properties = {
    "user": "septadmin",
    "password": "Dharmavaram1@",
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

# COMMAND ----------

# Table names
raw_table = "customer_raw_scd2"
bronze_table = "customer_bronze_scd2"
silver_table = "customer_silver_scd2"

# COMMAND ----------

# MAGIC %md 
# MAGIC #backup table for qa purpose

# COMMAND ----------

silver_df = spark.read.jdbc(url=jdbc_url, table=silver_table, properties=jdbc_properties)
silver_df.write.jdbc(url=jdbc_url, table='customer_silver_scd2_backup', mode="overwrite", properties=jdbc_properties)

# COMMAND ----------

# MAGIC %md
# MAGIC #Raw load

# COMMAND ----------





# Step 1: Read source CSV file from ADLS
source_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("batchid", IntegerType(), True)
])
source_df = spark.read.csv(adls_path, schema=source_schema, header=True)
source_df.display()
source_batchid = source_df.select("batchid").distinct().collect()[0][0]
print("source batch id", source_batchid)

column_list = source_df.columns

source_df = source_df.withColumn("created_date", current_timestamp()).withColumn("updated_date", current_timestamp()).withColumn("hash_key", sha2(concat_ws("||", *[col for col in column_list]), 256))


print("source df")
source_df.display
source_df.write.jdbc(url=jdbc_url, table=raw_table, mode="append", properties=jdbc_properties)




# COMMAND ----------

# MAGIC %md
# MAGIC #Bronze load

# COMMAND ----------


raw_df = spark.read.jdbc(url=jdbc_url, table=raw_table, properties=jdbc_properties)

raw_df = raw_df.filter(raw_df.batchid == source_batchid)


print("raw_df after batchid filter")
raw_df.display()

bronze_df = raw_df.dropDuplicates(["customer_id"])  # Replace "customer_id" with your unique key

# print("bronze_df includes audit columns")
# bronze_df.show()

# Write to Bronze Table
bronze_df.write.jdbc(url=jdbc_url, table=bronze_table, mode="overwrite", properties=jdbc_properties)


# COMMAND ----------

# bronze_df = spark.read.jdbc(url=jdbc_url, table=bronze_table, properties=jdbc_properties)

# bronze_df = bronze_df.withColumn('start_date', current_timestamp()).withColumn('end_date',lit('2099-12-31T23:59:59.999+0000')).withColumn('history_flag',lit(False))

# bronze_df.display()
# bronze_df.write.jdbc(url=jdbc_url, table=silver_table, mode="overwrite", properties=jdbc_properties)

# COMMAND ----------

# MAGIC %md
# MAGIC #Silver load

# COMMAND ----------


bronze_df = spark.read.jdbc(url=jdbc_url, table=bronze_table, properties=jdbc_properties)

silver_df = spark.read.jdbc(url=jdbc_url, table=silver_table, properties=jdbc_properties)

print("bronze df")
bronze_df.display()
print("silver_df")
silver_df.display()


# COMMAND ----------

columns = ['customer_id','name','email','phone','batchid','created_date','updated_date','hash_key','start_date','end_date','history_flag']
updates1 = bronze_df.join(silver_df.select("customer_id", "created_date","batchid"), on="customer_id", how="left_semi").withColumn('start_date', current_timestamp()).withColumn('end_date',lit('2099-12-31T23:59:59')).withColumn('history_flag',lit(False))

print("updates")
updates1.display()

updates2 = silver_df.join(bronze_df.select("customer_id", "created_date","batchid"), on="customer_id", how="left_semi").withColumn('end_date',current_timestamp()).withColumn('history_flag',lit(True))

updates2.display()

updates = updates1.union(updates2)



# COMMAND ----------

silver_not_in_bronze = silver_df.join(bronze_df, on="customer_id", how="left_anti")
print("silver_not_in_bronze")
silver_not_in_bronze.display()



# COMMAND ----------

new_records = bronze_df.join(silver_df, on="customer_id", how="left_anti").withColumn('start_date', current_timestamp()).withColumn('end_date',lit('2099-12-31T23:59:59')).withColumn('history_flag',lit(False))
print("new_records")
new_records.display()



# COMMAND ----------

final_df = updates.select(*columns).union(new_records.select(*columns)).union(silver_not_in_bronze.select(*columns))

final_df.cache()
print("final df")
final_df.display()

# COMMAND ----------

final_df.write.jdbc(url=jdbc_url, table=silver_table, mode="overwrite", properties=jdbc_properties)

# COMMAND ----------

final_df.display()