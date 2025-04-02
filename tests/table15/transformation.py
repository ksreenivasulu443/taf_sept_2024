from gitdb.fun import delta_types
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, sha2, concat_ws, date_format
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Initialize Spark session
spark = SparkSession.builder.master("local[4]") \
        .appName("pytest_framework") \
        .config("spark.jars", "/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jar") \
        .config("spark.driver.extraClassPath", "/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jar") \
        .config("spark.executor.extraClassPath", "/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jar") \
        .getOrCreate()

adls_account_name = "septauto"
adls_container_name = "raw"
key = "6TR8QTDWIWj0EshX2YRzMln2dYylTAVUECMoLHE2JPo0SwXt9Kbybqpca96qNTnndDFGB/t4UbTo+AStbQROcg=="
input_file = "customer_data_02.csv"
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


# delta - target also containing delta(1)
#         you need to make previously loaded data is not touched

bronze_df = spark.read.jdbc(url=jdbc_url, table='customer_bronze', properties=jdbc_properties)

silver_df = spark.read.jdbc(url=jdbc_url, table='customer_silver_backup', properties=jdbc_properties)

print("bronze df")
# bronze_df.display()
print("silver_df")
# silver_df.display()

columns = ['customer_id','name','email','phone','batchid','created_date','updated_date','hash_key']
updates = bronze_df.join(silver_df.select("customer_id", "created_date","batchid"), on="customer_id", how="inner").drop(bronze_df.created_date,bronze_df.batchid)

print("updates")
# updates.display()

silver_not_in_bronze = silver_df.join(bronze_df, on="customer_id", how="left_anti")
print("silver_not_in_bronze")
# silver_not_in_bronze.display()

new_records = bronze_df.join(silver_df, on="customer_id", how="left_anti")
print("new_records")
# new_records.display()

final_df = updates.select(*columns).union(new_records.select(*columns)).union(silver_not_in_bronze.select(*columns))

final_df.cache()
print("final df")
final_df.show()

final_df.write.jdbc(url=jdbc_url, table='customer_silver_expected', mode="overwrite", properties=jdbc_properties)