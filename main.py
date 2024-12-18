# # import dbutils
#
# from pyspark.sql import SparkSession
# import logging
# import os
# from pyspark.sql.functions import col
#
# # Initialize logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
#
# # Initialize Spark session
# snow_jar = '/Users/admin/PycharmProjects/test_automation_project/jar/snowflake-jdbc-3.14.3.jar'
# postgres_jar = '/Users/admin/PycharmProjects/test_automation_project/jar/postgresql-42.2.5.jar'
# azure_storage = '/Users/admin/PycharmProjects/test_automation_project/jar/azure-storage-8.6.6.jar'
# hadoop_azure = '/Users/admin/PycharmProjects/test_automation_project/jar/hadoop-azure-3.3.1.jar'
# jar_path = snow_jar + ',' + postgres_jar + ','+azure_storage + ',' + hadoop_azure
#
#
# # Spark Session
# spark = SparkSession.builder.master("local[1]") \
#     .appName("test") \
#     .config("spark.jars", jar_path) \
#     .config("spark.driver.extraClassPath", jar_path) \
#     .config("spark.executor.extraClassPath", jar_path) \
#     .getOrCreate()
#
# # ADLS account information
# adls_account_name = "septauto"  # Your ADLS account name
# adls_container_name = "project1"  # Your container name
# key = "6TR8QTDWIWj0EshX2YRzMln2dYylTAVUECMoLHE2JPo0SwXt9Kbybqpca96qNTnndDFGB/t4UbTo+AStbQROcg=="  # Your Account Key
#
# # Set Spark configuration for ADLS Gen2 using SharedKey authentication
# spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
# spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)
#
#
# # Set the filesystem path
# adls_file_system_url = f"abfss://{adls_container_name}@{adls_account_name}.dfs.core.windows.net/test/*Contact*.csv"
#
#
#
# df = spark.read.csv(adls_file_system_url, header=True,inferSchema=True)
#
# df.show()