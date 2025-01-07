from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[0]').getOrCreate()


#from Utility_functions.Common_libraries import * #get_dataset, count_validation,read_file, read_cosmos, read_db, duplicate


from pyspark.sql import SparkSession
import pandas as pd
import json


spark = SparkSession.builder.master("local")\
    .appName("test") \
    .config("spark.jars","/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jarr")\
    .config("spark.driver.extraClassPath","/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jar") \
    .config("spark.executor.extraClassPath","/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jar") \
    .getOrCreate()







url = "jdbc:sqlserver://septauto.database.windows.net:1433;database=sampledb;"
database_name = "septbatch"
username ='sampleadmin'
password='Dharmavaram1@'
database="sampledb"
table_name = "dbo.Persons2"


def read_db():
    mssql_df = spark.read \
        .format("jdbc") \
        .option("url", url) \
        .option("query", source_query) \
        .option("user", username) \
        .option("password", password ) \
        .option("driver",'com.microsoft.sqlserver.jdbc.SQLServerDriver')\
        .load()
    return mssql_df

df = read_db()
