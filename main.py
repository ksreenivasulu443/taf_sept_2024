from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when
from src.utility.report_lib import write_output
from pyspark.sql.types import  StructType
import json


spark = SparkSession.builder.master('local[1]').getOrCreate()

source = spark.read.csv("/Users/admin/PycharmProjects/taf/input_files/Contact_info_s.csv",inferSchema=True, header=True)
# target =spark.read.csv("/Users/admin/PycharmProjects/taf/input_files/Contact_info_t.csv", header=True)
# key_column = ["identifier"]
source.show(2)
source_schema = source.schema

source_schema_df = spark.createDataFrame(
        [(field.name.lower(), field.dataType.simpleString()) for field in source_schema],
        ["col_name", "source_data_type"]
    )

source_schema_df.show()

# def read_schema():
#     schema_path = "/Users/admin/PycharmProjects/taf/tests/table7/schema.json"
#     with open(schema_path, 'r') as schema_file:
#         schema = StructType.fromJson(json.load(schema_file))
#         print(schema)
#     return schema
#
# schema = read_schema()
#
# source2 = spark.read.schema(schema).csv("/Users/admin/PycharmProjects/taf/input_files/Contact_info_s.csv", header=True)
# source2.show(2)
# print("source2.schema.json()", source2.schema.json())
# # def schema_check(source, target):
# #     # Extract schema details as lists of (column_name, data_type) tuples
# #     source_schema = source.schema
# #     target_schema = target.schema
# #     print(source_schema)
# #     print(target_schema)
# #
# #     # Convert schemas to DataFrames
# #     source_schema_df = spark.createDataFrame(
# #         [(field.name.lower(), field.dataType.simpleString()) for field in source_schema],
# #         ["col_name", "source_data_type"]
# #     )
# #
# #     print("source_schema_df",source_schema_df.show())
# #     target_schema_df = spark.createDataFrame(
# #         [(field.name.lower(), field.dataType.simpleString()) for field in target_schema],
# #         ["col_name", "target_data_type"]
# #     )
# #     print(target_schema_df)
# #
# #     # Perform a full join on column names and compare data types
# #     schema_comparison = (
# #         source_schema_df.alias("src")
# #         .join(target_schema_df.alias("tgt"), col("src.col_name") == col("tgt.col_name"), "full_outer")
# #         .select(
# #             col("src.col_name").alias("source_col_name"),
# #             col("tgt.col_name").alias("target_col_name"),
# #             col("src.source_data_type"),
# #             col("tgt.target_data_type"),
# #             when(col("src.source_data_type") == col("tgt.target_data_type"), "pass")
# #             .otherwise("fail")
# #             .alias("status")
# #         )
# #     )
# #
# #     # Filter only rows where the status is 'fail'
# #     failed = schema_comparison.filter(col("status") == "fail")
# #     failed
# #
# #     return failed
# #
# # schema_check(source,target)
# #
# #
# #
# #
# # # def data_compare(source, target, key_column):
# # #
# # #     src_cnt = source.count()
# # #     tgt_cnt = target.count()
# # #     columnList = source.columns
# # #     smt = source.exceptAll(target).withColumn("datafrom", lit("source"))
# # #     tms = target.exceptAll(source).withColumn("datafrom", lit("target"))
# # #     failed = smt.union(tms)
# # #     failed.show()
# # #
# # #     failed_count = failed.count()
# # #     if failed_count > 0:
# # #         failed_records = failed.limit(5).collect()  # Get the first 5 failing rows
# # #         failed_preview = [row.asDict() for row in failed_records]
# # #         write_output(
# # #                 "Uniqueness Check",
# # #                 "FAIL",
# # #                 f"Data mismatch data: {failed_preview}"
# # #             )
# # #     else:
# # #         write_output(
# # #             "Uniqueness Check",
# # #             "PASS",
# # #             f"No mismatches found"
# # #         )
# # #
# # #
# # #     if failed.count() > 0:
# # #
# # #         print("columnList", columnList)
# # #         print("keycolumns", key_column)
# # #         for column in columnList:
# # #             print(column.lower())
# # #             if column.lower() not in key_column:
# # #                 key_column.append(column)
# # #                 temp_source = source.select(key_column).withColumnRenamed(column, "source_" + column)
# # #
# # #                 temp_target = target.select(key_column).withColumnRenamed(column, "target_" + column)
# # #                 key_column.remove(column)
# # #                 temp_join = temp_source.join(temp_target, key_column, how='full_outer')
# # #                 temp_join.withColumn("comparison", when(col('source_' + column) == col("target_" + column),
# # #                                                         "True").otherwise("False")).filter(
# # #                     f"comparison == False ").show()
# # #
# # #         return True
# # #
# # #
# # # data_compare(source,target,key_column)
