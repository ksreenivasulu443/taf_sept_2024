from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when
from src.utility.report_lib import write_output
from tests.table1.test_table1 import target

spark = SparkSession.builder.master('local[1]').getOrCreate()

source= spark.read.csv("/Users/admin/PycharmProjects/taf/input_files/Contact_info_s.csv",inferSchema=True, header=True)
target =spark.read.csv("/Users/admin/PycharmProjects/taf/input_files/Contact_info_t.csv",inferSchema=True, header=True)
key_column = ["identifier"]





def data_compare(source, target, key_column):

    src_cnt = source.count()
    tgt_cnt = target.count()
    columnList = source.columns
    smt = source.exceptAll(target).withColumn("datafrom", lit("source"))
    tms = target.exceptAll(source).withColumn("datafrom", lit("target"))
    failed = smt.union(tms)
    failed.show()

    failed_count = failed.count()
    if failed_count > 0:
        failed_records = failed.limit(5).collect()  # Get the first 5 failing rows
        failed_preview = [row.asDict() for row in failed_records]
        write_output(
                "Uniqueness Check",
                "FAIL",
                f"Data mismatch data: {failed_preview}"
            )
    else:
        write_output(
            "Uniqueness Check",
            "PASS",
            f"No mismatches found"
        )


    if failed.count() > 0:

        print("columnList", columnList)
        print("keycolumns", key_column)
        for column in columnList:
            print(column.lower())
            if column.lower() not in key_column:
                key_column.append(column)
                temp_source = source.select(key_column).withColumnRenamed(column, "source_" + column)

                temp_target = target.select(key_column).withColumnRenamed(column, "target_" + column)
                key_column.remove(column)
                temp_join = temp_source.join(temp_target, key_column, how='full_outer')
                temp_join.withColumn("comparison", when(col('source_' + column) == col("target_" + column),
                                                        "True").otherwise("False")).filter(
                    f"comparison == False ").show()

        return True


data_compare(source,target,key_column)
