from pyspark.sql import SparkSession


spark = SparkSession.builder.master('local[1]').getOrCreate()

target_df = spark.read.csv("/Users/admin/PycharmProjects/taf/input_files/Contact_info_t.csv",inferSchema=True, header=True)

key_col =['identifier']

print("priniting original target df")
target_df.show()

duplicates = target_df.groupBy(key_col).count().filter("count > 1")

print("priniting original duplicates df")
duplicates.show()
duplicate_count = duplicates.count()
print("priniting total duplicate count", duplicate_count)


failed_records = duplicates.limit(5).collect()

print("failed records",failed_records)# Get the first 5 failing rows
failed_preview = [row.asDict() for row in failed_records]

print("failed_preview",failed_preview)