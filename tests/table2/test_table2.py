from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local[1]').appName('pytest automation').getOrCreate()

source = spark.read.parquet("/Users/admin/PycharmProjects/taf/input_files/userdata1_s.parquet" )

target = spark.read.parquet("/Users/admin/PycharmProjects/taf/input_files/userdata1_t.parquet")

src_count = source.count()
tgt_count = target.count()

def test_count():
    if src_count == tgt_count:
        status = 'PASS'
    else:
        status = 'FAIL'
    assert status == 'PASS'