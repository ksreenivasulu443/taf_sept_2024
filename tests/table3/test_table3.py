
from pyspark.sql import SparkSession
import pytest
from src.data_validations.count_validation import count_check

@pytest.fixture
def spark_session():
    spark = SparkSession.builder.master('local[1]').appName('pytest automation').getOrCreate()
    return spark


@pytest.fixture
def read_data(spark_session):
    spark = spark_session
    source = spark.read.csv("/Users/admin/PycharmProjects/taf/input_files/Contact_info_s.csv", header=True, inferSchema=True)
    target = spark.read.csv("/Users/admin/PycharmProjects/taf/input_files/Contact_info_t.csv", header=True, inferSchema=True)
    return source, target



def test_count(read_data):
    source,target = read_data
    assert count_check(source,target) == 'PASS'