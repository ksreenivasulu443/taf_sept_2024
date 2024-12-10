
from pyspark.sql import SparkSession
import pytest
from src.data_validations.count_validation import count_check



def test_count(read_data):
    source,target = read_data
    assert count_check(source,target) == 'PASS'