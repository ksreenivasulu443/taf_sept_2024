
from pyspark.sql import SparkSession
import pytest
from src.data_validations.count_validation import count_check
from src.data_validations.duplicate_validation import duplicate_check



def test_count_check(read_data):
    source,target = read_data
    assert count_check(source,target) == 'PASS'


def test_duplicate_check(read_data,read_config):
    _,target = read_data
    config = read_config
    key_col = config['validations']['duplicate_check']['key_columns']
    print("key col", key_col)
    assert duplicate_check(target_df=target,key_col=key_col,config_data=config) == 'PASS'

