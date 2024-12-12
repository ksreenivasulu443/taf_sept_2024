
from pyspark.sql import SparkSession
import pytest
from src.data_validations.count_validation import count_check
from src.data_validations.duplicate_validation import duplicate_check
from src.data_validations.null_validation import null_value_check



def test_count_check(read_data):
    source,target = read_data
    assert count_check(source,target) == 'PASS'


def test_duplicate_check(read_data,read_config):
    source,target = read_data
    config = read_config
    key_col = config['validations']['duplicate_check']['key_columns']
    print("key col", key_col)
    assert duplicate_check(df=target,key_col=key_col) == 'PASS'

def test_null_check(read_data,read_config):
    source, target = read_data
    config = read_config
    null_cols = config['validations']['null_check']['null_columns']

    print("null_cols ", null_cols)
    assert null_value_check(df=target, null_cols=null_cols)

