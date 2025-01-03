
from pyspark.sql import SparkSession
import pytest
from src.data_validations.count_validation import count_check
from src.data_validations.duplicate_validation import duplicate_check
from src.data_validations.null_validation import null_value_check
from src.data_validations.uniqueness_check import uniqueness_check
from src.data_validations.data_compare import data_compare
from src.data_validations.schema_validation import schema_check



def test_count_check(read_data,read_config):
    source,target = read_data
    print(source.columns)
    print(target.columns)
    config = read_config
    key_columns = config['validations']['count_check']['key_columns']
    print("key col", key_columns)
    assert count_check(source=source,target=target,key_columns=key_columns) == 'PASS'


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
    assert null_value_check(df=target, null_cols=null_cols) =='PASS'

def test_uniqueness_check(read_data,read_config):
    source, target = read_data
    config = read_config
    unique_cols = config['validations']['uniqueness_check']['unique_columns']
    print("unique_cols ", unique_cols)
    assert uniqueness_check(df=target, unique_cols=unique_cols)

def test_data_compare_check(read_data,read_config):
    source, target = read_data
    config = read_config
    key_columns = config['validations']['data_compare_check']['key_column']
    print("key_columns ", key_columns)
    assert data_compare(source=source, target=target, key_column=key_columns) =='PASS'

def test_schema_check(read_data, read_config,spark_session):
    source,target = read_data
    config = read_config
    spark = spark_session
    assert schema_check(source=source,target=target,spark=spark) == "PASS"

