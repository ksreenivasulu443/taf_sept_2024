from pyspark.sql import SparkSession
import pytest
import yaml
import os

@pytest.fixture(scope='session')
def spark_session(request):
    dir_path = request.node.fspath.dirname
    jar_path = '/Users/admin/PycharmProjects/taf/jars/postgresql-42.7.3.jar'
    spark = SparkSession.builder.master("local[2]") \
        .appName("pytest_framework") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .config("spark.executor.extraClassPath", jar_path) \
        .getOrCreate()
    return spark

@pytest.fixture(scope='module')
def read_config(request):
    print("request.node.fspath.dirname", request.node.fspath.dirname)
    dir_path = request.node.fspath.dirname
    config_path = dir_path + '/config.yml'
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    return config_data

def read_file(config_data,spark):
    if config_data['type'] == 'csv':
        df = spark.read.csv(config_data['path'], header= config_data['options']['header'],inferSchema=True)
    elif config_data['type'] == 'json':
        df = spark.read.json(config_data['path'], multiLine=config_data['options']['multiline'] )
    elif config_data['type'] == 'parquet':
        df = spark.read.parquet(config_data['path'])
    elif config_data['type'] == 'avro':
        df = spark.read.format('avro').load(config_data['path'])
    return df

def read_db(config_data,spark):
    df = spark.read.format("jdbc"). \
        option("url", config_data['options']['url']). \
        option("user", config_data['options']['user']). \
        option("password", config_data['options']['password']). \
        option("dbtable", config_data['options']['table']). \
        option("driver", config_data['options']['driver']).load()
    return df

@pytest.fixture(scope='module')
def read_data(read_config,spark_session):
    spark = spark_session
    config_data = read_config
    source_config = config_data['source']
    target_config = config_data['target']
    if source_config['type'] == 'database':
        source = read_db(source_config,spark)
    else:
        source = read_file(config_data = source_config,spark=spark)

    if target_config['type'] == 'database':
        target = read_db(target_config,spark)
    else:
        target = read_file(config_data =target_config,spark=spark)





    return source, target

