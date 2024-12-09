from pyspark.sql import SparkSession
import pytest
import yaml

@pytest.fixture(scope='session')
def spark_session():
    spark = SparkSession.builder.master('local[1]').appName('pytest automation').getOrCreate()
    return spark

@pytest.fixture(scope='module')
def read_config(request):
    print("request.node.fspath.dirname", request.node.fspath.dirname)
    dir_path = request.node.fspath.dirname
    config_path = dir_path + '/config.yml'
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    return config_data

@pytest.fixture(scope='module')
def read_data(spark_session, read_config):
    config_data = read_config
    source_config = config_data['source']
    target_config = config_data['target']
    spark = spark_session
    source = spark.read.csv(source_config['path'], header= source_config['options']['header'],inferSchema=True)
    target = spark.read.csv(target_config['path'], header=target_config['options']['header'], inferSchema=True)
    return source, target

