# from unicodedata import lookup
from pyspark.sql import SparkSession
import pytest
import yaml
import os
import json
from pyspark.sql.types import StructType, ArrayType
from src.utility.general_utility import flatten
import subprocess


@pytest.fixture(scope='session')
def spark_session(request):
    #dir_path = request.node.fspath.dirname
    snow_jar = '/Users/admin/PycharmProjects/test_automation_project/jar/snowflake-jdbc-3.14.3.jar'
    postgres_jar = '/Users/admin/PycharmProjects/test_automation_project/jar/postgresql-42.2.5.jar'
    azure_storage = '/Users/admin/PycharmProjects/test_automation_project/jar/azure-storage-8.6.6.jar'
    hadoop_azure = '/Users/admin/PycharmProjects/test_automation_project/jar/hadoop-azure-3.3.1.jar'
    sql_server = '/Users/admin/PycharmProjects/taf/jars/mssql-jdbc-12.2.0.jre8.jar'
    jar_path = snow_jar + ',' + postgres_jar + ',' + azure_storage + ',' + hadoop_azure + ',' + sql_server
    # spark = SparkSession.builder.master("local[4]") \
    #     .appName("pytest_framework") \
    #     .config("spark.jars", jar_path) \
    #     .config("spark.driver.extraClassPath", jar_path) \
    #     .config("spark.executor.extraClassPath", jar_path) \
    #     .getOrCreate()
    #
    # adls_account_name = "septauto"  # Your ADLS account name
    # adls_container_name = "raw"  # Your container name
    # key = "6TR8QTDWIWj0EshX2YRzMln2dYylTAVUECMoLHE2JPo0SwXt9Kbybqpca96qNTnndDFGB/t4UbTo+AStbQROcg=="  # Your Account Key
    #
    # spark.conf.set(f"fs.azure.account.auth.type.{adls_account_name}.dfs.core.windows.net", "SharedKey")
    # spark.conf.set(f"fs.azure.account.key.{adls_account_name}.dfs.core.windows.net", key)
    #
    # return spark

@pytest.fixture(scope='module')
def read_config(request):
    print("request.node.fspath.dirname", request.node.fspath.dirname)
    dir_path = request.node.fspath.dirname
    config_path = dir_path + '/config.yml'
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)
    return config_data


def read_schema(dir_path):
    schema_path = dir_path + '/schema.json'
    with open(schema_path, 'r') as schema_file:
        schema = StructType.fromJson(json.load(schema_file))
    return schema

def read_query(dir_path):
    sql_query_path = dir_path + '/transformation.sql'
    with open(sql_query_path, "r") as file:
        sql_query = file.read()
    return sql_query


def read_file(config_data,spark, dir_path=None):
    if config_data['type'] == 'csv':
        if config_data['schema'] == 'Y':
            schema = read_schema(dir_path)
            df = spark.read.schema(schema).csv(config_data['path'], header=config_data['options']['header'])
        else:
            df = spark.read.csv(config_data['path'], header= config_data['options']['header'],inferSchema=True)
    elif config_data['type'] == 'json':
        df = spark.read.json(config_data['path'], multiLine=config_data['options']['multiline'] )
        df = flatten(df)
    elif config_data['type'] == 'parquet':
        df = spark.read.parquet(config_data['path'])
    elif config_data['type'] == 'avro':
        df = spark.read.format('avro').load(config_data['path'])
    return df

def read_db(config_data,spark,dir_path):
    creds = load_credentials()
    cred_lookup = config_data['cred_lookup']
    creds = creds[cred_lookup]
    if config_data['transformation'][0].lower() == 'y' and config_data['transformation'][1].lower() == 'sql':
        sql_query= read_query(dir_path)
        print("sql_query", sql_query)
        df = spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("query", sql_query). \
            option("driver", creds['driver']).load()

    else:
        df = spark.read.format("jdbc"). \
            option("url", creds['url']). \
            option("user", creds['user']). \
            option("password", creds['password']). \
            option("dbtable", config_data['table']). \
            option("driver", creds['driver']).load()
    return df

@pytest.fixture(scope='module')
def read_data(read_config,spark_session,request ):
    spark = spark_session
    config_data = read_config
    source_config = config_data['source']
    target_config = config_data['target']
    dir_path = request.node.fspath.dirname
    if source_config['type'] == 'database':
        if source_config['transformation'][1].lower() == 'python' and source_config['transformation'][0].lower() == 'y':
            python_file_path = dir_path + '/transformation.py'
            print("python file name", python_file_path)
            subprocess.run(["python", python_file_path])

        source = read_db(config_data=source_config,spark=spark,dir_path=dir_path)
    else:
        source = read_file(config_data = source_config,spark=spark, dir_path=dir_path)

    if target_config['type'] == 'database':
        if target_config['transformation'][1].lower() == 'python' and target_config['transformation'][0].lower() == 'y':
            python_file_path = dir_path + '/transformation.py'
            subprocess.run(python_file_path)
        target = read_db(config_data=target_config,spark=spark,dir_path=dir_path)
    else:
        target = read_file(config_data =target_config,spark=spark,dir_path=dir_path)

    print("target_exclude", target_config['exclude_cols'])

    return source.drop(*source_config['exclude_cols']), target.drop(*target_config['exclude_cols'])



def load_credentials(env="qa"):
    """Load credentials from the centralized YAML file."""
    taf_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    credentials_path = taf_path+'/project_config/cred_config.yml'

    with open(credentials_path, "r") as file:
        credentials = yaml.safe_load(file)
        print(credentials[env])
    return credentials[env]

# conftest.py
# from pytest_html import extras

def pytest_html_results_table_header(cells):
    """Add a custom header for Failure Details in the HTML report."""
    cells.insert(1, "Failure Details")

def pytest_html_results_table_row(report, cells):
    """Insert failure details for each test into the HTML report."""
    if hasattr(report, "details"):
        cells.insert(1, report.details)
    else:
        cells.insert(1, "N/A")

def pytest_runtest_logreport(report):
    """Attach failure details dynamically."""
    if report.when == "call" and report.failed:
        if hasattr(report, "details"):
            report.details = (
                f"Source count: {report.source_count}, "
                f"Target count: {report.target_count}, "
                f"Records only in Source: {report.fail_count_source}, "
                f"Records only in Target: {report.fail_count_target}."
            )






