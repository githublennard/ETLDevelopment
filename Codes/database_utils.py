import config.database_config as database_config
import boto3
import time
import sys
import json
import datetime
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql import Row, SparkSession, Column
from pyspark import sql
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql.functions import isnan, when, count, col, current_timestamp, lit, monotonically_increasing_id, row_number,  lower
import math
import os


# Sin esto da error al hacer print con determinados caracteres
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from config.config import ENTORNO

if ENTORNO == "PRE":
    name = os.path.relpath(os.path.abspath("."),"/home/dq/dq_users/")
    name = os.path.dirname(name)
else:
    name = "Master"

spark = SparkSession \
    .builder \
    .appName("DATAWALL: Database Utils " + name ) \
    .getOrCreate()


def crear_claves(df, name, start):        
    def create_rownum(ziprow):
        row, index=ziprow
        row=row.asDict()
        index = index + start
        row[name]= index 
        return(Row(**row))

    # First create a rownumber then add to dataframe
    return df.rdd.zipWithIndex().map(create_rownum).toDF()

def siguiente_clave(tabla):
    query_count = 'SELECT MAX(ejc."ClEjecControl") FROM "DQA"."EjecucionControl" ejc' 
    if ENTORNO == "PRE":
        connection_data= json.loads(get_secret("pre-dq-rdb-dqa"))
    else:
        connection_data= json.loads(get_secret("pro-dq-rdb-dqa"))          
        
    df_count = spark.read.format("jdbc") \
        .option("url", "jdbc:postgresql://" + connection_data["host"] + ":" + str(connection_data["port"]) + "/" + connection_data["dbname"]) \
        .option("user", connection_data["username"]) \
        .option("password", connection_data["password"])\
        .option("query", query_count) \
        .load()    
    start = df_count.first()[0]
    if start == None:
        return 0
    return start + 1

def df_2_db(df, tabla):
    if ENTORNO == "PRE":
        connection_data= json.loads(get_secret("pre-dq-rdb-dqa"))
    else:
        connection_data= json.loads(get_secret("pro-dq-rdb-dqa"))          
    
    df.write \
        .format("jdbc") \
        .option("dbtable", tabla) \
        .option("url", "jdbc:postgresql://" + connection_data["host"] + ":" + str(connection_data["port"]) + "/" + connection_data["dbname"]) \
        .option("user", connection_data["username"]) \
        .option("password", connection_data["password"])\
        .mode("append")\
        .save()

def get_secret(secret_name):
    #secret_name = "pre-dq-rdb-fast"
    region_name = "eu-central-1"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS CMK.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return (secret)
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])