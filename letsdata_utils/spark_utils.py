import os
from pyspark.sql import *
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col, split, expr, udf, regexp_replace, lit
from letsdata_utils.arn_utils import parse_bucket_name_from_s3_uri
from letsdata_utils.logging_utils import logger
from typing import Type, Dict, Any

def createSparkSession(appName : str, readDestination: str, writeDestination : str, readUri : str, writeUri : str, readCredentials : Dict[str, Any], writeCredentials : Dict[str, Any]):
    
    # dumpEnvironmentVariables()

    # stop any existing sessions so that we create new sessions
    if SparkSession.getActiveSession() is not None:
        SparkSession.getActiveSession().stop()

    # create new session
    builder = SparkSession \
        .builder \
        .appName(appName) \
        .config("spark.ui.enabled", "false") \
        .config("spark.driver.bindAddress", "127.0.0.1") 
        #.config("spark.eventLog.enabled", "true") \
        #.config("spark.eventLog.dir", "/tmp/spark/log") \
        
    if readDestination.upper() == "S3" or writeDestination.upper() == "S3":
        # .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.EnvironmentVariableCredentialsProvider") \
        builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
        readBucket = None
        if readDestination.upper() == "S3":
            readBucket = parse_bucket_name_from_s3_uri(readUri)
            builder.config("spark.hadoop.fs.s3a.bucket."+readBucket+".access.key", readCredentials['AWS_ACCESS_KEY_ID']) 
            builder.config("spark.hadoop.fs.s3a.bucket."+readBucket+".secret.key", readCredentials['AWS_SECRET_ACCESS_KEY']) 
            builder.config("spark.hadoop.fs.s3a.bucket."+readBucket+".session.token", readCredentials['AWS_SESSION_TOKEN']) 
        if writeDestination.upper() == "S3":
            writeBucket = parse_bucket_name_from_s3_uri(writeUri)
            if readBucket != writeBucket:
                builder.config("spark.hadoop.fs.s3a.bucket."+writeBucket+".access.key", writeCredentials['AWS_ACCESS_KEY_ID']) 
                builder.config("spark.hadoop.fs.s3a.bucket."+writeBucket+".secret.key", writeCredentials['AWS_SECRET_ACCESS_KEY']) 
                builder.config("spark.hadoop.fs.s3a.bucket."+writeBucket+".session.token", writeCredentials['AWS_SESSION_TOKEN']) 
    else:
        raise(Exception(readDestination+" readDestination not supported"))

    spark = builder.getOrCreate()
    return spark

def readSparkDataframe(spark : SparkSession, readDestination : str, readUri : str, readFormat: str, readOptions : dict) -> DataFrame:
    df : DataFrame = None
    if readDestination.upper() == "S3":
        dfReader = spark.read
        lineSep = None
        if readOptions is not None and len(readOptions) > 0:
            for keyName in readOptions.keys():
                if str(keyName).lower == "linesep":
                    lineSep = readOptions[keyName]
                else:
                    dfReader = dfReader.option(keyName, readOptions[keyName])
        if readFormat == 'json':
            dfReader = dfReader.format(readFormat)
            df = dfReader.load(readUri)
        elif readFormat == "parquet":
            dfReader = dfReader.format(readFormat)
            df = dfReader.load(readUri)
        elif readFormat == "text":
            dfReader = dfReader.format(readFormat)
            df = dfReader.load(readUri)
        elif readFormat == 'text':
            df = dfReader.text(readUri, lineSep=lineSep)
        else:
            raise(Exception(readFormat+" readFormat not supported"))
    else:
        raise(Exception(readDestination+" readDestination not supported"))
    return df
   
def writeSparkDataframe(spark : SparkSession, writeDestination : str, writeUri : str, writeFormat: str, writeMode: str, writeOptions : dict, df : DataFrame):
    if writeDestination.upper() == "S3":
        dfWriter = df.write.mode(writeMode)
        
        if writeOptions is not None and len(writeOptions) > 0:
            for keyName in writeOptions.keys():
                dfWriter = dfWriter.option(keyName, writeOptions[keyName])

        if writeFormat == "json":
            dfWriter = dfWriter.format(writeFormat)
        elif writeFormat == "csv":
            dfWriter = dfWriter.format(writeFormat)
        elif writeFormat == "parquet":
            dfWriter = dfWriter.format(writeFormat)
        else:
            raise(Exception(writeFormat+" writeFormat not supported"))
        
        dfWriter.save(writeUri)
    else:
        raise(Exception(writeDestination+" writeDestination not supported"))
    return df

def dumpEnvironmentVariables():
    for name, value in os.environ.items():
        logger.debug("{0}: {1}".format(name, value))
    