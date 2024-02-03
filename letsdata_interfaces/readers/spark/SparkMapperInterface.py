# letsdata imports
import uuid
import os
from letsdata_interfaces.readers.model.RecordParseHint import RecordParseHint
from letsdata_interfaces.readers.model.RecordHintType import RecordHintType
from letsdata_interfaces.readers.model.ParseDocumentResultStatus import ParseDocumentResultStatus
from letsdata_interfaces.readers.model.ParseDocumentResult import ParseDocumentResult
from letsdata_interfaces.documents.Document import Document
from letsdata_interfaces.documents.DocumentType import DocumentType
from letsdata_interfaces.documents.ErrorDoc import ErrorDoc
from letsdata_utils.logging_utils import logger
from letsdata_utils.secretmanager_util import SecretManagerUtil

# spark imports
from pyspark.sql import *
from pyspark.sql.types import MapType, StringType
from pyspark.sql.functions import col, split, expr, udf, regexp_replace, lit
from letsdata_utils.spark_utils import createSparkSession, readSparkDataframe, writeSparkDataframe

'''
a custom udf used to convert the key value pair list ['warcFormatKey', 'k1', 'v1', 'k2', 'v2', ...] to a map  {'k1': 'v1', 'k2': 'v2', ... }
'''
def to_map(arr):
	map_value = dict()
	try:
		for i in range(1,len(arr),2):
			key = arr[i]
			value = arr[i+1]
			map_value[key] = value
	except Exception as err:
		print("error: "+str(err)+", arr: "+str(arr))
		pass
	return map_value


class SparkMapperInterface:
   def __init__(self) -> None:
         pass
   '''
   This an example implementation of the spark's mapper interface. 
   
   The implementation uses the 'default implementations' for the following which wrap the user's spark transformation code:
     * the code has default credentials get - separate credentials are used for the read bucket and write bucket
     * spark session setup - spark is setup in standalone mode
     * read the file into the dataframe method calls - read the data frame using the format, options and the uri specified in the method args
     * user's spark transformation code (narrow transformations that need to be applied to each partition)
          * read the file as a text file using \n\r\n\r\n as the line separator (record separator)
          * trim the whitespaces from the record
          * split the line read into a header and payload components using \r\n\r\n as the delimiter
          * header further split as each line key value pair into a string array ['warcFormatKey', 'k1', 'v1', 'k2', 'v2', ...]
          * a custom udf is used to convert the above key value pair list to a map  {'k1': 'v1', 'k2': 'v2', ... }
          * each key is converted to a column and value as the value of that colum for each row
          * the output dataframe with headerKey columns and a payload column is created
     * write the output dataframe as files to the intermediate S3 bucket for processing by reduce tasks 

   For example, the following read connector and write connector parameters would be sent via different function parameters (json annotated below)
     "readConnector": {
          "connectorDestination": "S3",                     # passed as the readDestination
          "artifactImplementationLanguage": "python",
          "interfaceECRImageResourceLocation": "Customer",
          "interfaceECRImagePath": "151166716410.dkr.ecr.us-east-1.amazonaws.com/letsdata_python_functions:latest",
          "readerType": "SPARKREADER",
          "bucketName": "commoncrawl",
          "bucketResourceLocation": "Customer",
          "sparkFileFormat": "text",                        # passed as the readFormat
          "sparkReadOptions": {                             # passed as readOptions
               "lineSep": "\n\r\n\r\n"
          }
     }

     "manifestFile": {
          "manifestType": "S3ReaderTextManifestFile",
          "region": "us-east-1",
          "fileContents": "crawl-data/CC-MAIN-2023-50/segments/1700679099281.67/wet/CC-MAIN-20231128083443-20231128113443-00000.warc.wet.gz\n     # s3a://<bucketName>/<fileName> is passed as the readUri
                         crawl-data/CC-MAIN-2023-50/segments/1700679099281.67/wet/CC-MAIN-20231128083443-20231128113443-00001.warc.wet.gz\n
                         crawl-data/CC-MAIN-2023-50/segments/1700679099281.67/wet/CC-MAIN-20231128083443-20231128113443-00002.warc.wet.gz\n
                         crawl-data/CC-MAIN-2023-50/segments/1700679099281.67/wet/CC-MAIN-20231128083443-20231128113443-00003.warc.wet.gz",
          "readerType": "SPARKREADER"
     }

     Since this is MAPPER_AND_REDUCER run configuration, system generates intermediate file locations for write as follows:
          * writeDestination : S3
          * writeUri : s3a://<intermediateFilesBucketName>/<intermediateFilename> 
          * writeFormat : Store in parquet format to allow for efficient reduce processing
          * writeMode : Currently defaults to 'overwrite'. 
          * writeOptions : Write as a gzip compressed file. '{"compression":"gzip"}'

   
     Parameters
     ----------
     appName : str
                 The appName is a system generated spark app name
     readDestination : str
                 The readDestination for spark mapper - currently only S3 is supported
     readUri : str
                 The readUri for the readDestination (the s3 file link) that the mapper will read
     readFormat : str
                 The format of the file being read by the mapper. We currently support csv, text, json and parquet. You can add additional formats, just make sure to add code in spark utils to handle these.  
     readOptions : dict 
                 The options for the spark mapper's read. For text files, we can specify the lineSep as an option. Different formats can have different options that can be specified here.  
     writeDestination : str
                 The readDestination for spark mapper - currently only S3 is supported. 
     writeUri : str
                 The writeUri for the writeDestination (the s3 file link) that the mapper will write the output file to. In case of "MAPPER_AND_REDUCER" run config, this is a system generated uri that saves the file in the intermediate bucket. In case of "MAPPER_ONLY" run configuration, an output file is written to write destination bucket 
     writeFormat : str
                 The format of the file being written by the mapper. In case of "MAPPER_AND_REDUCER" run config, 'parquet' is passed, otherwise this is passed as the format. We currently support csv, text, json and parquet. You can add additional formats, just make sure to add code in spark utils to handle these.  
     writeMode : str
                 The writeMode for spark mapper - currently defaults to 'overwrite'. 
     writeOptions : dict
                 The options for the spark mapper's write. In case of "MAPPER_AND_REDUCER" run config, '{"compression":"gzip"}' is passed, otherwise the dataset's sparkWriteOptions are passed as the writeOptions. Different formats can have different options that can be specified here.  
     spark_credentials_secret_arn : str
                 The secret manager arn for credentials for reading and writing to the read / write buckets
     Returns
     -------
     void
        The function writes the data to write destination and does not return anything
   '''
   def mapper(self, appName: str, readDestination : str, readUri : str, readFormat : str, readOptions : dict, writeDestination:str, writeUri : str, writeFormat: str, writeMode: str, writeOptions : dict, spark_credentials_secret_arn : str):
        readCredentials = self.get_read_destination_credentials(os.environ['AWS_REGION'], spark_credentials_secret_arn)
        writeCredentials = self.get_write_destination_credentials(os.environ['AWS_REGION'], spark_credentials_secret_arn)
        spark = createSparkSession(appName, readDestination, writeDestination, readUri, writeUri, readCredentials, writeCredentials)
        df = readSparkDataframe(spark, readDestination, readUri, readFormat, readOptions)

        # take a smaller set to keep it manageable in build and tests
        # df = df.randomSplit([0.3,0.7], 5)[0]
		
        # trim whitespaces, not just spaces
        df = df.select(regexp_replace(df.value, r"^\s+|\s+$", "").alias("value"))

        # split string into header and payload columns
        df = df.select(split(df.value, '\r\n\r\n').alias('s')) \
            .withColumn('header', expr('s[0]')) \
            .withColumn('payload', expr('s[1]')) \
            .select(col('header'), col('payload'))
		
        # split the header into key value pair list 
        # example: WARC/1.0\r\nWARC-Type: warcinfo\r\nWARC-Date: 2023-12-12T01:40:15Z\r\nWARC-Filename: CC-MAIN-20231128083443-20231128113443-00003.warc.wet.gz\r\nWARC-Record-ID: <urn:uuid:6082596d-524a-4e49-b1dd-86582dc01a2f>\r\nContent-Type: application/warc-fields\r\nContent-Length: 382
        # becomes: [WARC/1.0, WARC-Type, warcinfo, WARC-Date, 2023-12-12T01:40:15Z, WARC-Filename, CC-MAIN-20231128083443-20231128113443-00003.warc.wet.gz, WARC-Record-ID, <urn:uuid:6082596d-524a-4e49-b1dd-86582dc01a2f>, Content-Type, application/warc-fields, Content-Length, 382]
        df = df.select(split(df.header, ': |\r\n').alias('h'), col('payload'))

        # create a UDF for the to_map function defined above
        array_to_map = udf(to_map, MapType(StringType(),StringType()))

        # convert array to map, and then map to columns using the known schema 
        df = df.withColumn('header_map',array_to_map(df.h)) \
            .select(expr("header_map['WARC-Filename']").alias("warcFileName"), 
                    expr("header_map['Content-Length']").cast("integer").alias("contentLength"),
                    expr("to_timestamp(header_map['WARC-Date'])").alias("warcDate"),
                    expr("header_map['WARC-Record-ID']").alias("warcRecordId"),
                    expr("header_map['Content-Type']").alias("contentType"),
                    expr("header_map['WARC-Type']").alias("warcType"),
                    expr("header_map['WARC-Refers-To']").alias("warcRefersTo"),
                    expr("header_map['WARC-Identified-Content-Language']").alias("language"),
                    expr("header_map['WARC-Block-Digest']").alias("blockDigest"),
                    expr("header_map['WARC-Target-URI']").alias("url"),
                    expr("header_map['WARC-Target-URI']").alias("docId"),
                    expr("header_map['WARC-Target-URI']").alias("partitionKey"),
                    col('payload').alias("docText")) \
            .where(col('warcType') == lit('conversion')) \
            .drop('warcFileName')
		
        writeSparkDataframe(spark, writeDestination, writeUri, writeFormat, writeMode, writeOptions, df)
        
   '''
   Helper function to get the credentials for reading the spark mapper read bucket from secret manager
   '''               
   def get_read_destination_credentials(self, region, spark_credentials_secret_arn):
        return SecretManagerUtil.get_spark_aws_credentials(region, spark_credentials_secret_arn, "mapper", "read")

   '''
   Helper function to get the credentials for reading the spark mapper write bucket from secret manager
   '''     
   def get_write_destination_credentials(self, region, spark_credentials_secret_arn):
        return SecretManagerUtil.get_spark_aws_credentials(region, spark_credentials_secret_arn, "mapper", "write")