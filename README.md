# LetsData Python Interface
The letsdata data interfaces package for the python language.

The #Let'sData infrastructure simplifies the data processing for the customers. Our promise to the customers is that "Focus on the data - we'll manage the infrastructure".

The #Let'sData infrastructure implements a control plane for the data tasks, reads and writes to different destinations, scales the processing infrastructure as needed, builds in monitoring and instrumentation. 

However, it does not know what the data is, how it is stored in the data files and how to read the data and create transformed documents. This is where the #Let'sData infrastructure requires the customer code.
The customer needs to implement user data handlers that tell us what makes a data record in the files - we'll then send the records to these data handlers where the user can transform the records into documents that can be sent to the write destination. This requires implementation of the user data handler interfaces. 

This package defines these user data handler interfaces for the python language. 

(You can also view the # Let's Data docs to see how to implement different usecases, examples and more: https://www.letsdata.io/docs)

In addition to the python interfaces, the package also contains some #Lets Data internal implementations that are used as request-response handlers for the lambda function. (okay to leave these as is)

The code in the package is divided into the following sub-folders:
* **letsdata_interfaces:** These are the #LetsData's user data handler interfaces that users are required to implement. This is where you'd be defining your implementations. 
* **letsdata_service:** System's internal implementation of the request-response handlers for the lambda function. You'll probably not need to do anything here, but for the curious, the lambda request to actual exection translation happens in these code files. 
* **letsdata_utils:** Some common utility classes, users would find the `logger` in `logging_utils.py` and the `letsdata_assert` in `validations.py` useful during the interface coding.
* **letsdata_lambda_function.py:** This is the entry point for the lambda function's handler code.  You'll probably not need to do anything here, but for the curious, you can trace the lambda function's code logic if interested.
* **requirements.txt:** the python packages that should be installed. Add any additional packages in here as may be needed. These are optional. 
* **dockerfile:** The dockerfile has docker commands to build the docker image. In case you add new files, move around existing files etc, do make sure you make the corresponding changes in this docker file as well. 
* **build.sh:** This is a simple script that can build this package using the dockerfile, run quick tests using curl (remember to define your request and expected responses) and upload the docker image to the ECR repo. You are expected to customize this build.sh file if needed to build and deploy images and run quick sanity tests on the built image. 
* **tests:** Well, there aren't any for this package except the ones defined in build.sh (we use these to make sure build is okay). Since this is a fully functional python package, feel free to add tests as needed and include any additional test infrastructure as might be needed. 
* **letsdata_example_configs** We've included some example dataset configurations to get your started with #LetsData datasets. See the 'How to Implement' section below on details around these. 

## letsdata_interfaces:
Here is a look at the #LetsData interfaces and what you might need to implement in your case.

### Documents
#Let's Data has defined the document classes in the namespace `letsdata_interfaces.documents` that you can use to return parsed documents, errors and skip docs. These are as follows:
* **Document:** The `letsdata_interfaces.documents.Document` is the container for any document that can be returned by the user handlers. All other document interfaces and documents either extend or implement this interface.
* **ErrorDoc:** The `letsdata_interfaces.documents.ErrorDoc` extends the "Document" and is the container for any error documents that are returned by the user handlers. Customers can return errors from handlers using this implementation.
* **SkipDoc:** The `letsdata_interfaces.documents.SkipDoc` extends the "Document" and is the container for any skip documents that are returned by the user handlers. A skip document is returned when the processor determines that the record from the file is not of interest to the current processor and should be skipped from being written to the write destination. Customers can return skip records from handlers using this default implementation.

### Readers
* **S3 - SingleFileParser**: The `letsdata_interfaces.readers.parsers.SingleFileParser` is the parser interface for reading an S3 File. This is where you tell us how to parse the individual records from the file. The implementation needs to be stateless.
* **Kinesis - KinesisRecordReader**: The `letsdata_interfaces.readers.kinesis.KinesisRecordReader` is the parser interface for processing a kinesis record. This is where you transform a Kinesis record to a document.
* **DynamoDB Streams - DynamoDBStreamsRecordReader**: The `letsdata_interfaces.readers.dynamodbstreams.DynamoDBStreamsRecordReader` is the parser interface for processing a dynamodb streams record. This is where you transform a DynamoDB Streams record to a document.
* **SQS - QueueMessageReader**: The `letsdata_interfaces.readers.sqs.QueueMessageReader` is the parser interface for processing an sqs message. This is where you transform an sqs message to a document.
* **Sagemaker - SagemakerVectorsInterface**: The `letsdata_interfaces.readers.sagemaker.SagemakerVectorsInterface` is the interface for processing documents for AWS Sagemaker vector embeddings generation. This is where you extract the document that needs vectorizationfrom the feature doc in `extractDocumentElementsForVectorization` and construct an output doc from the vectors in `constructVectorDoc`.

### Model
The `letsdata_interfaces.model` has helper classes that are used to return results and metadata to the callers. The `ParseDocumentResult` returns the parsed document and status code. The `RecordParseHint` is what is used to return record start and record end patterns for parsing records from S3 file. 

## How to Implement:
* Create a copy of the code by Forking the repo to your account. 
* Plan your dataset as to what is the read destination, the write destination, the error destination and compute engine. Depending on these, you'll need to implement the interfaces. 
    * For SQS read, implement the `letsdata_interfaces.readers.sqs.QueueMessageReader`
    * For Kinesis read, implement the `letsdata_interfaces.readers.sqs.KinesisRecordReader`
    * For S3 read, implement the `letsdata_interfaces.readers.parsers.SingleFileParser`
    * For Sagemaker read, implement the `letsdata_interfaces.readers.sagemaker.SagemakerVectorsInterface`
* In the forked repo, implement the interfaces for the usecases. Example implementation of these are in the examples repo.
* Update the build.sh 
    * with your AWS account 
    * with your test curl commands
    * with your ECR repo details
* Optionally update the package.json in case you are adding new packages and dockerfile in case your are adding new files / moving existing files. 
* Build and upload using build.sh
* Grant #LetsData account access to the ECR Image: https://www.letsdata.io/docs/access-grants#ecr-image-access-grant 
* Create a dataset using the built image
    * Create a dataset configuration json file. We've included the following examples:
        * `dataset1.json`: Reads from S3 File, runs the single file parser interface to extract documents that are written to kinesis stream. Example implementation of the interface is in the repo:   
        * `dataset2.json`: Reads the records from kinesis stream and runs sagemaker interfaces to extract the text that needs vectorization, and then combines the vectors with the feature doc to produce an output doc. This is written to SQS. Example implementation of the Kinesis read interface and the Sagemaker extract and construct interface is in the repo:    
        * `dataset3.json`: Reads the records from SQS queue and writes to the Kinesis stream. Example implementation of the SQS interface is in the repo:
    * Create the dataset using the CLI
        ```
        ./letsdata datasets create --configFile dataset1.json --prettyPrint
        ```

## References:
* **LetsData:** LetsData links for learning about datasets, creating dataset configurations, access grants, examples and the sdk docs.
    * **Datasets:** https://www.letsdata.io/docs/datasets/
    * **Read Connectors:** https://www.letsdata.io/docs/read-connectors/
    * **Write Connectors:** https://www.letsdata.io/docs/write-connectors/
    * **Compute Engine:** https://www.letsdata.io/docs/compute-engine/
    * **Access Grants:** https://www.letsdata.io/docs/access-grants/
    * **Examples:** https://www.letsdata.io/docs/examples/
    * **SDK Interface:** https://www.letsdata.io/docs/sdk-interface/
    


