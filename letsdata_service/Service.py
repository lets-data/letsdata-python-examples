from enum import Enum
from letsdata_utils.logging_utils import logger
'''
    event:
    {
        "requestId": "requestId",
        "interface": "SingleFileParser",
        "function": "getS3FileType|getResolvedS3FileName|getRecordStartPattern|getRecordEndPattern|parseDocument",
        "letsdataAuth": {
            "tenantId": "tenant_id",
            "userId": "user_id",
            "datasetName": "datasetName",
            "datasetId": "datasetId"
        },
        "data": {
            "name": "value"
            ...
        }
        "batchedData": [
            {
                "name": "value"
                ...
            },
            ...
        ]
    }
'''

class LetsDataAuthParams:
    def __init__(self, letsDataAuthDict: dict) -> None:
        if letsDataAuthDict is None:
            raise(Exception("letsDataAuthDict is null"))
        
        if not isinstance(letsDataAuthDict, dict):
            raise(Exception("lambda letsDataAuthDict is not a dictionary "+str(type(letsDataAuthDict))))
        
        self.tenantId = letsDataAuthDict['tenantId']
        self.userId = letsDataAuthDict['userId']
        self.datasetName = letsDataAuthDict['datasetName']
        self.datasetId = letsDataAuthDict['datasetId']
        logger.debug("LetsDataAuthParams initialized - LetsDataAuthParams: "+str(self))

class InterfaceNames(Enum):
    SingleFileParser = "singlefileparser"
    QueueMessageReader = "queuemessagereader"
    SagemakerVectorsInterface = "sagemakervectorsinterface"
    KinesisRecordReader = "kinesisrecordreader"
    DynamoDBStreamsRecordReader = "dynamodbstreamsrecordreader"
    DynamoDBTableItemReader = "dynamodbtableitemreader"
    SparkMapperInterface = "sparkmapperinterface"
    SparkReducerInterface = "sparkreducerinterface"

    @classmethod
    def fromValue(cls, enumValue):
        return cls(enumValue.lower())

class ServiceRequest:
    def __init__(self, requestId: str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str) -> None:
        self.requestId = requestId
        self.letsDataAuth = letsDataAuth
        self.interfaceName = interfaceName
        self.functionName = functionName

    def execute(self) -> object:
        pass

