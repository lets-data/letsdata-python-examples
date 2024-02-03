from letsdata_utils.logging_utils import logger
from letsdata_utils.validations import letsdata_assert
from letsdata_service.Service import ServiceRequest, LetsDataAuthParams, InterfaceNames
from letsdata_service.SingleFileParserService import getSingleFileParserRequest
from letsdata_service.QueueMessageReaderService import getQueueMessageReaderServiceRequest
from letsdata_service.SagemakerVectorsInterfaceService import getSagemakerVectorsInterfaceServiceRequest
from letsdata_service.KinesisRecordReaderService import getKinesisRecordReaderServiceRequest
from letsdata_service.DynamoDBRecordReaderService import getDynamoDBRecordReaderServiceRequest
from letsdata_service.DynamoDBTableItemReaderService import getDynamoDBTableItemReaderServiceRequest
from letsdata_service.SparkMapperInterfaceService import getSparkMapperInterfaceServiceRequest
from letsdata_service.SparkReducerInterfaceService import getSparkReducerInterfaceServiceRequest

def getServiceRequest(event : dict) -> ServiceRequest:
    if event is None:
        raise(Exception("lambda event is null"))

    if not isinstance(event, dict):
        raise(Exception("lambda event is not a dictionary"))

    letsdata_assert(not event['requestId'] is None, "invalid requestId - None")
    letsdata_assert(isinstance(event['requestId'], str), "invalid requestId - value to be string")
    requestId = event['requestId']
    letsDataAuthParams = LetsDataAuthParams(event['letsdataAuth'])
    interfaceName = InterfaceNames.fromValue(event['interface'].lower())
    functionName = event['function']
    serviceRequest : ServiceRequest = None
    data : dict = event['data'] if 'data' in event.keys() else None
    batchedData : [] = event['batchedData'] if 'batchedData' in event.keys() else None
    if (interfaceName == InterfaceNames.SingleFileParser):
        serviceRequest = getSingleFileParserRequest(requestId, letsDataAuthParams, interfaceName, functionName, data, batchedData)
    elif (interfaceName == InterfaceNames.QueueMessageReader):
        serviceRequest = getQueueMessageReaderServiceRequest(requestId, letsDataAuthParams, interfaceName, functionName, data, batchedData)
    elif (interfaceName == InterfaceNames.SagemakerVectorsInterface):
        serviceRequest = getSagemakerVectorsInterfaceServiceRequest(requestId, letsDataAuthParams, interfaceName, functionName, data, batchedData)
    elif (interfaceName == InterfaceNames.KinesisRecordReader):
        serviceRequest = getKinesisRecordReaderServiceRequest(requestId, letsDataAuthParams, interfaceName, functionName, data, batchedData)
    elif (interfaceName == InterfaceNames.DynamoDBStreamsRecordReader):
        serviceRequest = getDynamoDBRecordReaderServiceRequest(requestId, letsDataAuthParams, interfaceName, functionName, data, batchedData)
    elif (interfaceName == InterfaceNames.DynamoDBTableItemReader):
        serviceRequest = getDynamoDBTableItemReaderServiceRequest(requestId, letsDataAuthParams, interfaceName, functionName, data, batchedData)
    elif (interfaceName == InterfaceNames.SparkMapperInterface):
        serviceRequest = getSparkMapperInterfaceServiceRequest(requestId, letsDataAuthParams, interfaceName, functionName, data, batchedData)
    elif (interfaceName == InterfaceNames.SparkReducerInterface):
        serviceRequest = getSparkReducerInterfaceServiceRequest(requestId, letsDataAuthParams, interfaceName, functionName, data, batchedData)
    else:
        raise(Exception("lambda event - interfaceName not yet supported "+str(interfaceName)))
    
    logger.debug("serviceRequest initialized - interfaceName: "+str(interfaceName)+", functionName: "+str(functionName))
    return serviceRequest
        