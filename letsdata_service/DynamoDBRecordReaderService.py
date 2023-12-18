
from letsdata_utils.logging_utils import logger
from letsdata_service.Service import ServiceRequest, LetsDataAuthParams, InterfaceNames
from letsdata_utils.validations import letsdata_assert
from letsdata_interfaces.readers.dynamodbstreams.DynamoDBStreamsRecordReader import DynamoDBStreamsRecordReader
    
class DynamoDBRecordReaderService_ParseRecord(ServiceRequest):
    def __init__(self, requestId: str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, streamArn : str, shardId : str, eventId : str, eventName : str, identityPrincipalId : str, identityType : str, sequenceNumber : str, sizeBytes : int, streamViewType : str, approximateCreationDateTime : int, keys : {}, oldImage : {}, newImage : {}) -> None: 
        super().__init__(requestId, letsDataAuth, interfaceName, functionName)
        self.streamArn = streamArn
        self.shardId = shardId
        self.eventId = eventId
        self.eventName = eventName
        self.identityPrincipalId = identityPrincipalId
        self.identityType = identityType
        self.sequenceNumber = sequenceNumber
        self.sizeBytes = sizeBytes
        self.streamViewType = streamViewType
        self.approximateCreationDateTime = approximateCreationDateTime
        self.keys = keys
        self.oldImage = oldImage
        self.newImage = newImage
        
    def execute(self): 
        parser = DynamoDBStreamsRecordReader()
        return  parser.parseRecord(self.streamArn, self.shardId, self.eventId, self.eventName, self.identityPrincipalId, self.identityType, self.sequenceNumber, self.sizeBytes, self.streamViewType, self.approximateCreationDateTime, self.keys, self.oldImage, self.newImage)

def getDynamoDBRecordReaderServiceRequest(requestId : str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, data : {}, batchedData : []):
    letsdata_assert(interfaceName == InterfaceNames.DynamoDBStreamsRecordReader, "invalid interfaceName - expected DynamoDBStreamsRecordReader, got "+str(interfaceName))
    letsdata_assert(letsDataAuth is not None, "invalid letsDataAuth - None")
    
    DynamoDBStreamsRecordReaderInterfaceNames = set(["parseRecord"])    

    if functionName not in DynamoDBStreamsRecordReaderInterfaceNames:
            raise(Exception("lambda event - invalid functionName "+functionName+" for interface DynamoDBStreamsRecordReader"))
    
    if functionName == "parseRecord":
        letsdata_assert(batchedData is None or len(batchedData) == 0, "invalid data - DynamoDBStreamsRecordReader.parseRecord requires empty batchedData dictionary")
        letsdata_assert(isinstance(data, dict) and len(data) == 11, "invalid data - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        
        letsdata_assert(not data['streamArn'] is None, "invalid streamArn - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['streamArn'], str), "invalid streamArn - DynamoDBStreamsRecordReader.parseRecord requires streamArn value to be string")

        letsdata_assert(not data['shardId'] is None, "invalid shardId - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['shardId'], str), "invalid shardId - DynamoDBStreamsRecordReader.parseRecord requires shardId value to be string")

        letsdata_assert(not data['eventId'] is None, "invalid eventId - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['eventId'], str), "invalid eventId - DynamoDBStreamsRecordReader.parseRecord requires eventId value to be string")

        letsdata_assert(not data['eventName'] is None, "invalid eventName - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['eventName'], str), "invalid eventName - DynamoDBStreamsRecordReader.parseRecord requires eventName value to be string")

        if not data['identityPrincipalId'] is None:
            letsdata_assert(isinstance(data['identityPrincipalId'], str), "invalid identityPrincipalId - DynamoDBStreamsRecordReader.parseRecord requires identityPrincipalId value to be string")

        if not data['identityType'] is None:
            letsdata_assert(isinstance(data['identityType'], str), "invalid identityType - DynamoDBStreamsRecordReader.parseRecord requires identityType value to be string")

        letsdata_assert(not data['sequenceNumber'] is None, "invalid sequenceNumber - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['sequenceNumber'], str), "invalid sequenceNumber - DynamoDBStreamsRecordReader.parseRecord requires sequenceNumber value to be string")

        letsdata_assert(not data['sizeBytes'] is None, "invalid sizeBytes - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['sizeBytes'], int), "invalid sizeBytes - DynamoDBStreamsRecordReader.parseRecord requires sizeBytes value to be int")

        letsdata_assert(not data['approximateCreationDateTime'] is None, "invalid approximateCreationDateTime - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['approximateCreationDateTime'], int), "invalid approximateCreationDateTime - DynamoDBStreamsRecordReader.parseRecord requires approximateCreationDateTime value to be int")
        
        letsdata_assert(not data['streamViewType'] is None, "invalid streamViewType - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['streamViewType'], str), "invalid streamViewType - DynamoDBStreamsRecordReader.parseRecord requires streamViewType value to be string")

        letsdata_assert(not data['data'] is None, "invalid data['data'] - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['data'], dict) and len(data['data']) == 3, "invalid data['data'] - DynamoDBStreamsRecordReader.parseRecord requires data['data'] keys [keys, oldImage, newImage]")

        letsdata_assert(not data['data']['keys'] is None, "invalid data['data']['keys'] - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['data']['keys'], dict) and len(data['data']) > 0, "invalid data['data']['keys'] - DynamoDBStreamsRecordReader.parseRecord requires data['data'] keys [keys, oldImage, newImage]")

        letsdata_assert(not data['data']['oldImage'] is None, "invalid data['data']['oldImage'] - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['data']['oldImage'], dict), "invalid data['data']['oldImage'] - DynamoDBStreamsRecordReader.parseRecord requires data['data'] keys [keys, oldImage, newImage]")

        letsdata_assert(not data['data']['newImage'] is None, "invalid data['data']['newImage'] - None - DynamoDBStreamsRecordReader.parseRecord requires data keys [streamArn,shardId,eventId,eventName,identityPrincipalId,identityType,sequenceNumber,sizeBytes,streamViewType,approximateCreationDateTime,data]")
        letsdata_assert(isinstance(data['data']['newImage'], dict), "invalid data['data']['newImage'] - DynamoDBStreamsRecordReader.parseRecord requires data['data'] keys [keys, oldImage, newImage]")
        
        return DynamoDBRecordReaderService_ParseRecord(requestId, letsDataAuth, interfaceName, functionName, data['streamArn'], data['shardId'],  data['eventId'], data['eventName'], data['identityPrincipalId'], data['identityType'], data['sequenceNumber'],data['sizeBytes'],data['streamViewType'],data['approximateCreationDateTime'],data['data']['keys'],data['data']['oldImage'],data['data']['newImage'])
    else:
        raise(Exception("Unknown functionName"))
    
    