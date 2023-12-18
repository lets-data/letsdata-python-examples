
from letsdata_utils.logging_utils import logger
from letsdata_service.Service import ServiceRequest, LetsDataAuthParams, InterfaceNames
from letsdata_utils.validations import letsdata_assert
from letsdata_interfaces.readers.dynamodb.DynamoDBTableItemReader import DynamoDBTableItemReader
    
class DynamoDBTableItemReaderService_ParseDynamoDBItem(ServiceRequest):
    def __init__(self, requestId: str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, tableName : str, segmentNumber : int, keys : {}, item : {}) -> None: 
        super().__init__(requestId, letsDataAuth, interfaceName, functionName)
        self.tableName = tableName
        self.segmentNumber = segmentNumber
        self.keys = keys
        self.item = item
        
    def execute(self): 
        parser = DynamoDBTableItemReader()
        return  parser.parseDynamoDBItem(self.tableName, self.segmentNumber, self.keys, self.item)

def getDynamoDBTableItemReaderServiceRequest(requestId : str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, data : {}, batchedData : []):
    letsdata_assert(interfaceName == InterfaceNames.DynamoDBTableItemReader, "invalid interfaceName - expected DynamoDBTableItemReader, got "+str(interfaceName))
    letsdata_assert(letsDataAuth is not None, "invalid letsDataAuth - None")
    
    DynamoDBTableItemReaderInterfaceNames = set(["parseDynamoDBItem"])    

    if functionName not in DynamoDBTableItemReaderInterfaceNames:
            raise(Exception("lambda event - invalid functionName "+functionName+" for interface DynamoDBTableItemReader"))
    
    if functionName == "parseDynamoDBItem":
        letsdata_assert(batchedData is None or len(batchedData) == 0, "invalid data - DynamoDBTableItemReader.parseDynamoDBItem requires empty batchedData dictionary")
        letsdata_assert(isinstance(data, dict) and len(data) == 3, "invalid data - DynamoDBTableItemReader.parseDynamoDBItem requires data keys [tableName,segmentNumber,data]")
        
        letsdata_assert(not data['tableName'] is None, "invalid tableName - None - DynamoDBTableItemReader.parseDynamoDBItem requires data keys [tableName,segmentNumber,data]")
        letsdata_assert(isinstance(data['tableName'], str), "invalid tableName - DynamoDBTableItemReader.parseDynamoDBItem requires tableName value to be string")

        letsdata_assert(not data['segmentNumber'] is None, "invalid segmentNumber - None - DynamoDBTableItemReader.parseDynamoDBItem requires data keys [tableName,segmentNumber,data]")
        letsdata_assert(isinstance(data['segmentNumber'], int), "invalid segmentNumber - DynamoDBTableItemReader.parseDynamoDBItem requires segmentNumber value to be int")
        
        letsdata_assert(not data['data'] is None, "invalid data['data'] - None - DynamoDBTableItemReader.parseDynamoDBItem requires data keys [tableName,segmentNumber,data]")
        letsdata_assert(isinstance(data['data'], dict) and len(data['data']) == 2, "invalid data['data'] - DynamoDBTableItemReader.parseDynamoDBItem requires data['data'] keys [keys, item]")

        letsdata_assert(not data['data']['keys'] is None, "invalid data['data']['keys'] - None - DynamoDBTableItemReader.parseDynamoDBItem requires data keys [tableName,segmentNumber,data]")
        letsdata_assert(isinstance(data['data']['keys'], dict) and len(data['data']) > 0, "invalid data['data']['keys'] - DynamoDBTableItemReader.parseDynamoDBItem requires data['data'] keys [keys, item]")

        letsdata_assert(not data['data']['item'] is None, "invalid data['data']['item'] - None - DynamoDBTableItemReader.parseDynamoDBItem requires data keys [tableName,segmentNumber,data]")
        letsdata_assert(isinstance(data['data']['item'], dict), "invalid data['data']['item'] - DynamoDBTableItemReader.parseDynamoDBItem requires data['data'] keys [keys, item]")

        return DynamoDBTableItemReaderService_ParseDynamoDBItem(requestId, letsDataAuth, interfaceName, functionName, data['tableName'], data['segmentNumber'], data['data']['keys'],data['data']['item'])
    else:
        raise(Exception("Unknown functionName"))
    
    