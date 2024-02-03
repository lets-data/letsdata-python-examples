from letsdata_utils.logging_utils import logger
from letsdata_service.Service import ServiceRequest, LetsDataAuthParams, InterfaceNames
from letsdata_utils.validations import letsdata_assert
from letsdata_interfaces.readers.parsers.SingleFileParser import SingleFileParser


class SingleFileParser_GetS3FileType(ServiceRequest):
    def __init__(self, requestId: str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str) -> None:
        super().__init__(requestId, letsDataAuth, interfaceName, functionName)

    def execute(self) -> object:
        parser = SingleFileParser()
        return  parser.getS3FileType()

class SingleFileParser_GetResolvedS3FileName(ServiceRequest):
    def __init__(self, requestId: str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, s3FileType : str, fileName : str) -> None:
        super().__init__(requestId, letsDataAuth, interfaceName, functionName)
        self.s3FileType = s3FileType
        self.fileName = fileName
    
    def execute(self) -> object:
        parser = SingleFileParser()
        return  parser.getResolvedS3FileName(self.s3FileType, self.fileName)
    
class SingleFileParser_GetRecordStartPattern(ServiceRequest):
    def __init__(self, requestId: str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, s3FileType : str) -> None:
        super().__init__(requestId, letsDataAuth, interfaceName, functionName)
        self.s3FileType = s3FileType

    def execute(self) -> object:
        parser = SingleFileParser()
        return  parser.getRecordStartPattern(self.s3FileType)

class SingleFileParser_GetRecordEndPattern(ServiceRequest):
    def __init__(self, requestId: str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, s3FileType : str) -> None:
        super().__init__(requestId, letsDataAuth, interfaceName, functionName)
        self.s3FileType = s3FileType

    def execute(self) -> object:
        parser = SingleFileParser()
        return  parser.getRecordEndPattern(self.s3FileType)
    
class SingleFileParser_ParseDocument(ServiceRequest):
    def __init__(self, requestId: str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, s3FileType : str, s3Filename : str, offsetBytes : int , content : str, startIndex : int, endIndex : int) -> None:
        super().__init__(requestId, letsDataAuth, interfaceName, functionName)
        self.s3FileType = s3FileType
        self.s3FileName = s3Filename
        self.offsetBytes = offsetBytes
        contentBytes = bytearray(content, encoding="utf-8")
        self.byteArr = contentBytes
        self.startIndex = 0
        if startIndex != self.startIndex:
            raise(Exception("SingleFileParser_ParseDocument - invalid startIndex for byteArray - expected: "+str(self.startIndex)+", actual: "+str(startIndex)))
        self.endIndex = len(contentBytes)
        if endIndex != self.endIndex:
            raise(Exception("SingleFileParser_ParseDocument - invalid endIndex for byteArray - expected: "+str(self.endIndex)+", actual: "+str(endIndex)))

    def execute(self) -> object:
        parser = SingleFileParser()
        return  parser.parseDocument(self.s3FileType, self.s3FileName, self.offsetBytes, self.byteArr, self.startIndex, self.endIndex)


def getSingleFileParserRequest(requestId : str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, data : dict, batchedData : []):
    letsdata_assert(interfaceName == InterfaceNames.SingleFileParser, "invalid interfaceName - expected SingleFileParser, got "+str(interfaceName))
    letsdata_assert(letsDataAuth is not None, "invalid letsDataAuth - None")
    SingleFileParserInterfaceNames = set(["getS3FileType", "getResolvedS3FileName", "getRecordStartPattern", "getRecordEndPattern", "parseDocument"])    

    if functionName not in SingleFileParserInterfaceNames:
            raise(Exception("lambda event - invalid functionName "+functionName+" for interface SingleFileParser"))
    
    if functionName == "getS3FileType":
        letsdata_assert(data is None or len(data) == 0, "invalid data - SingleFileParser.getS3FileType requires empty data dictionary")
        letsdata_assert(batchedData is None or len(batchedData) == 0, "invalid data - SingleFileParser.getS3FileType requires empty batchedData dictionary")
        return SingleFileParser_GetS3FileType(requestId, letsDataAuth, interfaceName, functionName)
    
    elif functionName == "getResolvedS3FileName":
        letsdata_assert(batchedData is None or len(batchedData) == 0, "invalid data - SingleFileParser.getResolvedS3FileName requires empty batchedData dictionary")
        letsdata_assert(isinstance(data, dict) and len(data) == 2, "invalid data - SingleFileParser.getResolvedS3FileName requires data keys [s3FileType, fileName]")
        
        letsdata_assert(not data['s3FileType'] is None, "invalid s3FileType - None - SingleFileParser.getResolvedS3FileName requires data keys [s3FileType, fileName]")
        letsdata_assert(isinstance(data['s3FileType'], str), "invalid s3FileType - SingleFileParser.getResolvedS3FileName requires s3FileType value to be string")

        letsdata_assert(not data['fileName'] is None, "invalid fileName - None - SingleFileParser.getResolvedS3FileName requires data keys [s3FileType, fileName]")
        letsdata_assert(isinstance(data['fileName'], str), "invalid fileName - SingleFileParser.getResolvedS3FileName requires fileName value to be string")
        return SingleFileParser_GetResolvedS3FileName(requestId, letsDataAuth, interfaceName, functionName, data['s3FileType'], data['fileName'])
    
    elif functionName == "getRecordStartPattern":
        letsdata_assert(batchedData is None or len(batchedData) == 0, "invalid data - SingleFileParser.getRecordStartPattern requires empty batchedData dictionary")
        letsdata_assert(isinstance(data, dict) and len(data) == 1, "invalid data - SingleFileParser.getRecordStartPattern requires data keys [s3FileType]")
        
        letsdata_assert(not data['s3FileType'] is None, "invalid s3FileType - None - SingleFileParser.getRecordStartPattern requires data keys [s3FileType]")
        letsdata_assert(isinstance(data['s3FileType'], str), "invalid s3FileType - SingleFileParser.getRecordStartPattern requires s3FileType value to be string")
        return SingleFileParser_GetRecordStartPattern(requestId, letsDataAuth, interfaceName, functionName, data['s3FileType'])
            
    elif functionName == "getRecordEndPattern":
        letsdata_assert(batchedData is None or len(batchedData) == 0, "invalid data - SingleFileParser.getRecordEndPattern requires empty batchedData dictionary")
        letsdata_assert(isinstance(data, dict) and len(data) == 1, "invalid data - SingleFileParser.getRecordEndPattern requires data keys [s3FileType]")
        
        letsdata_assert(not data['s3FileType'] is None, "invalid s3FileType - None - SingleFileParser.getRecordStartPattern requires data keys [s3FileType]")
        letsdata_assert(isinstance(data['s3FileType'], str), "invalid s3FileType - SingleFileParser.getRecordStartPattern requires s3FileType value to be string")
        return SingleFileParser_GetRecordEndPattern(requestId, letsDataAuth, interfaceName, functionName, data['s3FileType'])
    
    elif functionName == "parseDocument":
        letsdata_assert(batchedData is None or len(batchedData) == 0, "invalid data - SingleFileParser.parseDocument requires empty batchedData dictionary")
        letsdata_assert(isinstance(data, dict) and len(data) == 6, "invalid data - SingleFileParser.parseDocument requires data keys [s3FileType, fileName, offsetBytes, content, startIndex, endIndex]")
        
        letsdata_assert(not data['s3FileType'] is None, "invalid s3FileType - None - SingleFileParser.parseDocument requires data keys [s3FileType, fileName, offsetBytes, content, startIndex, endIndex]")
        letsdata_assert(isinstance(data['s3FileType'], str), "invalid s3FileType - SingleFileParser.parseDocument requires s3FileType value to be string")

        letsdata_assert(not data['fileName'] is None, "invalid fileName - None - SingleFileParser.parseDocument requires data keys [s3FileType, fileName, offsetBytes, content, startIndex, endIndex]")
        letsdata_assert(isinstance(data['fileName'], str), "invalid fileName - SingleFileParser.parseDocument requires fileName value to be string")
        
        letsdata_assert(not data['offsetBytes'] is None, "invalid offsetBytes - None - SingleFileParser.parseDocument requires data keys [s3FileType, fileName, offsetBytes, content, startIndex, endIndex]")
        letsdata_assert(isinstance(data['offsetBytes'], int), "invalid offsetBytes - SingleFileParser.parseDocument requires offsetBytes value to be int")
        
        letsdata_assert(not data['content'] is None, "invalid content - None - SingleFileParser.parseDocument requires data keys [s3FileType, fileName, offsetBytes, content, startIndex, endIndex]")
        letsdata_assert(isinstance(data['content'], str), "invalid content - SingleFileParser.parseDocument requires content value to be str")

        letsdata_assert(not data['startIndex'] is None, "invalid startIndex - None - SingleFileParser.parseDocument requires data keys [s3FileType, fileName, offsetBytes, content, startIndex, endIndex]")
        letsdata_assert(isinstance(data['startIndex'], int), "invalid startIndex - SingleFileParser.parseDocument requires startIndex value to be int")

        letsdata_assert(not data['endIndex'] is None, "invalid startIndex - None - SingleFileParser.parseDocument requires data keys [s3FileType, fileName, offsetBytes, content, startIndex, endIndex]")
        letsdata_assert(isinstance(data['endIndex'], int), "invalid startIndex - SingleFileParser.parseDocument requires endIndex value to be int")        
        
        return SingleFileParser_ParseDocument(requestId, letsDataAuth, interfaceName, functionName, data['s3FileType'], data['fileName'], data['offsetBytes'], data['content'], data['startIndex'], data['endIndex'])

    else:
        raise(Exception())

    