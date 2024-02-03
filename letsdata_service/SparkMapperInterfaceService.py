from letsdata_utils.logging_utils import logger
from letsdata_service.Service import ServiceRequest, LetsDataAuthParams, InterfaceNames
from letsdata_utils.validations import letsdata_assert
from letsdata_interfaces.readers.spark.SparkMapperInterface import SparkMapperInterface


class SparkMapperInterfaceService_Mapper(ServiceRequest):
    def __init__(self, requestId : str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, appName: str, readDestination : str, readUri : str, readFormat : str, readOptions : dict, writeDestination:str, writeUri : str, writeFormat: str, writeMode: str, writeOptions : dict, sparkCredentialsSecretArn : str):
        super().__init__(requestId, letsDataAuth, interfaceName, functionName)
        self.appName = appName 
        self.readDestination = readDestination 
        self.readUri = readUri 
        self.readFormat = readFormat 
        self.readOptions = readOptions 
        self.writeDestination = writeDestination 
        self.writeUri = writeUri 
        self.writeFormat = writeFormat 
        self.writeMode = writeMode 
        self.writeOptions = writeOptions 
        self.sparkCredentialsSecretArn = sparkCredentialsSecretArn

    def execute(self):
        sparkMapper = SparkMapperInterface()
        return sparkMapper.mapper(self.appName, self.readDestination, self.readUri, self.readFormat, self.readOptions, self.writeDestination, self.writeUri, self.writeFormat, self.writeMode, self.writeOptions, self.sparkCredentialsSecretArn)

def getSparkMapperInterfaceServiceRequest(requestId : str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, data : {}, batchedData : []):
    letsdata_assert(interfaceName == InterfaceNames.SparkMapperInterface, "invalid interfaceName - expected SparkMapperInterface, got "+str(interfaceName))
    letsdata_assert(letsDataAuth is not None, "invalid letsDataAuth - None")

    SparkMapperInterfaceInterfaceNames = set(["mapper"])    

    if functionName not in SparkMapperInterfaceInterfaceNames:
            raise(Exception("lambda event - invalid functionName "+functionName+" for interface SparkMapperInterface"))
    
    if functionName == "mapper":
        letsdata_assert(batchedData is None or len(batchedData) == 0, "invalid data - SparkMapperInterface.mapper requires empty batchedData dictionary")
        letsdata_assert(isinstance(data, dict) and len(data) == 11, "invalid data - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        
        letsdata_assert(not data['appName'] is None, "invalid appName - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['appName'], str), "invalid appName - SparkMapperInterface.mapper requires appName value to be str")

        letsdata_assert(not data['readDestination'] is None, "invalid readDestination - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['readDestination'], str), "invalid readDestination - SparkMapperInterface.mapper requires readDestination value to be str")

        letsdata_assert(not data['readUri'] is None, "invalid readUri - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['readUri'], str), "invalid readUri - SparkMapperInterface.mapper requires readUri value to be str")

        letsdata_assert(not data['readFormat'] is None, "invalid readFormat - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['readFormat'], str), "invalid readFormat - SparkMapperInterface.mapper requires readFormat value to be str")

        letsdata_assert(not data['readOptions'] is None, "invalid readOptions - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['readOptions'], dict), "invalid readOptions - SparkMapperInterface.mapper requires readOptions value to be dict")

        letsdata_assert(not data['writeDestination'] is None, "invalid writeDestination - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeDestination'], str), "invalid writeDestination - SparkMapperInterface.mapper requires writeDestination value to be str")

        letsdata_assert(not data['writeUri'] is None, "invalid writeUri - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeUri'], str), "invalid writeUri - SparkMapperInterface.mapper requires writeUri value to be str")

        letsdata_assert(not data['writeFormat'] is None, "invalid writeFormat - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeFormat'], str), "invalid writeFormat - SparkMapperInterface.mapper requires writeFormat value to be str")

        letsdata_assert(not data['writeMode'] is None, "invalid writeMode - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeMode'], str), "invalid writeMode - SparkMapperInterface.mapper requires writeMode value to be str")

        letsdata_assert(not data['writeOptions'] is None, "invalid writeOptions - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeOptions'], dict), "invalid writeOptions - SparkMapperInterface.mapper requires writeOptions value to be dict")

        letsdata_assert(not data['sparkCredentialsSecretArn'] is None, "invalid sparkCredentialsSecretArn - None - SparkMapperInterface.mapper requires data keys [appName, readDestination, readUri, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['sparkCredentialsSecretArn'], str), "invalid sparkCredentialsSecretArn - SparkMapperInterface.mapper requires sparkCredentialsSecretArn value to be str")

        return SparkMapperInterfaceService_Mapper(requestId, letsDataAuth, interfaceName, functionName, data['appName'], data['readDestination'], data['readUri'], data['readFormat'], data['readOptions'], data['writeDestination'], data['writeUri'], data['writeFormat'], data['writeMode'], data['writeOptions'], data['sparkCredentialsSecretArn'])
    else:
        raise(Exception("Unknown functionName"))    
