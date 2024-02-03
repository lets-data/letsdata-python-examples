from letsdata_utils.logging_utils import logger
from letsdata_service.Service import ServiceRequest, LetsDataAuthParams, InterfaceNames
from letsdata_utils.validations import letsdata_assert
from letsdata_interfaces.readers.spark.SparkReducerInterface import SparkReducerInterface


class SparkReducerInterfaceService_Reducer(ServiceRequest):
    def __init__(self, requestId : str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, appName: str, readDestination : str, readUris : [str], readFormat : str, readOptions : dict, writeDestination:str, writeUri : str, writeFormat: str, writeMode: str, writeOptions : dict, sparkCredentialsSecretArn : str):
        super().__init__(requestId, letsDataAuth, interfaceName, functionName)
        self.appName = appName 
        self.readDestination = readDestination 
        self.readUris = readUris
        self.readFormat = readFormat 
        self.readOptions = readOptions 
        self.writeDestination = writeDestination 
        self.writeUri = writeUri 
        self.writeFormat = writeFormat 
        self.writeMode = writeMode 
        self.writeOptions = writeOptions 
        self.sparkCredentialsSecretArn = sparkCredentialsSecretArn

    def execute(self):
        sparkReducer = SparkReducerInterface()
        return  sparkReducer.reducer(self.appName, self.readDestination, self.readUris, self.readFormat, self.readOptions, self.writeDestination, self.writeUri, self.writeFormat, self.writeMode, self.writeOptions, self.sparkCredentialsSecretArn)

def getSparkReducerInterfaceServiceRequest(requestId : str, letsDataAuth: LetsDataAuthParams, interfaceName : InterfaceNames, functionName : str, data : {}, batchedData : []):
    letsdata_assert(interfaceName == InterfaceNames.SparkReducerInterface, "invalid interfaceName - expected SparkReducerInterface, got "+str(interfaceName))
    letsdata_assert(letsDataAuth is not None, "invalid letsDataAuth - None")

    SparkReducerInterfaceInterfaceNames = set(["reducer"])    

    if functionName not in SparkReducerInterfaceInterfaceNames:
            raise(Exception("lambda event - invalid functionName "+functionName+" for interface SparkReducerInterface"))
    
    if functionName == "reducer":
        letsdata_assert(batchedData is None or len(batchedData) == 0, "invalid data - SparkReducerInterface.reducer requires empty batchedData dictionary")
        letsdata_assert(isinstance(data, dict) and len(data) == 11, "invalid data - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        
        letsdata_assert(not data['appName'] is None, "invalid appName - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['appName'], str), "invalid appName - SparkReducerInterface.reducer requires appName value to be str")

        letsdata_assert(not data['readDestination'] is None, "invalid readDestination - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['readDestination'], str), "invalid readDestination - SparkReducerInterface.reducer requires readDestination value to be str")

        letsdata_assert(not data['readUris'] is None, "invalid readUris - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['readUris'], list), "invalid readUris - SparkReducerInterface.reducer requires readUris value to be [str]")

        letsdata_assert(not data['readFormat'] is None, "invalid readFormat - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['readFormat'], str), "invalid readFormat - SparkReducerInterface.reducer requires readFormat value to be str")

        letsdata_assert(not data['readOptions'] is None, "invalid readOptions - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['readOptions'], dict), "invalid readOptions - SparkReducerInterface.reducer requires readOptions value to be dict")

        letsdata_assert(not data['writeDestination'] is None, "invalid writeDestination - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeDestination'], str), "invalid writeDestination - SparkReducerInterface.reducer requires writeDestination value to be str")

        letsdata_assert(not data['writeUri'] is None, "invalid writeUri - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeUri'], str), "invalid writeUri - SparkReducerInterface.reducer requires writeUri value to be str")

        letsdata_assert(not data['writeFormat'] is None, "invalid writeFormat - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeFormat'], str), "invalid writeFormat - SparkReducerInterface.reducer requires writeFormat value to be str")

        letsdata_assert(not data['writeMode'] is None, "invalid writeMode - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeMode'], str), "invalid writeMode - SparkReducerInterface.reducer requires writeMode value to be str")

        letsdata_assert(not data['writeOptions'] is None, "invalid writeOptions - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['writeOptions'], dict), "invalid writeOptions - SparkReducerInterface.reducer requires writeOptions value to be dict")

        letsdata_assert(not data['sparkCredentialsSecretArn'] is None, "invalid sparkCredentialsSecretArn - None - SparkReducerInterface.reducer requires data keys [appName, readDestination, readUris, readFormat, readOptions, writeDestination, writeUri, writeFormat, writeMode, writeOptions, sparkCredentialsSecretArn]")
        letsdata_assert(isinstance(data['sparkCredentialsSecretArn'], str), "invalid sparkCredentialsSecretArn - SparkReducerInterface.reducer requires sparkCredentialsSecretArn value to be str")

        return SparkReducerInterfaceService_Reducer(requestId, letsDataAuth, interfaceName, functionName, data['appName'], data['readDestination'], data['readUris'], data['readFormat'], data['readOptions'], data['writeDestination'], data['writeUri'], data['writeFormat'], data['writeMode'], data['writeOptions'], data['sparkCredentialsSecretArn'])
    else:
        raise(Exception("Unknown functionName"))    
