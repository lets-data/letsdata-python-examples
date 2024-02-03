import gzip, base64, json, os
from letsdata_utils.logging_utils import logger
from letsdata_utils.stage import Stage
from letsdata_utils.request_utils import return500ResponseFromException, getLambdaStageFromEnvironment, getJsonObject
from letsdata_service.RequestParser import getServiceRequest
from letsdata_service.Service import ServiceRequest

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
def lambda_handler(event, context):
    logger.debug("letsdata_lambda_function start - event: "+str(event))
    try:
        stage : Stage = getLambdaStageFromEnvironment()
        requestId : str = event['requestId']
        logger.debug("letsdata_lambda_function start - requestId: "+requestId)
        if event is None:
            raise(Exception("lambda event is null"))
        
        request : ServiceRequest = getServiceRequest(event)
        responseObj = request.execute()
        print("letsdata_lambda_function end - requestId: "+requestId+", response: "+str(responseObj))
        logger.debug("letsdata_lambda_function end - requestId: "+requestId+", response: "+str(responseObj))
        return {
            "StatusCode": 200,
            "headers": {
                "Content-Type": "application/json"
            },
            "body": {
                "statusCode": "SUCCESS",
                "data": getJsonObject(responseObj)
            }
        }
        
    except Exception as err:
        return return500ResponseFromException(err, None)

