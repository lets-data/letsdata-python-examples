
import uuid
from letsdata_interfaces.readers.model.RecordParseHint import RecordParseHint
from letsdata_interfaces.readers.model.RecordHintType import RecordHintType
from letsdata_interfaces.readers.model.ParseDocumentResultStatus import ParseDocumentResultStatus
from letsdata_interfaces.readers.model.ParseDocumentResult import ParseDocumentResult
from letsdata_interfaces.documents.Document import Document
from letsdata_interfaces.documents.DocumentType import DocumentType
from letsdata_interfaces.documents.ErrorDoc import ErrorDoc
from letsdata_interfaces.documents.SkipDoc import SkipDoc
from letsdata_utils.logging_utils import logger

class DynamoDBStreamsRecordReader:
    def __init__(self) -> None:
        pass
        
    '''
     An example implementation that simply echoes the incoming record. You could add custom logic as needed.

     For detailed explanation of the parameters, see AWS docs:
        * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_Record.html
        * https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_streams_StreamRecord.html
     
     Parameters
     ----------
     streamArn : str
                      The Kinesis streamArn
     shardId   : str
                      The DynamoDB Shard Id
     eventId : str
                      A globally unique identifier for the event that was recorded in this stream record.
     eventName : str
                      The type of data modification that was performed on the DynamoDB table. INSERT | MODIFY | REMOVE
     identityPrincipalId : str
                      The userIdentity's principalId
     identityType : str
                      The userIdentity's principalType
     sequenceNumber : str
                      The sequence number of the stream record
     sizeBytes : str
                      The size of the stream record, in bytes
     streamViewType : str
                      The stream view type - NEW_IMAGE | OLD_IMAGE | NEW_AND_OLD_IMAGES | KEYS_ONLY
     approximateCreationDateTime : int 
                      The approximate date and time when the stream record was created, in UNIX epoch time format and rounded down to the closest second
     keys : str
                      The primary key attribute(s) for the DynamoDB item that was modified
     oldImage : str
                      The item in the DynamoDB table as it appeared before it was modified
     newImage : str
                      The item in the DynamoDB table as it appeared after it was modified
     
     Returns
     -------
     ParseDocumentResult 
        ParseDocumentResult has the extracted document and the status (error, success or skip)
    '''
    def parseRecord(self, streamArn : str, shardId : str, eventId : str, eventName : str, identityPrincipalId : str, identityType : str, sequenceNumber : str, sizeBytes : int, streamViewType : str, approximateCreationDateTime : int, keys : {}, oldImage : {}, newImage : {}) -> ParseDocumentResult:
        try:
            docId : str = None
            for keyValue in keys.values():
                if docId is None:
                    docId = keyValue
                else:
                    docId += ('|'+keyValue)
            
            if newImage is None or len(newImage) <= 0:
                logger.error(f"newImage is null, returning skip - streamArn: {streamArn}, shardId: {shardId}, eventName: {eventName}, sequenceNumber: {sequenceNumber}, approximateArrivalTimestamp: {approximateCreationDateTime}, keys: {keys}")
                error_doc = SkipDoc(docId, "DYNAMODBSTREAMS_SKIP", docId, {}, {}, {"sequenceNumber": sequenceNumber}, {"sequenceNumber": sequenceNumber}, "delete record")
                return ParseDocumentResult(None, error_doc, "SKIP")

        
            logger.debug(f"processing record - sequenceNumber: {sequenceNumber}")
            
            logger.debug(f"returning success - docId: {docId}")
            return ParseDocumentResult(None, Document(DocumentType.Document, docId, "DOCUMENT", docId, {}, newImage), "SUCCESS")
        except Exception as ex:
            logger.debug(f"Exception in reading the document - streamArn: {streamArn}, shardId: {shardId}, eventName: {eventName}, sequenceNumber: {sequenceNumber}, approximateArrivalTimestamp: {approximateCreationDateTime}, keys: {keys}, ex: {ex}")
            docIdUUID = str(uuid.uuid4())
            error_doc = ErrorDoc(docIdUUID, "KINESIS_ERROR", docIdUUID, {}, {}, {"sequenceNumber": sequenceNumber}, {"sequenceNumber": sequenceNumber}, f"Exception - {ex}")
            return ParseDocumentResult(None, error_doc, "ERROR")
    

