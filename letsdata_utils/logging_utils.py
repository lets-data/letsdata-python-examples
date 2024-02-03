import logging

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logging.getLogger('boto3').setLevel(logging.INFO)
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('botocore.session').setLevel(logging.INFO)
logging.getLogger('pyspark').setLevel(logging.INFO)
logging.getLogger("py4j").setLevel(logging.INFO)
