import boto3
import json
from botocore.client import Config
from botocore.exceptions import NoCredentialsError
from letsdata_utils.logging_utils import logger
from typing import Type, Dict, Any

class SecretManagerUtil:
    client_configuration = Config(
        max_pool_connections=350,
        connect_timeout=50,
        read_timeout=120,
        retries={'max_attempts': 0},
        tcp_keepalive=True
    )
    
    instance_map = {}

    def __init__(self, region: str):
        self.aws_secrets_manager = boto3.client('secretsmanager', region_name=region, config=self.client_configuration)
        self.cache = {}

    @classmethod
    def get_instance(cls, region: str) -> 'SecretManagerUtil':
        if region not in cls.instance_map:
            cls.instance_map[region] = cls(region)
        return cls.instance_map[region]

    def get_secret_value_string(self, secret_arn: str) -> str:
        return self.get_secrets_value(secret_arn)

    def get_secrets_value(self, secret_arn: str) -> str:
        cached_value = self.cache.get(secret_arn)
        if cached_value is not None:
            return cached_value

        try:
            result = self.aws_secrets_manager.get_secret_value(SecretId=secret_arn)
        except Exception as e:
            raise RuntimeError("Get secrets value threw exception", e)

        secret_string = result['SecretString']
        self.cache[secret_arn] = secret_string
        return secret_string

    @classmethod
    def get_spark_aws_credentials(cls, region: str, spark_credentials_secret_arn: str, method_name: str, destination_type: str) -> Dict[str, Any]:
        instance = cls.get_instance(region)
        secret_value = instance.get_secrets_value(spark_credentials_secret_arn)
        credentials_map = json.loads(secret_value)

        return {
            'AWS_SESSION_TOKEN': credentials_map[method_name][destination_type]['AWS_SESSION_TOKEN'],
            'AWS_ACCESS_KEY_ID': credentials_map[method_name][destination_type]['AWS_ACCESS_KEY_ID'],
            'AWS_SECRET_ACCESS_KEY': credentials_map[method_name][destination_type]['AWS_SECRET_ACCESS_KEY']
        }
