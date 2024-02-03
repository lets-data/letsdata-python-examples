from urllib.parse import urlparse

def parse_arn(arn):
    # http://docs.aws.amazon.com/general/latest/gr/aws-arns-and-namespaces.html
    elements = arn.split(':', 5)
    result = {
        'arn': elements[0],
        'partition': elements[1],
        'service': elements[2],
        'region': elements[3],
        'account': elements[4],
        'resource': elements[5],
        'resource_type': None
    }
    if '/' in result['resource']:
        result['resource_type'], result['resource'] = result['resource'].split('/',1)
    elif ':' in result['resource']:
        result['resource_type'], result['resource'] = result['resource'].split(':',1)
    return result

def parse_bucket_name_from_s3_uri(s3_uri):
    parsed_uri = urlparse(s3_uri)
    if parsed_uri.scheme == 's3a' and parsed_uri.netloc:
        return parsed_uri.netloc
    else:
        raise ValueError(f"Not a valid S3 URI: {s3_uri}")