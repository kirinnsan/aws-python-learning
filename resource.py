import boto3


def create_aws_resource(resource_name):
    client = boto3.resource(resource_name)
    return client
