from __future__ import print_function

import boto3
import json

print('Loading function')


def lambda_handler(event, context):
    '''Provide an event that contains the following keys:

    - operation: one of the operations in the operations dict below
    - tableName: required for operations that interact with DynamoDB
    - payload: a parameter to pass to the operation being performed
    '''
    print("Received event: " + json.dumps(event, indent=2))

    print('------------')
    print(dir(context))
    print('------------')

    operation = event['operation']

    if 'tablename' in operation:
        dynamo_table = boto3.resource('dynamo').Table(event['tableName'])

    operations = {
        'create': lambda x: dynamo_table.put_item(**x),
        'read': lambda x: dynamo_table.get_item(**x),
        'update': lambda x: dynamo_table.update_item(**x),
        'delete': lambda x: dynamo_table.delete_item(**x),
        'list': lambda x: dynamo_table.scan(**x),
        'echo': lambda x: x,
        'ping': lambda x: 'pinggggggggg',
    }

    if operation in operations:
        return operations[operation](event.get('payload'))
    else:
        raise ValueError(f'Unrecognized operation:{operation}')
