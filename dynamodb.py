from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import botostubs

import resource


def create_table(dynamodb: botostubs.DynamoDB, table_name, schema, attribute, throughput):
    """テーブル作成"""

    # create_table 呼び出しでは、テーブル名、プライマリキー属性、そのデータ型を指定します。
    try:
        table: botostubs.DynamoDB = dynamodb.create_table(
            TableName=table_name,
            KeySchema=schema,
            AttributeDefinitions=attribute,
            ProvisionedThroughput=throughput

        )
        # テーブル作成まで待機
        table.meta.client.get_waiter('table_exists').wait(TableName=table_name)

    except ClientError as e:
        print(e)

    return table


def delete_table(dynamodb: botostubs.DynamoDB, table_name):
    """テーブル削除"""

    table: botostubs.DynamoDB = dynamodb.Table(table_name)
    try:
        table.delete()
    except ClientError as e:
        print(e)


def put_item(dynamodb: botostubs.DynamoDB, table_name, item):
    """アイテムの作成"""

    table: botostubs.DynamoDB = dynamodb.Table(table_name)
    try:
        response = table.put_item(Item=item)
    except ClientError as e:
        print(e)

    return response


def batch_put_item(dynamodb: botostubs.DynamoDB, table_name):
    """アイテムをまとめて作成"""

    table: botostubs.DynamoDB = dynamodb.Table(table_name)

    try:
        with table.batch_writer() as batch:
            for i in range(50):
                batch.put_item(
                    Item={
                        'account_type': 'anonymous',
                        'username': 'user' + str(i),
                        'first_name': 'unknown',
                        'last_name': 'unknown'
                    }
                )
    except ClientError as e:
        print(e)


def get_item(dynamodb: botostubs.DynamoDB, table_name, key):
    """アイテムの取得"""

    table: botostubs.DynamoDB = dynamodb.Table(table_name)

    try:
        response = table.get_item(Key=key)
    except ClientError as e:
        print(e)

    return response


def get_item_by_query(dynamodb: botostubs.DynamoDB, table_name, key, param):
    """アイテムをクエリー検索して取得"""

    table: botostubs.DynamoDB = dynamodb.Table(table_name)

    try:
        response = table.query(
            KeyConditionExpression=Key(key).eq(param)
        )
    except ClientError as e:
        print(e)

    return response


def update_item(dynamodb: botostubs.DynamoDB, table_name, key,
                expression, value):
    """アイテムの更新"""

    table: botostubs.DynamoDB = dynamodb.Table(table_name)

    try:
        table.update_item(Key=key,
                          UpdateExpression=expression,
                          ExpressionAttributeValues=value)
    except ClientError as e:
        print(e)


# テーブル内のクエリ検索及びスキャン
# TODO
# スキャン検索
# response = table.scan(
#     FilterExpression=Attr('age').lt(27)
# )
# items = response['Items']
# print('-----スキャン結果-----')
# print(items)

# # 論理演算子を用いたスキャン検索
# response = table.scan(
#     FilterExpression=Attr('first_name').begins_with(
#         'J') & Attr('account_type').eq('super_user')
# )
# items = response['Items']
# print('-----論理演算子を用いた結果-----')
# print(items)

# # ネストされた属性に対してのスキャン検索
# response = table.scan(
#     FilterExpression=Attr('first_name').begins_with(
#         'J') & Attr('account_type').eq('super_user')
# )
# items = response['Items']
# print('-----ネストされた属性に対してのスキャン結果-----')
# print(items)

if __name__ == '__main__':
    # TODO エラーハンドリングはデコレータで共通化

    # リソースの取得
    dynamodb: botostubs.DynamoDB = resource.create_aws_resource('dynamodb')

    print('-----------テーブル作成-----------')
    tableName = 'users'
    schema = [
        {
            'AttributeName': 'username',
            'KeyType': 'HASH'  # パーテーションキー
        },
        {
            'AttributeName': 'last_name',
            'KeyType': 'RANGE'  # ソートキー
        }
    ]
    attributeDefinitions = [
        {
            'AttributeName': 'username',
            'AttributeType': 'S'
        },
        {
            'AttributeName': 'last_name',
            'AttributeType': 'S'
        },
    ]
    provisionedThroughput = {
        'ReadCapacityUnits': 5,
        'WriteCapacityUnits': 5
    }
    table = create_table(dynamodb, tableName, schema,
                         attributeDefinitions, provisionedThroughput)

    print('-----------アイテム作成-----------')
    item = {
        'username': 'janedoeasfdsa',
        'first_name': 'Jane',
        'last_name': 'Doe',
        'age': 26,
        'account_type': 'standard_user',
        'account_type1': 'standard_user1',
    }
    table = put_item(dynamodb, tableName, item)

    print('-----------アイテムをまとめて作成-----------')
    table = batch_put_item(dynamodb, tableName)

    print('-----------アイテム取得-----------')
    key = {
        'username': 'janedoeasfdsa',
        'last_name': 'Doe',
    }
    result = get_item(dynamodb, tableName, key)
    item = result['Item']
    print('-----アイテム情報一覧-----')
    print(result)
    print('-----アイテム-----')
    print(item)

    print('-----------アイテムのクエリー検索-----------')
    key = 'username'
    param = 'johndoe'
    result = get_item_by_query(dynamodb, tableName, key, param)
    item = result['Items']
    print('-----クエリ結果-----')
    print(item)

    print('-----------アイテム更新-----------')
    key = {
        'username': 'janedoeasfdsa',
        'last_name': 'Doe',
    }
    expression = 'SET age = :val1'
    set_value = {':val1': 30}
    result = update_item(dynamodb, tableName, key, expression, set_value)

    print('-----------テーブル削除-----------')
    delete_table(dynamodb, tableName)
