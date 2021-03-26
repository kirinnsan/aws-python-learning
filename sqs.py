from botocore.exceptions import ClientError
import botostubs

import resource

RESOURCE_NAME_SQS = 'sqs'


def create_queue(sqs_resource: botostubs.SQS, name):
    """キューの作成"""

    try:
        queue = sqs_resource.create_queue(QueueName=name)
    except ClientError as e:
        print('Error', e)

    return queue


def list_all_queue(sqs_resource: botostubs.SQS):
    """キューの一覧表示"""

    for q in sqs_resource.queues.all():
        print(q.url)
        print(q.attributes['QueueArn'].split(':')[-1])


def get_queue(sqs_resource: botostubs.SQS, name):
    """対象のキューを取得"""

    try:
        queue: botostubs.SQS = sqs_resource.get_queue_by_name(QueueName=name)
    except ClientError as e:
        print('Error', e)

    return queue


def send_message(queue: botostubs.SQS, message, attribute=None):
    """キューにメッセージを送信"""

    try:
        if attribute is not None:
            response = queue.send_message(
                MessageBody=message,
                MessageAttributes=attribute
            )
        else:
            response = queue.send_message(
                MessageBody=message,
            )
    except ClientError as e:
        print('Error', e)

    return response


def send_messages(queue: botostubs.SQS, message_list):
    """メッセージをまとめて送信"""

    try:
        response = queue.send_messages(Entries=message_list)
    except ClientError as e:
        print('Error', e)

    return response


def receive_message(queue: botostubs.SQS):
    """キュー内のメッセージ受信"""

    try:
        message_list = queue.receive_messages(MaxNumberOfMessages=10)
    except ClientError as e:
        print(e)

    # キュー内のメッセージの数が少ない場合（1,000未満）、
    # ReceiveMessage呼び出しごとに要求したよりも少ない少ないメッセージを受け取る可能性があります。
    # キュー内のメッセージの数が非常に少ない場合、特定のReceiveMessage応答でメッセージを受信しない可能性があります。
    # これが発生した場合は、リクエストを繰り返してください。
    for message in message_list:
        print(message)
        # カスタム作成者属性が設定されている場合は取得する
        author_text = ''
        if message.message_attributes is not None:
            authour_name = message.message_attributes.get(
                'Author').get('StringValue')
            if authour_name:
                author_text = f'({authour_name})'

        print(f'messageBody:{message.body}, authorText:{author_text}')

        # 処理されたメッセージの削除
        message.delete()


def delete_queues(queues):
    """キューの削除"""

    for queue in queues:
        queue.delete()


if __name__ == '__main__':
    # SQSのService Resourceを使用したサンプル

    # SQSリソース作成
    sqs: botostubs.SQS = resource.create_aws_resource('sqs')

    print('-----------キューの作成-----------')
    queue_name1 = 'test1'
    queue_name2 = 'test2'
    queue1 = create_queue(sqs, queue_name1)
    queue2 = create_queue(sqs, queue_name2)

    if queue1 is not None:
        print('----作成したキューの識別子と属性----')
        print(queue1.url)
        print(queue1.attributes.get('DelaySeconds'))
    if queue2 is not None:
        print('----作成したキューの識別子と属性----')
        print(queue2.url)
        print(queue2.attributes.get('DelaySeconds'))

    print('-----------キューの一覧表示-----------')
    list_all_queue(sqs)

    print('-----------メッセージ送信-----------')
    result = send_message(queue1, 'テストメッセージ')
    if result is not None:
        print(result.get('MessageId'))
        print(result.get('MD5OfMessageBody'))

    print('-----------メッセージをまとめて送信-----------')
    message_list = [
        {
            'Id': '1',
            'MessageBody': 'world1'
        },
        {
            'Id': '2',
            'MessageBody': 'boto3',
            'MessageAttributes': {
                'Author': {
                    'StringValue': 'TestUser',
                    'DataType': 'String'
                }
            }
        }
    ]
    results = send_messages(queue2, message_list)
    # レスポンスには、成功メッセージと失敗メッセージのリストが
    # 入っているため、必要に応じてリトライ可能
    print(results.get('Successful'))
    print(results.get('Failed'))

    print('-----------メッセージ受信-----------')
    receive_message(queue2)

    print('存在するキュー削除')
    queues = [queue1, queue2]
    delete_queues(queues)