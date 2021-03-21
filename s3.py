from botocore.exceptions import ClientError
import botostubs

import resource

REGION_NAME = 'ap-northeast-1'


def create_bucket(s3_resource: botostubs.S3, bucket_name, region=None):
    """バケットの作成"""

    if region is None:
        location = {'LocationConstraint': REGION_NAME}

    try:
        response: botostubs.S3 = s3_resource.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration=location
        )
    except ClientError as e:
        print('Error', e)
        return None

    return response


def list_all_bucket(s3: botostubs.S3):
    """バケット一覧表示"""

    for bucket in s3.buckets.all():
        print('バケット情報:', bucket)
        print('バケット名:', bucket.name)


def upload_file(s3_resource: botostubs.S3, bucket_name, file_name, object_name=None):
    """
    対象のバケットにファイルをアップロードする

    ：param s3_resource：S3のリソース
    ：param bucket_name : アップロードするバケット
    ：param file_name：アップロードするファイル
    ：param object_name：S3オブジェクト名。指定しない場合、file_nameが使用されます

    ：return：ファイルがアップロードされた場合はTrue,それ以外はFalse
    """

    if object_name is None:
        object_name = file_name

    try:
        s3_resource.meta.client.upload_file(
            file_name, bucket_name, object_name)
    except ClientError as e:
        print(e)
        return False

    return True


def download_file(s3_resource: botostubs.S3, bucket_name, file_name, object_name):
    """
    対象のバケットからファイルをダウンロードする

    ：param s3_resource：S3のリソース
    ：param bucket_name : ダウンロード対象のバケット
    ：param file_name：ダウンロードする時のファイル名
    ：param object_name：S3オブジェクト名。

    ：return：ファイルがダウンロードされた場合はTrue,それ以外はFalse
    """

    try:
        s3_resource.meta.client.download_file(
            bucket_name, object_name, file_name)
    except ClientError as e:
        print(e)
        return False

    return True


if __name__ == '__main__':
    # S3リソース作成
    s3_resource: botostubs.S3 = resource.create_aws_resource('s3')

    # バケット作成
    result = create_bucket(s3_resource, 'python-test-aaaaa')
    if result is not None:
        print(result)

    # バケット一覧取得
    list_all_bucket(s3_resource)

    # ファイルのアップロード
    result = upload_file(s3_resource, bucket_name='python-test-aaaaa',
                         file_name='resources/s3test.txt')
    print('ファイルアップロード結果')
    print(result)

    # ファイルのダウンロード
    result = download_file(s3_resource, bucket_name='python-test-aaaaa',
                           file_name='download.txt',
                           object_name='resources/s3test.txt'
                           )
    print('ファイルダウンロード結果')
    print(result)
