# minio_adapter.py
from minio import Minio
from minio.error import S3Error


class MinioS3:
    def __init__(
        self,
        endpoint_url: str,
        access_key: str,
        secret_key: str,
        secure: bool = True,
    ):
        self.endpoint_url = endpoint_url
        self.access_key = access_key
        self.secret_key = secret_key

        self.client = Minio(
            endpoint_url,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
        )

    def upload_file(self, bucket_name: str, object_name: str, file_path: str):
        try:
            self.client.fput_object(bucket_name, object_name, file_path)
            print(f'Uploaded {file_path} to {bucket_name}/{object_name}')
        except S3Error as e:
            print(f'Error uploading file: {e}')

    def download_file(
        self,
        bucket_name: str,
        object_name: str,
        file_path: str,
    ):
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
            print(f'Downloaded {bucket_name}/{object_name} to {file_path}')
        except S3Error as e:
            print(f'Error downloading file: {e}')

    def list_objects(self, bucket_name: str, prefix: str = ''):
        try:
            return [
                obj.object_name
                for obj in self.client.list_objects(bucket_name, prefix=prefix)
            ]  # noqa: E501
        except S3Error as e:
            print(f'Error listing objects: {e}')
            return []

    def create_bucket_if_not_exists(self, bucket_name: str):
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                print(f'Bucket {bucket_name} created')
            else:
                print(f'Bucket {bucket_name} already exists')
        except S3Error as e:
            print(f'Error creating bucket: {e}')
