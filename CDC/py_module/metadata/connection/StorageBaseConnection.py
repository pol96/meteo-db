from google.cloud import storage
from google.cloud.storage import Blob
from google.oauth2 import service_account

class StorageConn:
    def __init__(self, credential_path: str | None = None):
        if credential_path:
            credentials = service_account.Credentials.from_service_account_file(credential_path)
            self.client = storage.Client(credentials=credentials)
        else:
            self.client = storage.Client()
        self.project = self.client.project

    def get_bucket(self, bucket_name: str):
        bucket = self.client.get_bucket(bucket_name)  # or use .bucket(bucket_name)
        return bucket

    def define_blob(self, blob_name: str, bucket_name: str):
        bucket = self.get_bucket(bucket_name=bucket_name)
        blob = Blob(blob_name, bucket)
        return blob