from google.cloud import storage
from google.oauth2 import service_account
from typing import Optional
from py_module.metadata.logging.logger import BaseLogger


class StorageConn(BaseLogger):
    def __init__(self, credential_path: Optional[str] = None, project: Optional[str] = None):
        """
        Initialize the Google Cloud Storage client.
        """
        super().__init__()
        try:
            if credential_path:
                credentials = service_account.Credentials.from_service_account_file(credential_path)
                self.client = storage.Client(credentials=credentials, project=project)
                self.logger.info("Initialized GCS client with service account.")
            else:
                self.client = storage.Client(project=project)
                self.logger.info("Initialized GCS client with default credentials.")
            self._project = project
        except Exception as e:
            self.logger.exception(f"Failed to initialize GCS client: {e}")
            raise

    @property
    def project(self) -> str:
        try:
            if self._project is None:
                self._project = self.client.project  # lazy evaluation
                self.logger.debug(f"Lazy-loaded project: {self._project}")
            return self._project
        except Exception as e:
            self.logger.exception(f"Failed to get project: {e}")
            raise

    def get_bucket(self, bucket_name: str):
        try:
            bucket = self.client.bucket(bucket_name)  # No network call
            self.logger.debug(f"Defined bucket: {bucket_name}")
            return bucket
        except Exception as e:
            self.logger.exception(f"Failed to get bucket '{bucket_name}': {e}")
            raise

    def define_blob(self, blob_name: str, bucket_name: str):
        try:
            bucket = self.get_bucket(bucket_name)
            blob = bucket.blob(blob_name)  # No network call
            self.logger.debug(f"Defined blob '{blob_name}' in bucket '{bucket_name}'")
            return blob
        except Exception as e:
            self.logger.exception(f"Failed to define blob '{blob_name}' in '{bucket_name}': {e}")
            raise

    def upload_blob_from_string(self, blob_name: str, bucket_name: str, data: str):
        try:
            blob = self.define_blob(blob_name, bucket_name)
            blob.upload_from_string(data)
            self.logger.info(f"Uploaded blob '{blob_name}' to bucket '{bucket_name}'")
        except Exception as e:
            self.logger.exception(f"Failed to upload blob '{blob_name}' to '{bucket_name}': {e}")
            raise

    def download_blob_to_string(self, blob_name: str, bucket_name: str) -> str:
        try:
            blob = self.define_blob(blob_name, bucket_name)
            content = blob.download_as_text()
            self.logger.info(f"Downloaded blob '{blob_name}' from bucket '{bucket_name}'")
            return content
        except Exception as e:
            self.logger.exception(f"Failed to download blob '{blob_name}' from '{bucket_name}': {e}")
            raise
