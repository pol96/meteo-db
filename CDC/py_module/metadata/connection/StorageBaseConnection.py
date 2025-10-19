from google.cloud import storage
from google.oauth2 import service_account

class StorageConn:
    def __init__(self, credential_path: str | None = None, project: str | None = None):
        """
        Initialize the Google Cloud Storage client.

        Args:
            credential_path (str | None): Path to service account JSON key.
            project (str | None): Optional project ID to avoid network call.
        """
        if credential_path:
            credentials = service_account.Credentials.from_service_account_file(credential_path)
            self.client = storage.Client(credentials=credentials, project=project)
        else:
            self.client = storage.Client(project=project)

        self._project = project

    @property
    def project(self):
        if self._project is None:
            self._project = self.client.project  # lazy evaluation
        return self._project

    def get_bucket(self, bucket_name: str):
        """
        Get a reference to a bucket without making a network call.
        """
        bucket = self.client.bucket(bucket_name)  # No HTTP request yet
        return bucket

    def define_blob(self, blob_name: str, bucket_name: str):
        """
        Define a blob in the bucket without fetching metadata.
        """
        bucket = self.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)  # No network call until used
        return blob

    def upload_blob_from_string(self, blob_name: str, bucket_name: str, data: str):
        """
        Upload data to GCS blob.
        """
        blob = self.define_blob(blob_name, bucket_name)
        blob.upload_from_string(data)

    def download_blob_to_string(self, blob_name: str, bucket_name: str) -> str:
        """
        Download data from GCS blob.
        """
        blob = self.define_blob(blob_name, bucket_name)
        return blob.download_as_text()
