from google.cloud import storage
from google.oauth2 import service_account
import json
class StorageConn(storage):
    def __init__(self, 
                credential_path: str):
        
        with open(credential_path, 'r') as f:
            SA = json.load(f)

        credentials = service_account.Credentials.from_service_account_file(SA)
        storage = super().__init__()

        self.client = storage.Client(credentials = credentials)
        self.project = self.client.project_id


    def connect(self):
        pass

