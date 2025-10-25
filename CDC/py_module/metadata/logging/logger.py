import logging
import os
from typing import Optional

try:
    from google.cloud import logging as cloud_logging
    from google.cloud.logging.handlers import StructuredLogHandler
    from google.cloud import storage
except ImportError:
    cloud_logging = None
    StructuredLogHandler = None
    storage = None

class BaseLogger:
    """
    Base logger class that can push logs dynamically to:
    - Console
    - Local file
    - GCS (via temporary local file)
    - Cloud Logging (StructuredLogHandler)
    """

    # Keep one logger per class
    _loggers = {}

    def __init__(
        self,
        logger_name: Optional[str] = None,
        log_level=logging.INFO,
        log_to_console: bool = True,
        log_file: Optional[str] = None,  # local path or GCS path
        cloud_logging_client=None  # Google Cloud Logging client
    ):
        self.logger_name = logger_name or self.__class__.__name__

        # Initialize logger instance
        self.logger = logging.getLogger(self.logger_name)
        self.logger.setLevel(log_level)

        if not self.logger.handlers:
            formatter = logging.Formatter(
                fmt='%(asctime)s - [%(levelname)-8s] | %(message)s',
                datefmt='%Y-%m-%d %H:%M:%S'
            )

            # Console
            if log_to_console:
                console_handler = logging.StreamHandler()
                console_handler.setFormatter(formatter)
                self.logger.addHandler(console_handler)

            # File handler (local or GCS)
            self._gcs_path = None
            if log_file:
                if log_file.startswith("gs://") and storage:
                    # Temporary local file
                    self._local_temp_file = "/tmp/temp_log.log"
                    file_handler = logging.FileHandler(self._local_temp_file)
                    file_handler.setFormatter(formatter)
                    self.logger.addHandler(file_handler)

                    # Prepare GCS info
                    self._gcs_path = log_file
                    self._storage_client = storage.Client()
                    self._bucket_name, self._blob_name = self._parse_gcs_path(log_file)
                else:
                    # Local file
                    file_handler = logging.FileHandler(log_file)
                    file_handler.setFormatter(formatter)
                    self.logger.addHandler(file_handler)

            # Cloud Logging
            if cloud_logging_client and StructuredLogHandler:
                cloud_handler = StructuredLogHandler(cloud_logging_client.logger(self.logger_name))
                cloud_handler.setLevel(log_level)
                self.logger.addHandler(cloud_handler)

    # --------------------- Classmethod Logger ---------------------
    @classmethod
    def get_logger(cls):
        """
        Return a logger for the class.
        Can be called inside classmethods without creating an instance.
        """
        if cls not in cls._loggers:
            logger = logging.getLogger(cls.__name__)
            logger.setLevel(logging.INFO)

            if not logger.handlers:
                formatter = logging.Formatter(
                    fmt='%(asctime)s - [%(levelname)-7s] | %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
                )
                ch = logging.StreamHandler()
                ch.setFormatter(formatter)
                logger.addHandler(ch)

            cls._loggers[cls] = logger
        return cls._loggers[cls]

    # --------------------- GCS Push ---------------------
    def _parse_gcs_path(self, gcs_path):
        path = gcs_path.replace("gs://", "")
        bucket, *blob_parts = path.split("/")
        blob_name = "/".join(blob_parts)
        return bucket, blob_name

    def push_to_gcs(self):
        """Upload the temporary file to GCS and delete it afterwards"""
        if not getattr(self, "_gcs_path", None):
            return

        try:
            bucket = self._storage_client.bucket(self._bucket_name)
            blob = bucket.blob(self._blob_name)
            blob.upload_from_filename(self._local_temp_file)
            self.logger.info(f"Pushed logs to {self._gcs_path}")
        finally:
            # Delete temp file
            if os.path.exists(self._local_temp_file):
                try:
                    os.remove(self._local_temp_file)
                    self.logger.debug(f"Deleted temporary file {self._local_temp_file}")
                except Exception as e:
                    self.logger.warning(f"Failed to delete temp file: {e}")
