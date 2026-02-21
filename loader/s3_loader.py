"""
loader/s3_loader.py
A loader for pushing processed data as CSV or Parquet to an S3 bucket.
"""

import io
import pandas as pd
import boto3
from botocore.exceptions import NoCredentialsError, ClientError

from config import get_logger
from loader.base_loader import BaseLoader

logger = get_logger("s3_loader")


class S3Loader(BaseLoader):
    """
    Loads DataFrame directly into AWS S3 as CSV without intermediate files.
    """

    def __init__(self, bucket_name: str, object_key: str):
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.s3_client = boto3.client("s3")

    def run(self, df: pd.DataFrame) -> None:
        logger.info("Starting upload to s3://%s/%s", self.bucket_name, self.object_key)
        
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.object_key,
                Body=csv_buffer.getvalue()
            )
            logger.info("Successfully uploaded data to S3.")
        except NoCredentialsError:
            logger.error("AWS credentials not found. S3 upload failed.")
            raise
        except ClientError as e:
            logger.error("S3 ClientError during upload: %s", e)
            raise
