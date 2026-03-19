"""
loader/s3_loader.py
A loader for pushing processed data as CSV to an S3 bucket.
"""

import io

import boto3
import pandas as pd
from botocore.exceptions import ClientError, NoCredentialsError

from loader.base_loader import BaseLoader


class S3Loader(BaseLoader):
    """
    Load a DataFrame directly into AWS S3 as CSV without intermediate files.
    """

    def __init__(self, bucket_name: str, object_key: str):
        super().__init__()
        self.bucket_name = bucket_name
        self.object_key = object_key
        self.s3_client = boto3.client("s3")

    def load(self, df: pd.DataFrame) -> None:
        self.logger.info("Uploading to s3://%s/%s", self.bucket_name, self.object_key)

        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)

        try:
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=self.object_key,
                Body=csv_buffer.getvalue(),
            )
            self.logger.info("Successfully uploaded data to S3.")
        except NoCredentialsError:
            self.logger.error("AWS credentials not found. S3 upload failed.")
            raise
        except ClientError as exc:
            self.logger.error("S3 ClientError during upload: %s", exc)
            raise
