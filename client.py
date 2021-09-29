import os

import boto3

AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
AWS_BUCKET_NAME = "ams3.digitaloceanspaces.com"
AWS_REGION_NAME = "ams3"
AWS_BUCKET_PATH = "backups"


def get_space_client():
    session = boto3.session.Session()
    s3_client = session.client('s3',
                               region_name=AWS_REGION_NAME,
                               endpoint_url=f'https://{AWS_BUCKET_NAME}',
                               aws_access_key_id=AWS_ACCESS_KEY_ID,
                               aws_secret_access_key=AWS_SECRET_ACCESS_KEY
                               )
    return s3_client
