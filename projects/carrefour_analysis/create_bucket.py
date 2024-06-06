import boto3
import logging
from botocore.exceptions import ClientError

def create_bucket(bucket_name, region):
    try:
        s3_client = boto3.client('s3')
        # default region and does not require the creacte bucket location constraint
        if region == 'us-east-1':
            s3_client.create_bucket(Bucket=bucket_name)
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name, CreateBucketConfiguration=location)
        print(f'{bucket_name} created successfully')
    except ClientError as e:
        logging.error(e)
        print(f'Failed to create bucket {bucket_name}')

if __name__ == "__main__":
    bucket_name = 'c4-analysed'
    create_bucket(bucket_name, 'us-east-1')
