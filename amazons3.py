import logging
import boto3
from botocore.exceptions import ClientError

class AmazonS3():
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
    
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-creating-buckets.html
    def create_bucket(self, bucket_name):
        """Create an S3 bucket in a specified region

        If a region is not specified, the bucket is created in the S3 default
        region (us-east-1).

        :param bucket_name: Bucket to create
        :param region: String region to create bucket in, e.g., 'us-west-2'
        :return: True if bucket created, else False
        """

        # Create bucket
        try:
            if self.region_name is None:
                self.s3_client.create_bucket(Bucket=bucket_name)
            else:
                location = {'LocationConstraint': self.region_name}
                self.s3_client.create_bucket(Bucket=bucket_name,
                                        CreateBucketConfiguration=location)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def list_buckets(self):
        # Retrieve the list of existing buckets
        response = self.s3_client.list_buckets()

        # Output the bucket names
        print('Existing buckets:')
        for bucket in response['Buckets']:
            print(f'  {bucket["Name"]}')
        return response['Buckets']
        
    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
    def upload_file(self, file_name, bucket, object_name=None, extra_args=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        try:
            response = self.s3_client.upload_file(file_name, bucket, object_name, ExtraArgs=extra_args)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_fileobj(self, fileobj, bucket, object_name, extra_args=None):
        """Upload a file to an S3 bucket

        :param fileobj: File stream to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name.
        :return: True if file was uploaded, else False
        """

        try:
            response = self.s3_client.upload_fileobj(fileobj, bucket, object_name, ExtraArgs=extra_args)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def extra_public_read(self, args=None):
        if args is None:
            args = {'ACL': 'public-read'}
        else:
            args['ACL'] = 'public-read'
        return args