import logging
import boto3
from botocore.exceptions import ClientError
from boto3.s3.transfer import TransferConfig

class AmazonS3():
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, region_name=None, config=TransferConfig()):
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.region_name = region_name
        self.config = config
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
    def upload_file(self, file_name, bucket, object_name=None, extra_args=None, callback=None, config=None):
        """Upload a file to an S3 bucket

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # If S3 config was not specified, use self transfer config
        if config is None:
            config = self.config

        try:
            response = self.s3_client.upload_file(file_name, bucket, object_name, ExtraArgs=extra_args, Callback=callback, Config=config)
        except ClientError as e:
            logging.error(e)
            return False
        return True

    def upload_fileobj(self, fileobj, bucket, object_name, extra_args=None, callback=None, config=None):
        """Upload a file to an S3 bucket

        :param fileobj: File stream to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name.
        :return: True if file was uploaded, else False
        """

        # If S3 config was not specified, use self transfer config
        if config is None:
            config = self.config

        try:
            response = self.s3_client.upload_fileobj(fileobj, bucket, object_name, ExtraArgs=extra_args, Callback=callback, Config=config)
        except ClientError as e:
            logging.error(e)
            return False
        return True
    
    def extra_metadata(self, metadata, args={}):
        args['Metadata'] = metadata
        return args

    def extra_public_read(self, args={}):
        args['ACL'] = 'public-read'
        return args
    
    def extra_grant(self, grant_read, grant_full_control, args={}):
        args['GrantRead'] = grant_read
        args['GrantFullControl'] = grant_full_control
        return args

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-example-download-file.html
    def download_file(self, bucket, object_name, file_name=None, config=None):
        # If S3 object_name was not specified, use file_name
        if file_name is None:
            file_name = object_name
        
        # If S3 config was not specified, use self transfer config
        if config is None:
            config = self.config

        return self.s3_client.download_file(bucket, object_name, file_name, Config=config)

    def download_fileobj(self, bucket, object_name, fileobj, config=None):
        # If S3 config was not specified, use self transfer config
        if config is None:
            config = self.config

        return self.s3_client.download_file(bucket, object_name, fileobj, Config=config)

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3.html
    def transfer_config(self, multipart_threshold=None, max_concurrency=None, use_threads=None, forever=False):
        config = TransferConfig(multipart_threshold=multipart_threshold, max_concurrency=max_concurrency, use_threads=use_threads)
        if forever is True:
            self.config = config
        return config

    # https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
    def create_presigned_url(self, bucket_name, object_name, expiration=3600):
        """Generate a presigned URL to share an S3 object

        :param bucket_name: string
        :param object_name: string
        :param expiration: Time in seconds for the presigned URL to remain valid
        :return: Presigned URL as string. If error, returns None.
        """

        # Generate a presigned URL for the S3 object
        try:
            response = self.s3_client.generate_presigned_url('get_object',
                                                        Params={'Bucket': bucket_name,
                                                                'Key': object_name},
                                                        ExpiresIn=expiration)
        except ClientError as e:
            logging.error(e)
            return None

        # The response contains the presigned URL
        return response

    def create_presigned_url_expanded(self, client_method_name, method_parameters=None, expiration=3600, http_method=None):
        """Generate a presigned URL to invoke an S3.Client method

        Not all the client methods provided in the AWS Python SDK are supported.

        :param client_method_name: Name of the S3.Client method, e.g., 'list_buckets'
        :param method_parameters: Dictionary of parameters to send to the method
        :param expiration: Time in seconds for the presigned URL to remain valid
        :param http_method: HTTP method to use (GET, etc.)
        :return: Presigned URL as string. If error, returns None.
        """

        # Generate a presigned URL for the S3 client method
        try:
            response = self.s3_client.generate_presigned_url(ClientMethod=client_method_name,
                                                        Params=method_parameters,
                                                        ExpiresIn=expiration,
                                                        HttpMethod=http_method)
        except ClientError as e:
            logging.error(e)
            return None

        # The response contains the presigned URL
        return response

    def create_presigned_post(self, bucket_name, object_name, fields=None, conditions=None, expiration=3600):
        """Generate a presigned URL S3 POST request to upload a file

        :param bucket_name: string
        :param object_name: string
        :param fields: Dictionary of prefilled form fields
        :param conditions: List of conditions to include in the policy
        :param expiration: Time in seconds for the presigned URL to remain valid
        :return: Dictionary with the following keys:
            url: URL to post to
            fields: Dictionary of form fields and values to submit with the POST
        :return: None if error.
        """

        # Generate a presigned S3 POST URL
        try:
            response = self.s3_client.generate_presigned_post(bucket_name,
                                                        object_name,
                                                        Fields=fields,
                                                        Conditions=conditions,
                                                        ExpiresIn=expiration)
        except ClientError as e:
            logging.error(e)
            return None

        # The response contains the presigned URL and required fields
        return response