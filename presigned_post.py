import requests    # To install: pip install requests
import logging
import amazons3

# https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
s3 = amazons3.AmazonS3()
# Generate a presigned S3 POST URL
object_name = 'OBJECT_NAME'
response = s3.create_presigned_post('BUCKET_NAME', object_name)
if response is None:
    exit(1)

# Demonstrate how another Python program can use the presigned URL to upload a file
with open(object_name, 'rb') as f:
    files = {'file': (object_name, f)}
    http_response = requests.post(response['url'], data=response['fields'], files=files)
# If successful, returns HTTP status code 204
logging.info(f'File upload HTTP status code: {http_response.status_code}')