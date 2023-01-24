import toloka.client as toloka
import os
import boto3

OBGECT_STORAGE_KEY_ID = ''
OBGECT_STORAGE_REGION_ACCES_KEY = ''
BUCKET_NAME = ''

URL_API = "https://toloka.yandex.ru/api/v1/"
OAUTH_TOKEN = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}

toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')
print(toloka_client.get_requester())

session = boto3.session.Session(region_name='us-east-1',
                                aws_secret_access_key=OBGECT_STORAGE_REGION_ACCES_KEY,
                                aws_access_key_id=OBGECT_STORAGE_KEY_ID)
s3 = session.client(service_name='s3',endpoint_url='https://storage.yandexcloud.net')

s3_resource = boto3.resource('s3',region_name='us-east-1',aws_secret_access_key=OBGECT_STORAGE_REGION_ACCES_KEY,
                            aws_access_key_id=OBGECT_STORAGE_KEY_ID,endpoint_url='https://storage.yandexcloud.net')

the_bucket = s3_resource.Bucket(BUCKET_NAME)

for root, subdirectories, files in os.walk('dirs'):
    for file in files:
        file1 = open(os.path.join(root, file),'rb').read()
        key = str(os.path.join(root, file)).replace('dirs\\', '').replace('\\', '/')
        s3.put_object(Bucket = BUCKET_NAME, Key = key,
                      Body=file1,ContentType = 'image/jpg')
        print(os.path.join(root, file),' finished')