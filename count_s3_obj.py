import boto3

s3_client = boto3.client("s3")
dynamodb_client = boto3.resource('dynamodb', endpoint_url='https://dynamodb.your-region.amazonaws.com')

hh_bucket = 'your-bucket'
hh_dir = 'your-folder-to-file'
hh_limit = 100000

paginator = s3_client.get_paginator('list_objects_v2')
response = paginator.paginate(Bucket=hh_bucket, Prefix=hh_dir)
f = open("your-text-filename.txt", "a")
for i in response:
    for obj in i['Contents']:
        if obj['Size'] == 0:
            continue
        f.write(str(obj['Key']) + '\n' )
f.close()