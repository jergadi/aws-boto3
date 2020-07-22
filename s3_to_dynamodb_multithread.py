import boto3
import json
import gzip
from decimal import *
import gc
import threading
import pandas as pd

s3_client = boto3.client("s3")
dynamodb_client = boto3.resource('dynamodb', endpoint_url='https://dynamodb.us-west-2.amazonaws.com')

hh_bucket = 'your-bucket'
hh_dir = 'your-folder'
hh_limit = 1000
hh_header = ['linkkey']
source_excel_sheet = 'your-directory/github-multithread_sheet.xlsx'


class myThread (threading.Thread):
    def __init__(self, threadID, name, counter):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.name = name
        self.counter = counter
    def run(self):
        print("Starting " + self.name)
        load_thread(self.name, self.counter)
        print("Exiting " + self.name)


def load_item(items, dynamodb=None):
    if not dynamodb:
        dynamodb = boto3.resource('dynamodb', endpoint_url='https://dynamodb.your-region.amazonaws.com')
    table = dynamodb.Table('your-dynamodb-table')
    with table.batch_writer() as batch:
        batch.put_item(Item=items)

def load_thread(threadName, sht_num):
    df = pd.read_excel(source_excel_sheet, sheet_name=sht_num, header=None, names=hh_header)
    for index, row in df.iterrows():
        response = s3_client.list_objects_v2(Bucket=hh_bucket, Prefix=row[hh_header], MaxKeys=hh_limit)
        for i in response['Contents']:
            if i['Size'] == 0:
                continue
            json_obj = s3_client.get_object(Bucket=hh_bucket, Key=i['Key'])['Body'].read()
            json_data = gzip.decompress(json_obj)
            json_dict = json.loads(json_data.decode('utf-8'), parse_float=Decimal)
            for r in json_dict:
                load_item(r)
            gc.collect()

thread1 = myThread(1, "Thread-1", '1')
thread2 = myThread(2, "Thread-2", '2')
thread3 = myThread(3, "Thread-3", '3')
thread4 = myThread(4, "Thread-4", '4')
thread5 = myThread(5, "Thread-5", '5')
thread6 = myThread(6, "Thread-6", '6')
thread7 = myThread(7, "Thread-7", '7')
thread8 = myThread(8, "Thread-8", '8')
thread9 = myThread(9, "Thread-9", '9')
thread10 = myThread(10, "Thread-10", '10')
thread11 = myThread(11, "Thread-11", '11')
thread12 = myThread(12, "Thread-12", '12')
thread13 = myThread(13, "Thread-13", '13')
thread14 = myThread(14, "Thread-14", '14')
thread15 = myThread(15, "Thread-15", '15')
thread16 = myThread(16, "Thread-16", '16')
thread17 = myThread(17, "Thread-17", '17')
thread18 = myThread(18, "Thread-18", '18')
thread19 = myThread(19, "Thread-19", '19')
thread20 = myThread(20, "Thread-20", '20')


thread1.start()
thread2.start()
thread3.start()
thread4.start()
thread5.start()
thread6.start()
thread7.start()
thread8.start()
thread9.start()
thread10.start()
thread11.start()
thread12.start()
thread13.start()
thread14.start()
thread15.start()
thread16.start()
thread17.start()
thread18.start()
thread19.start()
thread20.start()