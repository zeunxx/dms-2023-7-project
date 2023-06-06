import subprocess
from kafka import KafkaConsumer
from kafka import KafkaProducer
from json import loads
from json import dumps
import boto3
import cv2
import os

AWS_ACCESS_KEY_ID ="AKIAQME3RJYR4PXK5XWQ"
AWS_SECRET_ACCESS_KEY = "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"
AWS_DEFAULT_REGION = "ap-northeast-2"
print("start")

def send(path):
    # s3 셋팅
    s3 = boto3.client('s3',
                        aws_access_key_id=AWS_ACCESS_KEY_ID,
                        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                        region_name=AWS_DEFAULT_REGION
                        )

    # kafka producer 셋팅
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))

    # TODO : 나중에 html에서 path받으면 넣기
    img_string={"path":path}

    producer.send('img-topic',value=img_string)
    producer.flush()


    
# consumer 테스트가 안돼서 잘 되는지 모르겠음 ..
def get():
    # kafka consumer 셋팅
    consumer = KafkaConsumer('img-topic', bootstrap_servers='localhost:9092', auto_offset_reset='latest')

    #  가져온 message의 key가 path면 value를 가져오고 break
    for message in consumer:
        if isinstance(message.value, dict) and "path" in message.value:
            img_path = message.value["path"]   
            break
        
    # s3에 파일 업로드 (consumer에서 받은 값으로 file_name 변경)
    bucket_name = '2023-dms-kafka-image'
    file_name = img_path
    key = 'test/test.jpg'
    # s3.upload_file(file_name,bucket_name, key)
             
        
if __name__ == '__main__':
    print("이미지 주소 입력!!!")
    path = input()
    send(path)
    get()