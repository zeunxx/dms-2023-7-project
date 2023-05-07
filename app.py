import json

from flask import Flask
from flask import *
from kafka import KafkaProducer, KafkaConsumer
from json import dumps
import sys
import time
from flask_cors import CORS
import datetime

app = Flask(__name__)
CORS(app)

# 09~22시 사이에는 배치 레이어 작업
# => 시간 체크하는 함수
# check_time(): time is 09~22 => return True
def check_time():
    time_now = datetime.datetime.now().hour
    print(time_now)
    if time_now < 9 or time_now > 21:
        print("False")
        return False
    else:
        print("True")
        return True

# producer가 topic에 msg 전송
def producerSend(writer, timestamp, content):
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))

    test_string={"writer":writer,"timestamp":str(timestamp),"content":content}
    producer.send('test-topic',value=test_string)
    producer.flush()

# consumer가 topic에서 msg 수신
def consumerGet():
    consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092')
    string = " "
    for message in consumer:
        value = message.value
        d = json.loads(value.decode('utf-8'))
        # print(f'{d.get("writer", "Nothing")} / {type(d.get("writer", "Nothing"))}')
        string = d.get("content", "Nothing")
        return string

# index route - 사용할 일 없음
@app.route("/")
def index():
    print("홈 들어옴")
    return "home"
    
# ui에서 msg 받아와 producerSend 메소드 실행
@app.route("/msg_send",methods=['POST'])
def producer_test():
    params = request.get_json()
    writer = params['writer']
    timestamp = params['timestamp']
    content = params['content']
    producerSend(writer, timestamp, content)
    return "ok"

@app.route("/msg_get",methods=['POST'])
def consumer_test():
    msg = consumerGet()
    d = {"message": msg}
    json_data = json.dumps(d)
    return json_data

app.run(port=8989, debug=True)