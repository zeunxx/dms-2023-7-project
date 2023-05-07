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
    for message in consumer:
        # message : ConsumerRecord(topic='test-topic', partition=0, offset=73, timestamp=1683459269644, timestamp_type=0, key=None, value=b'{"writer": "testttttttest", "timestamp": "05/07 20:11", "content": "test content 0507"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=87, serialized_header_size=-1)
        value = message.value
        d = json.loads(value.decode('utf-8'))
        string = {"content": d.get("content", "Nothing"), "timestamp": d.get("timestamp", "Nothing")}
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

@app.route("/msg_get",methods=['GET', 'POST'])
def consumer_test():
    ans = consumerGet()
    # ans : {'content': 'test content 0507', 'timestamp': '05/07 20:11'} / <class 'dict'>
    json_ans = json.dumps(ans)
    # json_ans : {"content": "test content 0507", "timestamp": "05/07 20:11"} / <class 'str'>
    # 모두 쌍따옴표로 감싸져 있어야 다시 javascript에서 JSON으로 변환 가능
    return json_ans

app.run(port=8989, debug=True)