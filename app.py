from flask import Flask
from flask import *
from kafka import KafkaProducer
from json import dumps
import sys
import time
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

# producer가 topic에 msg 전송
def producerSend(writer, timestamp, content):
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))

    test_string={"writer":writer,"timestamp":str(timestamp),"content":content}
    producer.send('test-topic',value=test_string)
    producer.flush()

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

app.run(port=8989, debug=True)