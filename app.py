from flask import Flask
from flask import *
from kafka import KafkaProducer
from json import dumps
import sys
import time
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

def producerSend(writer, timestamp, content):
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))

    test_string={"writer":writer,"timestamp":str(timestamp),"content":content}
    producer.send('test-topic',value=test_string)
    producer.flush()

@app.route("/")
def index():
    print("홈 들어옴")
    return "home"
    
@app.route("/msg_send",methods=['POST'])
def producer_test():
    params = request.get_json()
    writer = params['writer']
    timestamp = params['timestamp']
    content = params['content']
    producerSend(writer, timestamp, content)
    return "ok"

app.run(port=8989, debug=True)