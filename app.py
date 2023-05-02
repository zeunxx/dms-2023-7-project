from flask import Flask
from flask import *
from kafka import KafkaProducer
from json import dumps
import time

app = Flask(__name__)

def producerSend(writer, timestamp, content):
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))

    test_string={"writer":writer,"timestamp":str(timestamp),"content":content}
    producer.send('test-topic',value=test_string)
    producer.flush()

@app.route("/")
def index():
    return "home"

@app.route("/msg-send",methods=['POST'])
def producer_test():
    params = request.get_json()
    writer = params['writer']
    timestamp = params['timestamp']
    content = params['content']
    producerSend(writer, timestamp, content)
    return "ok"
    
    

app.run(port=5000, debug=True)