from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')),
    consumer_timeout_ms=1000
)

print('[begin] get consumer list')
for message in consumer:
    print("Timestamp: %s, Writer: %s, Content: %s" 
          % (message.value['timestamp'], message.value['writer'], message.value['content']))
print('[end] get consumer list')