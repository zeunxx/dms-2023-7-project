import json
from kafka import KafkaProducer, KafkaConsumer

def consumerGet():
    consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092')


    for message in consumer:
        # print(message)
        # ConsumerRecord(topic='test-topic', partition=0, offset=73, timestamp=1683459269644, timestamp_type=0, key=None, value=b'{"writer": "testttttttest", "timestamp": "05/07 20:11", "content": "test content 0507"}', headers=[], checksum=None, serialized_key_size=-1, serialized_value_size=87, serialized_header_size=-1)
        value = message.value
        d = json.loads(value.decode('utf-8'))
        # print(d) #{'writer': 'testttttttest', 'timestamp': '05/07 20:11', 'content': 'test content 0507'}
        #print(f'{value.decode("utf-8")} / {type(value.decode("utf-8"))}')
        # print(f'{d.get("content", "Nothing")} / {type(d.get("content", "Nothing"))}') # test content 0507 / <class 'str'>
        # print(f'{d.get("timestamp", "Nothing")} / {type(d.get("timestamp", "Nothing"))}') # 05/07 20:11 / <class 'str'>
        string = {"content": d.get("content", "Nothing"), "timestamp": d.get("timestamp", "Nothing")}
        return string
ans = consumerGet()
json_ans = json.dumps(ans)
print(f'{ans} / {type(ans)}\n') # {'content': 'test content 0507', 'timestamp': '05/07 20:11'} / <class 'dict'>
print(f'{json_ans} / {type(json_ans)}\n') #{"content": "test content 0507", "timestamp": "05/07 20:11"} / <class 'str'>
print("done")


# msg = "hello"
# d = {"message":msg}
# json_data = json.dumps(d)
# print(f'{d} {type(d)}\n')
# print(f'{json_data} {type(json_data)}')