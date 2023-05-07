import json
from kafka import KafkaProducer, KafkaConsumer
#
# def consumerGet():
#     consumer = KafkaConsumer('test-topic', bootstrap_servers='localhost:9092')
#
#     string = " "
#     for message in consumer:
#         print(message)
#         print("ok")
#
#         value = message.value
#         d = json.loads(value.decode('utf-8'))
#         #print(f'{value.decode("utf-8")} / {type(value.decode("utf-8"))}')
#         print(f'{d.get("writer", "Nothing")} / {type(d.get("writer", "Nothing"))}')
#         string = d.get("writer", "Nothing")
#         return string
# ans = consumerGet()
# print(ans)
# print("done")
msg = "hello"
d = {"message":msg}
json_data = json.dumps(d)
print(f'{d} {type(d)}\n')
print(f'{json_data} {type(json_data)}')