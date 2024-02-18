from kafka import KafkaProducer
import uuid, time

server = ['192.168.53.64:9092',]
token  = 'ecea8c6015c73989'
producer = KafkaProducer(bootstrap_servers=server, compression_type='gzip', client_id=token)
producer.send('auth', value=bytes(token, encoding='UTF-8'))

producer.close()
time.sleep(1)

import sys
print(sys.getrecursionlimit())
sys.setrecursionlimit(1500)
print(sys.getrecursionlimit())

data_path_list = ['images.png', 'avatar.png', 'error.png']

headers = [('token', bytes(str(token), encoding='UTF-8'))]
def produce_message(data_path, key):
    if producer.bootstrap_connected():
        print('ok')
        with open(data_path, mode='r+b') as f:
            for line in f:
                producer.send(topic='data_gathering', value=line, key=key, headers=headers)
                time.sleep(0.001)
        producer.close()
    else:
        produce_message(data_path, key)
    return 'Data has been sent successfully'''


for i, data_path in enumerate(data_path_list):
    id = str(uuid.uuid4().hex)
    key = bytes(id+data_path, encoding='UTF-8')
    producer = KafkaProducer(bootstrap_servers=server, compression_type='gzip', client_id=token)
    print(produce_message(data_path, key=key))
    producer.close()


producer = KafkaProducer(bootstrap_servers=server, compression_type='gzip', client_id=token)
key = bytes('ENDofMESSAGE', encoding='UTF-8')
producer.send(topic='data_gathering', value=b'ENDofMESSAGE', key=b'ENDofMESSAGE', headers=headers)
producer.close()