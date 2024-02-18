
from kafka import KafkaConsumer

bootstrap_servers = ['192.168.53.64:9092']

consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers, group_id='mindstream_group', auto_offset_reset='latest')
consumer.subscribe(topics=['auth', 'data_gathering'])

session_list = ['ecea8c6015c73989',]
authorized_list = list()

nameList = list()
for message in consumer:
    if message.topic == 'auth':
        # this pard must be replaced with auth api 
        token = message.value.decode('UTF-8')
        if token in session_list:
            print('authenticated')
            authorized_list.append(message.value.decode('UTF-8'))
    if message.topic == 'data_gathering':
        token = message.headers[-1][-1].decode('UTF-8')
        if token in authorized_list:
            name = './storage/'+ '_' + message.key.decode('UTF-8')
            if 'ENDofMESSAGE' not in name:
                with open(name, 'a+b') as f:
                    f.write(message.value)
                nameList.append(name)
                auxiliaryList = list(set(nameList))
                nameList.pop()
                nameList = auxiliaryList.copy()
            else:
                print(nameList)
                nameList = list()
        else:
            print('Not authorized')