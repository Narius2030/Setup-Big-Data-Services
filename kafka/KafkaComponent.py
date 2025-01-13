import os
from json import dumps
from kafka import KafkaProducer
from confluent_kafka import Consumer
# from confluent_kafka.serialization import StringSerializer


class Prod():
    def __init__(self, kafka_addr:str, topic:str, generator=None):
        self.kafka_addr = kafka_addr
        self.topic = topic
        # self.key = key
        self.generator = generator
    
    def run(self):
        producer = KafkaProducer(bootstrap_servers=[f'{self.kafka_addr}:9092'],
                                 key_serializer=lambda x: dumps(x).encode('utf-8'),
                                 value_serializer=lambda x: dumps(x, default=str).encode('utf-8'))
        # send data to topic
        for data in self.generator():
            producer.send(self.topic, key=data['key'], value=data['value'])
            print("processing")
        producer.close()
    

class Cons():
    def __init__(self, kafka_addr:str, topic:str, group_id:str=None, function=None):
        self.kafka_addr = kafka_addr
        self.topic = topic
        self.group_id = group_id
        self.function = function
        
    def run(self):
        settings = {
            'bootstrap.servers': f'{self.kafka_addr}:9092',     # Địa chỉ broker
            'group.id': self.group_id,                          # ID của consumer group
            'auto.offset.reset': 'earliest',                    # Đọc từ offset đầu tiên nếu không có offset
            'enable.auto.commit': True                          # tự động commit offset
        }
        consumer = Consumer(settings)
        consumer.subscribe([self.topic])
        
        try:
            empty_poll_count = 0  # Số lần liên tiếp không nhận được message
            max_empty_polls = 5   # Giới hạn số lần không nhận được message trước khi dừng
            
            while True:
                message = consumer.poll(1.0)
                if not message:
                    # Không nhận được message, tăng bộ đếm
                    empty_poll_count += 1
                    if empty_poll_count >= max_empty_polls:
                        print("No more messages, exiting loop.")
                        break
                    continue
                self.function(message)
                # Nhận được message, xử lý và reset bộ đếm
                empty_poll_count = 0
                
        except Exception as ex:
            print(f"An error occurred: {ex}")
        finally:
            consumer.close()
            print("Consumer closed.")

    