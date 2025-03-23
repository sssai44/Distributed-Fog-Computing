from confluent_kafka import Producer

conf = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(conf)

producer.produce('test-topic', key="key1", value="Hello from Python!")
producer.flush()

