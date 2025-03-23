from confluent_kafka import Consumer

conf = {'bootstrap.servers': 'localhost:9092', 'group.id': 'mygroup', 'auto.offset.reset': 'earliest'}
consumer = Consumer(conf)

consumer.subscribe(['test-topic'])

while True:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print(f"Consumer error: {msg.error()}")
        continue

    print(f"Received message: {msg.value().decode('utf-8')}")

