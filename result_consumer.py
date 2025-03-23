from kafka import KafkaConsumer
import json

KAFKA_BROKER = "localhost:9092"
TOPIC = "task-results"

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("[Result Consumer] Waiting for completed tasks...")

for message in consumer:
    task_data = message.value
    print(f"[Result Consumer] Task Completed: {task_data}")

