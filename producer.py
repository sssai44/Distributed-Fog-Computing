from kafka import KafkaProducer
import json
import uuid

KAFKA_BROKER = "localhost:9092"
TOPIC = "task-submission"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Sample Task Data
task = {
    "task_id": str(uuid.uuid4()),
    "task_type": "image_processing",
    "input_data": {"image_url": "http://example.com/image.jpg"},
    "estimated_cpu": 1.0,
    "estimated_ram": 2.0,
    "estimated_gpu": 0.0,
    "is_divisible": False,
    "max_execution_time": 60,
}

# Send Task to Kafka
producer.send(TOPIC, task)
producer.flush()
print("[Producer] Task sent successfully!")

