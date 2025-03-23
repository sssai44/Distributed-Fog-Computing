import json
import time
import threading
import docker
from kafka import KafkaConsumer, KafkaProducer
from concurrent.futures import ThreadPoolExecutor

KAFKA_BROKER = "localhost:9092"
INPUT_TOPIC = "task-submission"
OUTPUT_TOPIC = "task-results"

class TaskManager:
    def __init__(self, max_concurrent_tasks: int = 10):
        self.max_concurrent_tasks = max_concurrent_tasks
        self.tasks = {}  # Store task info
        self.running_tasks = 0
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_tasks)
        self.docker_client = docker.from_env()

        # Kafka setup
        self.consumer = KafkaConsumer(
            INPUT_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )

    def start(self):
        """ Start listening for tasks from Kafka. """
        print("[TaskManager] Listening for tasks...")
        for message in self.consumer:
            task_data = message.value
            print(f"[TaskManager] Received Task: {task_data}")
            self.executor.submit(self._process_task, task_data)

    def _process_task(self, task):
        """ Process and execute the received task. """
        task_id = task["task_id"]
        self.tasks[task_id] = task
        task["status"] = "Running"
        self.running_tasks += 1

        # Simulate Execution
        time.sleep(5)  # Placeholder for actual execution

        task["status"] = "Completed"
        task["result"] = f"Processed {task['task_type']} task successfully"

        # Send results to Kafka
        self.producer.send(OUTPUT_TOPIC, task)
        print(f"[TaskManager] Task {task_id} completed and sent to Kafka.")

        self.running_tasks -= 1

if __name__ == "__main__":
    task_manager = TaskManager()
    task_manager.start()

