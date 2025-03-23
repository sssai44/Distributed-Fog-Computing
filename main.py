import uuid
import json
import time
import threading
import queue
import docker
import requests
from typing import Dict, List, Optional, Tuple, Any, TYPE_CHECKING
from concurrent.futures import ThreadPoolExecutor

class Task:
    """
    Represents a task to be executed.
    """
    
    def __init__(self, task_type: str, input_data: Dict, estimated_cpu: float, estimated_ram: float,
                 estimated_gpu: float, is_divisible: bool, max_execution_time: int, assigned_nodes: List[str] = []):
        """
        Initialize a new task.
        
        Args:
            task_type: Type of the task (e.g., "image_processing", "text_analysis")
            input_data: Dictionary containing input data for the task
            estimated_cpu: Estimated CPU usage (in cores)
            estimated_ram: Estimated RAM usage (in GB)
            estimated_gpu: Estimated GPU usage (0 for no GPU, 1 for full GPU)
            is_divisible: Whether the task can be split and executed on multiple nodes
            max_execution_time: Maximum execution time in seconds
            assigned_nodes: List of node IDs assigned to the task (if already assigned)
        """
        self.task_id = str(uuid.uuid4())
        self.task_type = task_type
        self.input_data = input_data
        self.estimated_cpu = estimated_cpu
        self.estimated_ram = estimated_ram
        self.estimated_gpu = estimated_gpu
        self.is_divisible = is_divisible
        self.max_execution_time = max_execution_time
        self.status = "Pending"  # "Pending", "Scheduling", "Running", "Completed", "Failed"
        self.created_at = time.time()
        self.assigned_nodes = assigned_nodes
        self.results = None
    
    def to_dict(self) -> Dict:
        """Convert task to dictionary."""
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "input_data": self.input_data,
            "estimated_cpu": self.estimated_cpu,
            "estimated_ram": self.estimated_ram,
            "estimated_gpu": self.estimated_gpu,
            "is_divisible": self.is_divisible,
            "max_execution_time": self.max_execution_time,
            "status": self.status,
            "created_at": self.created_at,
            "assigned_nodes": self.assigned_nodes
        }

class DockerManager:
    def __init__(self):
        self.client = docker.from_env()
    
    def pull_image(self, image_name):
        """Pulls the required Docker image."""
        try:
            print(f"Pulling image {image_name}...")
            self.client.images.pull(image_name)
            print(f"Image {image_name} pulled successfully.")
        except Exception as e:
            print(f"Error pulling image {image_name}: {e}")
    
    def run_container(self, image_name, command, detach=True):
        """Runs a Docker container with the given image and command."""
        try:
            container = self.client.containers.run(
                image=image_name,
                command=command,
                detach=detach,
                remove=True  # Automatically remove container after execution
            )
            return container
        except Exception as e:
            print(f"Error running container: {e}")
            return None
    
    def get_container_logs(self, container):
        """Fetches logs from the container."""
        try:
            return container.logs().decode()
        except Exception as e:
            print(f"Error retrieving logs: {e}")
            return ""

class TaskScheduler:
    def __init__(self):
        self.task_queue = queue.Queue()
        self.running_tasks = {}
        self.lock = threading.Lock()
        self.docker_manager = DockerManager()
        
    def add_task(self, task_id, image_name, command):
        """Adds a new task to the queue."""
        self.task_queue.put((task_id, image_name, command))
        
    def _execute_local_task(self, task_id, image_name, command):
        """Executes a task using Docker on the local machine."""
        with self.lock:
            self.running_tasks[task_id] = "Running"
        
        # Ensure image is available
        self.docker_manager.pull_image(image_name)
        
        # Run the container
        container = self.docker_manager.run_container(image_name, command)
        
        if container:
            logs = self.docker_manager.get_container_logs(container)
            print(f"Task {task_id} Output:\n{logs}")
        else:
            print(f"Task {task_id} failed.")
        
        with self.lock:
            self.running_tasks.pop(task_id, None)
        
    def _process_task_queue(self):
        """Processes tasks from the queue in a separate thread."""
        while True:
            task_id, image_name, command = self.task_queue.get()
            self._execute_local_task(task_id, image_name, command)
            self.task_queue.task_done()
        
    def start_scheduler(self):
        """Starts the task processing thread."""
        worker_thread = threading.Thread(target=self._process_task_queue, daemon=True)
        worker_thread.start()

class NodeManager:
    """
    Manages available compute nodes.
    """
    
    def __init__(self, nodes: List[Dict]):
        """
        Initialize NodeManager with a list of nodes.
        
        Args:
            nodes: List of dictionaries, each representing a compute node
        """
        self.nodes = nodes
    
    def get_available_nodes(self, cpu: float, ram: float, gpu: float) -> List[Dict]:
        """
        Get a list of nodes that meet the specified resource requirements.
        
        Args:
            cpu: Required CPU cores
            ram: Required RAM in GB
            gpu: Required GPU (0 or 1)
            
        Returns:
            List of nodes that meet the requirements
        """
        available_nodes = [
            node for node in self.nodes
            if node["cpu"] >= cpu and node["ram"] >= ram and node["gpu"] >= gpu and node["active"]
        ]
        return available_nodes
    
    def get_active_nodes(self) -> List[Dict]:
        """
        Get a list of all active nodes.
        
        Returns:
            List of active nodes
        """
        return [node for node in self.nodes if node["active"]]

class TaskManager:
    """
    Manages task execution, scheduling, and resource allocation.
    """
    
    def __init__(self, node_manager, local_resources: Dict, max_concurrent_tasks: int = 10):
        """
        Initialize the Task Manager.
        
        Args:
            node_manager: Instance of NodeManager for accessing node resources
            local_resources: Dict containing local fog device resources
            max_concurrent_tasks: Maximum number of concurrent tasks to execute
        """
        self.node_manager = node_manager
        self.local_resources = local_resources
        self.max_concurrent_tasks = max_concurrent_tasks
        
        # Track running tasks and available resources
        self.tasks = {}  # task_id -> Task object
        self.running_tasks = 0
        self.available_cpu = local_resources["cpu"]
        self.available_ram = local_resources["ram"]
        self.available_gpu = local_resources.get("gpu", 0.0)
        
        # Docker client for container management
        self.docker_client = docker.from_env()
        
        # Thread pool for executing tasks
        self.executor = ThreadPoolExecutor(max_workers=max_concurrent_tasks)
        
        # Thread for task processing
        self.processing_thread = threading.Thread(target=self._process_task_queue, daemon=True)
        self.processing_thread.start()
        
        # Task queue
        self.task_queue = []
        self.queue_lock = threading.Lock()
    
    def submit_task(self, task: Task) -> str:
        """
        Submit a new task for execution.
        
        Args:
            task: Task object with all required parameters
            
        Returns:
            task_id: Unique identifier for tracking the task
        """
        with self.queue_lock:
            self.tasks[task.task_id] = task
            self.task_queue.append(task.task_id)
        
        print(f"Task {task.task_id} submitted, type: {task.task_type}")
        return task.task_id
    
    def get_task_status(self, task_id: str) -> Dict:
        """
        Get the current status of a task.
        
        Args:
            task_id: The ID of the task
            
        Returns:
            Dict containing task status information
        """
        task = self.tasks.get(task_id)
        if not task:
            return {"error": "Task not found"}
        
        result = task.to_dict()
        if task.status == "Completed":
            result["results"] = task.results
        
        return result
    
    def _process_task_queue(self) -> None:
        """Background thread to process the task queue."""
        while True:
            try:
                # Check if we have tasks to process and available concurrency
                if self.task_queue and self.running_tasks < self.max_concurrent_tasks:
                    with self.queue_lock:
                        if self.task_queue:
                            # Get the next task ID from the queue
                            task_id = self.task_queue.pop(0)
                            task = self.tasks[task_id]
                            
                            # Schedule the task
                            self._schedule_task(task)
                
                # Sleep to avoid too much CPU usage
                time.sleep(0.1)
                
            except Exception as e:
                print(f"Error in task queue processing: {e}")
                time.sleep(1)  # Sleep longer on error
    
    def _schedule_task(self, task: Task) -> None:
        """
        Determine where to run a task and start execution.
        
        Args:
            task: The task to be scheduled
        """
        task.status = "Scheduling"
        
        # Check if we can run locally
        if (self.available_cpu >= task.estimated_cpu and 
            self.available_ram >= task.estimated_ram and 
            self.available_gpu >= task.estimated_gpu):
            
            # Execute locally
            print(f"Executing task {task.task_id} locally")
            self._reserve_local_resources(task)
            task.status = "Running"
            self.running_tasks += 1
            self.executor.submit(self._execute_local_task, task)
            
        else:
            # Need to find external nodes
            required_nodes = self._find_nodes_for_task(task)
            
            if not required_nodes:
                # No suitable nodes found
                task.status = "Failed"
                print(f"No suitable nodes found for task {task.task_id}")
                return
            
            # Execute on external nodes
            task.assigned_nodes = [node["node_id"] for node in required_nodes]
            task.status = "Running"
            self.running_tasks += 1
            
            if task.is_divisible and len(required_nodes) > 1:
                # Split task across multiple nodes
                self.executor.submit(self._execute_distributed_task, task, required_nodes)
            else:
                # Execute on a single external node
                self.executor.submit(self._execute_remote_task, task, required_nodes[0])
    
    def _find_nodes_for_task(self, task: Task) -> List[Dict]:
        """
        Find suitable nodes for a task.
        
        Args:
            task: The task to find nodes for
            
        Returns:
            List of suitable node dictionaries
        """
        # Check if task can be run on a single node
        single_nodes = self.node_manager.get_available_nodes(
            task.estimated_cpu, task.estimated_ram, task.estimated_gpu
        )
        
        if single_nodes:
            # Found a single node that can handle the task
            return [single_nodes[0]]
        
        # If the task is divisible, try to split it
        if task.is_divisible:
            # Get all active nodes
            all_active_nodes = self.node_manager.get_active_nodes()
            
            # Sort by available resources
            all_active_nodes.sort(key=lambda x: (x["cpu"] + x["ram"]), reverse=True)
            
            # Calculate total available resources
            total_cpu = sum(node["cpu"] for node in all_active_nodes)
            total_ram = sum(node["ram"] for node in all_active_nodes)
            total_gpu = sum(node["gpu"] for node in all_active_nodes)
            
            # Check if combined resources are enough
            if (total_cpu >= task.estimated_cpu and 
                total_ram >= task.estimated_ram and 
                total_gpu >= task.estimated_gpu):
                
                # Determine how many nodes we need
                selected_nodes = []
                required_cpu = task.estimated_cpu
                required_ram = task.estimated_ram
                required_gpu = task.estimated_gpu
                
                for node in all_active_nodes:
                    selected_nodes.append(node)
                    required_cpu -= node["cpu"]
                    required_ram -= node["ram"]
                    required_gpu -= node["gpu"]
                    
                    if required_cpu <= 0 and required_ram <= 0 and required_gpu <= 0:
                        break
                
                return selected_nodes
        
        # If we get here, we couldn't find suitable nodes
        return []
    
    def _reserve_local_resources(self, task: Task) -> None:
        """Reserve local resources for a task."""
        self.available_cpu -= task.estimated_cpu
        self.available_ram -= task.estimated_ram
        self.available_gpu -= task.estimated_gpu
    
    def _release_local_resources(self, task: Task) -> None:
        """Release local resources after task completion."""
        self.available_cpu += task.estimated_cpu
        self.available_ram += task.estimated_ram
        self.available_gpu += task.estimated_gpu
    
    def _execute_local_task(self, task: Task) -> None:
        """
        Execute a task locally using Docker.
        
        Args:
            task: The task to execute
        """
        try:
            # Create task-specific Docker container
            container_name = f"task-{task.task_id}"
            
            # Determine image based on task type
            image = self._get_docker_image_for_task(task.task_type)
            
            # For testing purposes, we'll use a simple echo command instead of real processing
            # This ensures it will work on any system without custom Docker images
            command = f"echo 'Processing {task.task_type} task with input: {json.dumps(task.input_data)}'"
            
            # Run the container
            container = self.docker_client.containers.run(
                image=image,
                command=command,
                mem_limit=f"{int(task.estimated_ram * 1024)}m",
                cpu_quota=int(task.estimated_cpu * 100000),  # Docker CPU quota is in microseconds
                name=container_name,
                detach=True
            )
            
            # Wait for container with timeout
            result = container.wait(timeout=task.max_execution_time)
            
            if result["StatusCode"] == 0:
                # Get container output
                logs = container.logs().decode()
                print(f"Task output: {logs}")
                
                # Simulate successful task completion
                task.results = {"status": "success", "output": logs}
                task.status = "Completed"
                print(f"Task {task.task_id} completed successfully")
            else:
                task.status = "Failed"
                print(f"Task {task.task_id} failed with exit code {result['StatusCode']}")
            
            # Cleanup
            try:
                container.remove()
            except Exception as e:
                print(f"Error removing container: {e}")
            
        except Exception as e:
            task.status = "Failed"
            print(f"Error executing task {task.task_id}: {e}")
        
        finally:
            # Release resources
            self._release_local_resources(task)
            self.running_tasks -= 1
    
    def _get_docker_image_for_task(self, task_type: str) -> str:
        """
        Determine the appropriate Docker image for a task type.
        
        Args:
            task_type: The type of the task
            
        Returns:
            The Docker image name
        """
        # Use public Docker images that are guaranteed to exist
        task_image_mapping = {
            "image_processing": "alpine:latest",
            "text_analysis": "alpine:latest",
            "ml_training": "alpine:latest"
        }
        
        return task_image_mapping.get(task_type, "alpine:latest")
    
    def _execute_distributed_task(self, task: Task, nodes: List[Dict]) -> None:
        """
        Execute a task across multiple remote nodes.
        
        Args:
            task: The task to execute
            nodes: The list of nodes to use
        """
        try:
            # Calculate portion of task for each node
            num_nodes = len(nodes)
            
            # This is just a simulation - in a real implementation, you'd split the data
            print(f"Executing distributed task {task.task_id} across {num_nodes} nodes")
            
            # Simulate task execution
            time.sleep(2)
            
            # Simulate results
            task.results = {"status": "success", "distributed": True, "node_count": num_nodes}
            task.status = "Completed"
            print(f"Distributed task {task.task_id} completed")
            
        except Exception as e:
            task.status = "Failed"
            print(f"Error executing distributed task {task.task_id}: {e}")
        
        finally:
            self.running_tasks -= 1
    
    def _execute_remote_task(self, task: Task, node: Dict) -> None:
        """
        Execute a task on a remote node.
        
        Args:
            task: The task to execute
            node: The node to execute on
        """
        try:
            # This is just a simulation - in a real implementation, you'd send an HTTP request
            print(f"Executing task {task.task_id} on remote node {node['node_id']}")
            
            # Simulate network delay
            time.sleep(1)
            
            # Simulate successful execution
            task.results = {"status": "success", "remote": True, "node_id": node["node_id"]}
            task.status = "Completed"
            print(f"Task {task.task_id} completed on remote node")
            
        except Exception as e:
            task.status = "Failed"
            print(f"Error executing remote task {task.task_id}: {e}")
        
        finally:
            self.running_tasks -= 1
    
    def _send_task_to_remote_node(self, node: Dict, task_data: Dict) -> Dict:
        """
        Send a task to a remote node via HTTP.
        
        Args:
            node: The remote node information
            task_data: The task data to send
            
        Returns:
            The result from the remote node
        """
        # This function would typically make an HTTP request
        # For this example, we'll just simulate a successful response
        return {"status": "Completed", "results": {"simulated": True}}
    
    def _get_remote_task_result(self, task_id: str) -> Any:
        """
        Retrieve the result of a task from a remote node.
        
        Args:
            task_id: The ID of the task
            
        Returns:
            The task result
        """
        # Simulation - would typically make an HTTP request to get the result
        return {"status": "Completed", "data": f"Result for {task_id}"}
    
    def _combine_results(self, results: List[Any]) -> Any:
        """
        Combine partial results from multiple nodes into a single result.
        
        Args:
            results: List of partial results
            
        Returns:
            Combined result
        """
        # In a real implementation, this would combine partial results
        return {"combined": True, "parts": len(results)}


if __name__ == "__main__":
    # Define local resources
    local_resources = {"cpu": 4.0, "ram": 8.0, "gpu": 1.0}
    
    # Create NodeManager with some example nodes
    nodes = [
        {"node_id": "node1", "ip": "192.168.1.100", "port": 5000, "cpu": 2.0, "ram": 4.0, "gpu": 0.0, "active": True},
        {"node_id": "node2", "ip": "192.168.1.101", "port": 5000, "cpu": 4.0, "ram": 8.0, "gpu": 1.0, "active": True},
        {"node_id": "node3", "ip": "192.168.1.102", "port": 5000, "cpu": 8.0, "ram": 16.0, "gpu": 0.0, "active": False}
    ]
    node_manager = NodeManager(nodes)
    
    # Create TaskManager
    task_manager = TaskManager(node_manager, local_resources)
    
    print("Creating and submitting tasks...")
    
    # Create a task that runs locally
    local_task = Task(
        task_type="image_processing",
        input_data={"image_url": "http://example.com/image.jpg"},
        estimated_cpu=1.0,
        estimated_ram=2.0,
        estimated_gpu=0.0,
        is_divisible=False,
        max_execution_time=60
    )
    
    # Submit the task
    local_task_id = task_manager.submit_task(local_task)
    
    # Create a task that should run on a remote node
    remote_task = Task(
        task_type="ml_training",
        input_data={"dataset_url": "http://example.com/dataset.csv"},
        estimated_cpu=6.0,  # More than available locally
        estimated_ram=12.0,  # More than available locally
        estimated_gpu=0.0,
        is_divisible=True,
        max_execution_time=120
    )
    
    # Submit the task
    remote_task_id = task_manager.submit_task(remote_task)
    
    # Wait a bit for the tasks to be processed
    print("Waiting for tasks to complete...")
    time.sleep(5)
    
    # Check task statuses
    local_status = task_manager.get_task_status(local_task_id)
    remote_status = task_manager.get_task_status(remote_task_id)
    
    print(f"\nLocal task status: {local_status['status']}")
    if 'results' in local_status:
        print(f"Local task results: {local_status['results']}")
    
    print(f"\nRemote task status: {remote_status['status']}")
    if 'results' in remote_status:
        print(f"Remote task results: {remote_status['results']}")
    
    # Keep the main thread alive a bit longer to allow task processing
    print("\nWaiting for remaining tasks to complete...")
    time.sleep(5)
    
    print("Done!")
