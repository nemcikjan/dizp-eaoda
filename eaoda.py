from datetime import datetime
from flask import Flask
from frico import FRICO, Task
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Counter, Gauge
from k8s import init_nodes
from frico_redis import dequeue_item, enqueue_item, is_admissable, init, queues
import os
import threading
import logging
import signal
import time
import csv

QUEUE_NAME = queues.get('TASKS')

allocated_tasks_counter = Counter('allocated_tasks', 'Allocated tasks per node', ['node', 'simulation'])
unallocated_tasks_counter = Counter('unallocated_tasks', 'Unallocated tasks', ['simulation'])
total_tasks_counter = Counter('total_tasks', 'Total tasks', ['simulation'])
reallocated_tasks_counter = Counter('reallocated_tasks', 'Realocated tasks', ['simulation'])
objective_value_gauge = Gauge('objective_value', 'Current objective value', ['simulation'])
offloaded_tasks_counter = Counter('offloaded_tasks', 'Offloaded tasks', ['simulation'])
processing_pod_time = Gauge('pod_processing_time', 'Task allocation time', ['pod', 'simulation'])
priority_counter = Gauge('priority', 'Task priority', ['pod', 'priority', 'simulation'])
unallocated_priority_counter = Gauge('unallocated_priorities', 'Unallocated task priority', ['priority', 'simulation'])

MAX_REALLOC = int(os.environ.get("MAX_REALLOC"))
SIMULATION_NAME = os.environ.get("SIMULATION_NAME")
LOG_PATH = os.environ.get("LOG_PATH")
CURRENT_TIME = int(time.time())
TEST_BED_PATH = os.path.join(os.environ.get("TEST_BED_PATH"),f'{SIMULATION_NAME}-{datetime.fromtimestamp(CURRENT_TIME).strftime('%Y-%m-%d-%H-%M-%S')}.csv')
SIMULATION_NAME = SIMULATION_NAME + f"-{CURRENT_TIME}"
LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)


eaoda = Flask(__name__)

logging.basicConfig(filename=LOG_PATH, level=LOG_LEVEL, 
                    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')

metrics = PrometheusMetrics(eaoda)

nodes: dict[str, dict[str, bool | int]] = init_nodes()

init(nodes)

with open('simulation.id', 'w', newline='') as file:
    file.write(SIMULATION_NAME)
    file.close()

solver = FRICO(MAX_REALLOC)

stop_event = threading.Event()

def handle_sigterm(*args):
    logging.info("SIGTERM received, shutting down")
    stop_event.set()
    logging.info("Threads finished, good bye")
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

def append_test_bed(pod_name: str, priority: int, color: str, exec_time: str, arrival_time: int, cpu: int, memory: int):
    row_to_append = [pod_name, priority, color, exec_time, str(arrival_time), cpu, memory]
    with open(TEST_BED_PATH, 'a', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(row_to_append)
        file.close()


def process_pod():
     while not stop_event.is_set():
        
        if stop_event.is_set():
            break

        pod = dequeue_item(QUEUE_NAME)
        try:
            logging.info(f"Dequeueing item {pod}")
            pod_name = pod["name"]
            priority = int(pod["priority"])
            color = pod["color"]
            exec_time = pod["execTime"]
            cpu = int(pod["cpu"])
            memory = int(pod["memory"]) * 1024**2
            arrival_time = int(time.time())

            thr = threading.Thread(target=append_test_bed, args=(pod_name, priority, color, exec_time, arrival_time, cpu, memory))
            thr.start()
            
            logging.info(f"Name: {pod_name} Priority: {priority} Color: {color} Exec time: {exec_time}")
            task = Task(pod_name, pod_name, cpu, memory, priority, color, arrival_time, exec_time)
            total_tasks_counter.labels(simulation=SIMULATION_NAME).inc()

            node_name = ''
            shit_to_be_done: dict[str, str] = {}
            frico_start_time = time.perf_counter()
            logging.info(f"Is admissable {pod_name}: {is_admissable(task.cpu_requirement, task.memory_requirement, task.color)}")
            if is_admissable(task.cpu_requirement, task.memory_requirement, task.color):
                node_name, shit_to_be_done = solver.solve(task)
            frico_end_time = time.perf_counter()

            logging.info(f"Node name: {node_name}")

            allowed = node_name != ''
            processing_pod_time.labels(pod=pod_name, simulation=SIMULATION_NAME).set(frico_end_time - frico_start_time)
            if allowed:
                allocated_tasks_counter.labels(node=node_name, simulation=SIMULATION_NAME).inc()
                priority_counter.labels(simulation=SIMULATION_NAME, pod=pod_name, priority=str(task.priority)).inc()
                for shit, to_shit in shit_to_be_done.items():
                    if to_shit is None:
                        try:
                            enqueue_item(queues.get('DELETE'), {"name": shit, "namespace": "tasks"})
                        except Exception as e:
                            logging.warning(f"There was an issue deleting pod during offloading. Probably finished first")
                        offloaded_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
                        priority_counter.labels(simulation=SIMULATION_NAME,pod=pod_name, priority=priority).dec()
                    else:
                        try:
                            enqueue_item(queues.get('RESCHEDULE'), {"name": shit, "namespace": "tasks", "node": to_shit})
                            reallocated_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
                        except:
                            logging.warning(f"Removing pod {shit} from {to_shit} failed. Finished before reschedeling")
                    
                pod_data = {
                    "node_name": node_name,
                    "task_id": pod_name,
                    "arrival_time": str(arrival_time),
                    "exec_time": str(exec_time),
                    "priority": priority,
                    "color": color,
                    "cpu": cpu,
                    "memory": memory
                }

                logging.info(f"Task {pod_name} -> node {node_name}")
                enqueue_item(queue_name=queues.get('CREATE'), item=pod_data)
                    
            else:
                unallocated_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
                unallocated_priority_counter.labels(simulation=SIMULATION_NAME, priority=str(task.priority)).inc()


        except Exception as e:
            logging.error(f"Exception occured: {e}")

queue_thread = threading.Thread(target=process_pod, daemon=True)
queue_thread.start()

if __name__ == "__main__":
    eaoda.run(host='0.0.0.0', port=8080)