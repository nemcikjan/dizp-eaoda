from flask import Flask, request, jsonify
from frico import FRICO, Task
from prometheus_flask_exporter import PrometheusMetrics
from prometheus_client import Counter, Gauge
from k8s import init_nodes, prepare_and_create_pod, reschedule, delete_pod
from frico_redis import dequeue_item, enqueue_item, is_admissable, release_task, init
import os
import threading
import http
import logging
import signal
import time
import queue
import csv

request_events: dict[str, threading.Event] = {}
request_results: dict[str, tuple[bool, str, dict[str]]] = {}

pod_queue = queue.Queue()

QUEUE_NAME = 'tasks_queue'

allocated_tasks_counter = Counter('allocated_tasks', 'Allocated tasks per node', ['node', 'simulation'])
unallocated_tasks_counter = Counter('unallocated_tasks', 'Unallocated tasks', ['simulation'])
total_tasks_counter = Counter('total_tasks', 'Total tasks', ['simulation'])
reallocated_tasks_counter = Counter('reallocated_tasks', 'Realocated tasks', ['simulation'])
objective_value_gauge = Gauge('objective_value', 'Current objective value', ['simulation'])
offloaded_tasks_counter = Counter('offloaded_tasks', 'Offloaded tasks', ['simulation'])
processing_pod_time = Gauge('pod_processing_time', 'Task allocation time', ['pod', 'simulation'])
kube_processing_pod_time = Gauge('kube_pod_processing_time', 'K8S task processing time', ['pod', 'simulation'])
priority_counter = Gauge('priority', 'Task priority', ['pod', 'priority', 'simulation'])
unallocated_priority_counter = Gauge('unallocated_priorities', 'Unallocated task priority', ['priority', 'simulation'])

MAX_REALLOC = int(os.environ.get("MAX_REALLOC"))
SIMULATION_NAME = os.environ.get("SIMULATION_NAME")
LOG_PATH = os.environ.get("LOG_PATH")
CURRENT_TIME = str(int(time.time()))
SIMULATION_NAME = SIMULATION_NAME + f"-{CURRENT_TIME}"
TEST_BED_PATH = os.path.join(os.environ.get("TEST_BED_PATH"),f'{SIMULATION_NAME}-{CURRENT_TIME}.csv')

eaoda = Flask(__name__)

logging.basicConfig(filename=LOG_PATH, level=logging.INFO, 
                    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')

metrics = PrometheusMetrics(eaoda)


nodes: dict[str, dict[str, bool | int]] = init_nodes()

with open('simulation.id', 'w', newline='') as file:
    file.write(SIMULATION_NAME)
    file.close()

solver = FRICO(MAX_REALLOC)

stop_event = threading.Event()

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
        # Process the pod here (mutate, etc.)
        # Replace the following line with your actual mutation logic
            pod_name = pod["name"]
            priority = int(pod["priority"])
            color = pod["color"]
            exec_time = pod["execTime"]
            cpu = int(pod["cpu"])
            memory = int(pod["memory"]) * 1024**2
            arrival_time = int(time.time())

            thr = threading.Thread(target=append_test_bed, args=(pod_name, priority, color, exec_time, arrival_time, cpu, memory))
            thr.start()
            
            eaoda.logger.info(f"Name: {pod_name} Priority: {priority} Color: {color} Exec time: {exec_time}")
            task = Task(pod_name, pod_name, cpu, memory, priority, color, arrival_time, exec_time)
            total_tasks_counter.labels(simulation=SIMULATION_NAME).inc()

            node_name = ''
            shit_to_be_done: dict[str, str] = {}
            frico_start_time = time.perf_counter()
            if is_admissable(task.cpu_requirement, task.memory_requirement, task.color):
                node_name, shit_to_be_done = solver.solve(task)
            frico_end_time = time.perf_counter()

            allowed = node_name != ''
            processing_pod_time.labels(pod=pod_name, simulation=SIMULATION_NAME).set(frico_end_time - frico_start_time)
            if allowed:
                allocated_tasks_counter.labels(node=node_name, simulation=SIMULATION_NAME).inc()
                objective_value_gauge.labels(simulation=SIMULATION_NAME).inc(task.objective_value())
                priority_counter.labels(simulation=SIMULATION_NAME, pod=pod_name, priority=str(task.priority)).inc()

                # task_to_last_node: dict[str, tuple[Task, Node]] = {}

                # # Iterate through the array and filter it
                # filtered_array: list[tuple[Task, Node]] = []

                # for task, node in shit_to_be_done:
                #     task_to_last_node[task.id] = (task, node)

                # filtered_array = [task_node_tuple for _, task_node_tuple in task_to_last_node.items()]

                # for shit, to_shit in shit_to_be_done:
                for _, (shit, to_shit) in shit_to_be_done.items():
                    if to_shit is None:
                        try:
                            res = delete_pod(shit, "tasks")
                        except Exception as e:
                            logging.warning(f"There was an issue deleting pod during offloading. Probably finished first")
                        offloaded_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
                        priority_counter.labels(simulation=SIMULATION_NAME,pod=pod_name, priority=priority).dec()
                    else:
                        try:
                            res = reschedule(shit, "tasks", to_shit)
                        except:
                            logging.warning(f"Removing pod {shit} from {to_shit} failed. Finished before reschedeling")
                            release_task(to_shit, shit)
                        reallocated_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
                    
            else:
                unallocated_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
                unallocated_priority_counter.labels(simulation=SIMULATION_NAME, priority=str(task.priority)).inc()

            
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

            eaoda.logger.info(f"Task {pod_name} -> node {node_name}")
            res = prepare_and_create_pod(pod_data)
            eaoda.logger.info(res["message"])

        except Exception as e:
            logging.warning(f"Exception occured: {e}")

queue_thread = threading.Thread(target=process_pod, daemon=True)
queue_thread.start()
# Create a thread that runs the 'task' function
# cleanup_thread = threading.Thread(target=remove_expired, args=(solver,))
# cleanup_thread.start()

# def rescheduling(shit: Task, to_shit: Node, pod_id: str, priority: str) -> None:
#     if to_shit is None:
#         try:
#             res = delete_pod(shit.name, "tasks")
#         except Exception as e:
#             logging.warning(f"There was an issue deleting pod during offloading. Probably finished first")
#         offloaded_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
#         priority_counter.labels(simulation=SIMULATION_NAME,pod=pod_id, priority=priority).dec()
#     else:
#         try:
#             res = reschedule(shit, "tasks", to_shit.name)
#         except:
#             logging.warning(f"Removing pod {shit.name} from {to_shit.name} failed. Finished before reschedeling")
#             solver.release(shit, to_shit)
#         reallocated_tasks_counter.labels(simulation=SIMULATION_NAME).inc()

# def process_pod():
#     while not stop_event.is_set():
#         if stop_event.is_set():
#             break
#         # Get the pod data and its unique identifier from the queue
#         pod_id, pod = pod_queue.get()
#         try:
#         # Process the pod here (mutate, etc.)
#         # Replace the following line with your actual mutation logic
#             pod_name = pod["name"]
#             priority = int(pod["priority"])
#             color = pod["color"]
#             exec_time = pod["execTime"]
#             cpu = int(pod["cpu"])
#             memory = int(pod["memory"]) * 1024**2
#             arrival_time = int(time.time())
            
#             eaoda.logger.info(f"Name: {pod_name} Priority: {priority} Color: {color} Exec time: {exec_time}")
#             row_to_append = [pod_id, priority.value, color, exec_time, str(arrival_time), cpu, memory]

#             with open(TEST_BED_PATH, 'a', newline='') as file:
#                 writer = csv.writer(file)
#                 writer.writerow(row_to_append)
#                 file.close()

#             task = Task(pod_id, pod_name, cpu, memory, priority, color, arrival_time, exec_time)
#             total_tasks_counter.labels(simulation=SIMULATION_NAME).inc()

#             node_name = ''
#             shit_to_be_done: dict[str, tuple[Task, Node]] = {}
#             frico_start_time = time.perf_counter()
#             if solver.is_admissable(task):
#                 node_name, shit_to_be_done = solver.solve(task)
#             frico_end_time = time.perf_counter()

#             allowed = node_name != ''
#             processing_pod_time.labels(pod=pod_id, simulation=SIMULATION_NAME).set(frico_end_time - frico_start_time)
#             if allowed:
#                 allocated_tasks_counter.labels(node=node_name, simulation=SIMULATION_NAME).inc()
#                 objective_value_gauge.labels(simulation=SIMULATION_NAME).inc(task.objective_value())
#                 priority_counter.labels(simulation=SIMULATION_NAME, pod=pod_id, priority=str(task.priority.value)).inc()

#                 # task_to_last_node: dict[str, tuple[Task, Node]] = {}

#                 # # Iterate through the array and filter it
#                 # filtered_array: list[tuple[Task, Node]] = []

#                 # for task, node in shit_to_be_done:
#                 #     task_to_last_node[task.id] = (task, node)

#                 # filtered_array = [task_node_tuple for _, task_node_tuple in task_to_last_node.items()]

#                 # for shit, to_shit in shit_to_be_done:
#                 for _, (shit, to_shit) in shit_to_be_done.items():
#                     if to_shit is None:
#                         try:
#                             res = delete_pod(shit.name, "tasks")
#                         except Exception as e:
#                             logging.warning(f"There was an issue deleting pod during offloading. Probably finished first")
#                         offloaded_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
#                         priority_counter.labels(simulation=SIMULATION_NAME,pod=pod_id, priority=priority).dec()
#                     else:
#                         try:
#                             res = reschedule(shit, "tasks", to_shit.name)
#                         except:
#                             logging.warning(f"Removing pod {shit.name} from {to_shit.name} failed. Finished before reschedeling")
#                             solver.release(shit, to_shit)
#                         reallocated_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
                    
#             else:
#                 unallocated_tasks_counter.labels(simulation=SIMULATION_NAME).inc()
#                 unallocated_priority_counter.labels(simulation=SIMULATION_NAME, priority=str(task.priority.value)).inc()

#             pod_data = {
#                 "node_name": node_name,
#                 "task_id": pod_id,
#                 "arrival_time": str(arrival_time),
#                 "exec_time": str(exec_time),
#                 "priority": priority,
#                 "color": color,
#                 "cpu": cpu,
#                 "memory": memory
#             }

#             eaoda.logger.info(f"Task {pod_name} -> node {node_name}")

#         # Store the processed result for this pod_id
#             eaoda.logger.info(f"Setting results for pod {pod_name}: {(allowed, f"Task {pod_name} assigned to {node_name}" if allowed else f"No capacity for task {pod_name}")}")
#             request_results[pod_id] = (allowed, f"Task {pod_name} assigned to {node_name}" if allowed else f"No capacity for task {pod_name}", pod_data if allowed else {})
#         except Exception as e:
#             logging.warning(f"Exception occured: {e}")
#             request_results[pod_id] = (False, f"Exception occured: {e}", {})
#         finally:
#             pod_queue.task_done()
#             # Signal that processing is complete
#             request_events[pod_id].set()
    
#     logging.info("Stopping pod processing thread")

# pod_process_thread = threading.Thread(target=process_pod, daemon=True)
# pod_process_thread.start()

def handle_sigterm(*args):
    eaoda.logger.info("SIGTERM received, shutting down")
    stop_event.set()
    # watch_thread.join(timeout=5)
    queue_thread.join(timeout=5)
    eaoda.logger.info("Threads finished, good bye")
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

@eaoda.route("/health", methods=["GET"])
def health():
    return ("", http.HTTPStatus.NO_CONTENT)

@eaoda.route('/create', methods=['POST'])
def create():
    req = request.get_json()
    # pod_id = req["name"]

    enqueue_item(QUEUE_NAME, req)

    return {"message": "Pod process started"}

    request_events[pod_id] = threading.Event()
    pod_queue.put((pod_id, req))
    kube_processing_time_start = time.perf_counter()
    request_events[pod_id].wait()
    kube_processing_time_end = time.perf_counter()
    kube_processing_pod_time.labels(pod=pod_id, simulation=SIMULATION_NAME).set(kube_processing_time_end - kube_processing_time_start)

    allowed, message, pod_data = request_results.pop(pod_id)

    # Clean up: remove the event for this request
    del request_events[pod_id]

    if allowed:
        prepare_and_create_pod(pod_data)

    return {"message": message}


if __name__ == '__main__':
    eaoda.run(host='0.0.0.0', port=8080)