import logging
import os
import signal
import sys
import threading
from frico_redis import dequeue_item, queues
from k8s import prepare_and_create_pod

QUEUE_NAME = queues.get('CREATE')
LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

logging.basicConfig(level=LOG_LEVEL, handlers=(logging.StreamHandler(sys.stdout),),
                    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')


stop_event = threading.Event()

def handle_sigterm(*args):
    logging.info("SIGTERM received, shutting down")
    stop_event.set()
    logging.info("Threads finished, good bye")
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

def create():
     while not stop_event.is_set():
        if stop_event.is_set():
            break
        pod = dequeue_item(QUEUE_NAME)
        # pod_data = {
        #     "node_name": node_name,
        #     "task_id": pod_name,
        #     "arrival_time": str(arrival_time),
        #     "exec_time": str(exec_time),
        #     "priority": priority,
        #     "color": color,
        #     "cpu": cpu,
        #     "memory": memory
        # }
        if pod is not None:
            try:
                _ = prepare_and_create_pod(pod, True)
            except Exception as e:
                logging.warning(f"Unable to create pod {pod["task_id"]}: {e}")
        else:
            logging.warning("Unable to parse pod")

if __name__ == "__main__":
    logging.info("Starting worker")
    create()