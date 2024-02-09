from kubernetes import client, config
import time
from frico_redis import enqueue_item, queues, get_all_tasks, release_task
import utils
import logging
import os
import threading
import signal
import sys

config.load_config()
v1 = client.CoreV1Api()

LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

logging.basicConfig(level=LOG_LEVEL, handlers=(logging.StreamHandler(sys.stdout),),
                    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')


stop_event = threading.Event()

def reverse_cleanup():
    try:
        pods = v1.list_namespaced_pod("tasks")
        pod_list = [pod.metadata.name for pod in pods.items]
        tasks = [(t.split(':')[-2], t.split(':')[-1])for t in get_all_tasks()]
    except Exception as e:
        logging.warning(f"just in case: {e}")
    for t, n in tasks:
        if t not in pod_list:
            try:
                logging.info(f"Zombie {t} found on {n}")
                release_task(n, t)
            except Exception as e:
                logging.warning(f"I don't understand: {e}")

cleanup_thread = threading.Thread(target=reverse_cleanup, daemon=True)
cleanup_thread.start()

def handle_sigterm(*args):
    logging.info("SIGTERM received, shutting down")
    stop_event.set()
    cleanup_thread.join(timeout=5)
    logging.info("Threads finished, good bye")
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)


def check_pods_status(namespace='default'):
    # Load the kubeconfig file

    # Create an instance of the API class

    logging.info(f"Checking pods in namespace '{namespace}'...")

    # List all the pods in the specified namespace
    pods = v1.list_namespaced_pod(namespace)

    for pod in pods.items:
        if "eaoda-phase" not in pod.metadata.labels:
        # Check the phase of each pod
            if pod.status.phase == "Succeeded":
                logging.info(f"Pod {pod.metadata.name} succeeded.")
                enqueue_item(queues.get('DELETE'), {"name": pod.metadata.name, "namespace": pod.metadata.namespace})
            elif pod.status.phase == "Failed":
                logging.info(f"Pod {pod.metadata.name} failed.")
                enqueue_item(queues.get('DELETE'), {"name": pod.metadata.name, "namespace": pod.metadata.namespace})

if __name__ == "__main__":
    while not stop_event.is_set():
        check_pods_status('tasks')  # Specify your namespace here
        time.sleep(5)
    logging.info("Bey bey")