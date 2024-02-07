import logging
import os
import signal
import sys
import threading
from frico_redis import dequeue_item, handle_pod, queues, remove_from_queue
from k8s import delete_pod


QUEUE_NAME = queues.get('DELETE')
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

def delete():
     while not stop_event.is_set():
        if stop_event.is_set():
            break
        pod = dequeue_item(QUEUE_NAME)
        if pod is not None:
            try:
                _ = delete_pod(pod["name"], pod["namespace"])
            except Exception as e:
                logging.warning(f"Unable to delete pod {pod["name"]}: {e}")
            
            try:
                handle_pod(pod["name"], pod["node"])
            except Exception as e:
                logging.error(f"Handling for pod {pod["name"]} failed: {e}")
        else:
            logging.warning("Unable to parse pod")

if __name__ == "__main__":
    logging.info("Starting worker")
    delete()