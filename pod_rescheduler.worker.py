import logging
import os
import signal
import sys
import threading
from frico_redis import dequeue_item, queues
from k8s import reschedule


QUEUE_NAME = queues.get('RESCHEDULE')
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

def reschdule_pod():
     while not stop_event.is_set():
        if stop_event.is_set():
            break
        pod = dequeue_item(QUEUE_NAME)
        if pod is not None:
            try:
                _ = reschedule(pod["name"], pod["namespace"], pod["node"])
            except Exception as e:
                logging.warning(f"Unable to delete pod {pod["name"]}: {e}")
        else:
            logging.warning("Unable to parse pod")

if __name__ == "__main__":
    logging.info("Starting worker")
    reschdule_pod()