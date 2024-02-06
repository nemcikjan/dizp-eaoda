import logging
import os
import signal
import sys
import threading

from k8s import watch_pods

logging.basicConfig(level=logging.INFO, handlers=(logging.StreamHandler(sys.stdout),),
                    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')


stop_event = threading.Event()

def handle_sigterm(*args):
    logging.info("SIGTERM received, shutting down")
    stop_event.set()
    logging.info("Threads finished, good bye")
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

if __name__ == "__main__":
    logging.info("Starting worker")
    watch_pods(stop_event)