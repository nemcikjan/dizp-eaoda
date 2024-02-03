import logging
import os
import signal
import threading

from k8s import watch_pods


stop_event = threading.Event()

watch_thread = threading.Thread(target=watch_pods, args=(stop_event,), daemon=True)

def handle_sigterm(*args):
    logging.info("SIGTERM received, shutting down")
    stop_event.set()
    watch_thread.join(timeout=5)
    logging.info("Threads finished, good bye")
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)


if __name__ == "__main__":
    watch_thread.start()