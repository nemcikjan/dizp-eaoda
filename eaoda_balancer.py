from flask import Flask, request, jsonify
from frico_redis import enqueue_item, queues
import os
import threading
import http
import logging
import signal
import sys

QUEUE_NAME = queues.get('TASKS')
LOG_LEVEL = os.environ.get("LOG_LEVEL", logging.INFO)

eaoda_balancer = Flask(__name__)

logging.basicConfig(level=LOG_LEVEL, handlers=(logging.StreamHandler(sys.stdout),),
                    format='%(asctime)s %(levelname)s: %(message)s [in %(pathname)s:%(lineno)d]')

stop_event = threading.Event()

def handle_sigterm(*args):
    eaoda_balancer.logger.info("SIGTERM received, shutting down")
    stop_event.set()
    os._exit(0)

signal.signal(signal.SIGTERM, handle_sigterm)

@eaoda_balancer.route("/health", methods=["GET"])
def health():
    return ("", http.HTTPStatus.NO_CONTENT)

@eaoda_balancer.route('/create', methods=['POST'])
def create():
    req = request.get_json()

    enqueue_item(QUEUE_NAME, req)

    return jsonify({"message": "Pod process started"})

if __name__ == '__main__':
    eaoda_balancer.run(host='0.0.0.0', port=8080)