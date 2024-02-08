from kubernetes import client, config
import time
from frico_redis import enqueue_item, queues

config.load_config()
v1 = client.CoreV1Api()

def check_pods_status(namespace='default'):
    # Load the kubeconfig file

    # Create an instance of the API class

    print(f"Checking pods in namespace '{namespace}'...")

    # List all the pods in the specified namespace
    pods = v1.list_namespaced_pod(namespace)

    for pod in pods.items:
        # Check the phase of each pod
        if pod.status.phase == "Succeeded":
            print(f"Pod {pod.metadata.name} succeeded.")
            enqueue_item(queues.get('DELETE'), {"name": pod.metadata.name, "namespace": pod.metadata.namespace, "node": pod.spec.node_name})
        elif pod.status.phase == "Failed":
            print(f"Pod {pod.metadata.name} failed.")
            enqueue_item(queues.get('DELETE'), {"name": pod.metadata.name, "namespace": pod.metadata.namespace, "node": pod.spec.node_name})

if __name__ == "__main__":
    while True:
        check_pods_status('tasks')  # Specify your namespace here
        time.sleep(5)