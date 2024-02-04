from typing import Any
from kubernetes import client, config, watch
from frico import Task
from frico_redis import get_task, handle_pod
import logging
from threading import Event
import time

# config.load_incluster_config()
config.load_config()

class PodData(object):
    def __init__(self, name: str, labels: dict[str], annotations: dict[str], cpu_requirement: int, memory_requirement: int, exec_time: int, node_name: str) -> None:
        self.name = name
        self.labels = labels
        self.annotations = annotations
        self.cpu_requirement = cpu_requirement
        self.memory_requirement = memory_requirement
        self.exec_time = exec_time
        self.node_name = node_name

def init_nodes() -> dict[str, dict[str, bool | int]]:
    # nodes: dict[str, dict[str, Any]] = {
    #     'odin': {
    #         'cpu': 3800,
    #         'memory': 15643608473,
    #         'red': True,
    #         'green': True,
    #         'blue': True
    #     },
    #     'loki': {
    #         'cpu': 3800,
    #         'memory': 3775429017,
    #         'blue': True
    #     },
    #     'thor': {
    #         'cpu': 3800,
    #         'memory': 3775429017,
    #         'green': True,
    #         'blue': True
    #     },
    #     'bjorn': {
    #         'cpu': 3800,
    #         'memory': 3775429017,
    #         'green': True,
    #         'blue': True
    #     },
    # }
    v1 = client.CoreV1Api()
    ret = v1.list_node()
    return {n.metadata.name: {key: True for key in str.split(n.metadata.annotations["colors"], sep=",")} | {'cpu': int(.95 * parse_cpu_to_millicores(n.status.capacity["cpu"])), 'colors': n.metadata.annotations["colors"], 'memory': int(.95 * parse_memory_to_bytes(n.status.capacity["memory"]))} for n in ret.items}

def delete_pod(pod_name: str, namespace: str):
    v1 = client.CoreV1Api()
    try:
        res = v1.delete_namespaced_pod(name=pod_name, namespace=namespace, body=client.V1DeleteOptions(grace_period_seconds=0))
        logging.info(f"Pod {pod_name} deleted")
        return res
    except Exception as e:
        logging.warning(f"Exception when deleting pod: {e}")
        raise e
    
def create_pod(pod_data: PodData, namespace: str):
    v1 = client.CoreV1Api()
    pod = client.V1Pod()
    pod.metadata = client.V1ObjectMeta(name=pod_data.name, labels=pod_data.labels, annotations=pod_data.annotations)
    new_resources = client.V1ResourceRequirements(requests={"cpu": f"{str(pod_data.cpu_requirement)}m", "memory": f"{str(pod_data.memory_requirement)}"})
    pod.spec = client.V1PodSpec(node_selector={"name": pod_data.node_name},  restart_policy="Never", containers=[client.V1Container(name="task", image="alpine:3.19", command=["/bin/sh"], args=["-c", f"sleep {pod_data.exec_time if pod_data.exec_time > 0 else 0} && exit 0"], resources=new_resources)])
    try:
        response = v1.create_namespaced_pod(namespace=namespace, body=pod)
        logging.info(f"Pod {pod_data.name} created")
        return response
    except Exception as e:
        logging.warning(f"Exception while creating pod: {e}")
        raise e

def reschedule(task_name: str, namespace: str, new_node_name: str):
    v1 = client.CoreV1Api()
    try:
        logging.info(f"Rescheduling task {task_name} to {new_node_name}")
        pod = None
        try:
            pod = v1.read_namespaced_pod(name=task_name, namespace=namespace)
        except Exception as e:
            logging.warning(f"Got you fucker {task_name}")
        if pod is not None:
            try:
                res = delete_pod(task_name, namespace)
                logging.info(f"Pod {task_name} deleted due rescheduling")
            except Exception as e:
                logging.warning(f"Exception when deleting pod during rescheduling: {e}")
 
        new_labels = {}
        new_annotations = {}
        new_exec_time = 0
        task = get_task(new_node_name, task_name)
        if pod is None:
            new_annotations["v2x.context/priority"] = str(task['p'])
            new_annotations["v2x.context/color"] = task['c']
            new_labels["arrival_time"] = str(int(time.time()))
            new_labels["frico"] = "true"
            new_labels["task_id"] = task_name
            # logging.warning("We must fix this")
        else:
            new_labels = pod.metadata.labels
            new_annotations = pod.metadata.annotations
            arrival_time = int(pod.metadata.labels["arrival_time"])
            exec_time = int(pod.metadata.labels["exec_time"])
            new_exec_time = exec_time - (int(time.time()) - arrival_time)
            new_exec_time = new_exec_time if new_exec_time > 0 else 0

        new_labels["node_name"] = new_node_name
        new_labels["frico_skip"] = "true"
        new_labels["exec_time"] = str(new_exec_time)
        new_annotations["v2x.context/exec_time"] = str(new_exec_time)

        
        
        ppod = PodData(name=task_name, labels=new_labels, annotations=new_annotations, cpu_requirement=task["cpu"], memory_requirement=task["mem"], exec_time=new_exec_time, node_name=new_node_name)
        try:
            response = create_pod(ppod, namespace)
            logging.info(f"Pod {task_name} rescheduled")
            return response
        except Exception as e:
            logging.warning(f"Exception when creating pod during rescheduling: {e}")
    except Exception as e:
        logging.warning(f"Exception when rescheduling pod: {e}")
        raise e

def watch_pods(stop_signal: Event):
    # Create a client for the CoreV1 API
    corev1 = client.CoreV1Api()

    # Create a watcher for Pod events
    w = watch.Watch()
    while not stop_signal.is_set():
        logging.info("Starting watching for pods")
        # Watch for events related to Pods

        for event in w.stream(corev1.list_namespaced_pod, "tasks", field_selector="status.phase=Succeeded", label_selector="frico=true"):
            pod = event['object']
            event_type = event['type']

            if stop_signal.is_set():
                break

            try:
                # if "frico" in pod.metadata.labels and pod_status == "Succeeded":
                if event_type == "ADDED":
                    logging.info(f"Pod {pod.metadata.name} succeeded")
                    handle_pod(pod.metadata.name, pod.spec.node_name)
                    delete_pod(pod.metadata.name, pod.metadata.namespace)
            except Exception as e:
                logging.warning(f"Error while handling pod deletion in thread {e}")


    logging.info("Stopping thread")
    w.stop()

def prepare_and_create_pod(pod_data: dict[str, Any]) -> dict[str, str]:
    annotations = {
        "v2x.context/priority": str(pod_data["priority"]),
        "v2x.context/color": pod_data["color"],
        "v2x.context/exec_time": str(pod_data["exec_time"])
    }
    labels = {
        "arrival_time": pod_data["arrival_time"],
        "exec_time": str(pod_data["exec_time"]),
        "task_id": pod_data["task_id"],
        "frico": "true",
        "node_name": pod_data["node_name"]
    }
    p = PodData(name=pod_data['task_id'], annotations=annotations, labels=labels, cpu_requirement=pod_data["cpu"], memory_requirement=pod_data["memory"], exec_time=int(pod_data["exec_time"]), node_name=pod_data["node_name"])
    try:
        _ = create_pod(p, "tasks")
        return {"message": f"Pod {pod_data['task_id']} created"}
    except Exception as e:
        logging.error(f"Error while creating pod: {e}")
        return {"message": f"Error while creating pod: {e}"}

def parse_cpu_to_millicores(cpu_str: str):
    """
    Parse CPU resource string to millicores.
    Ex: "500m" -> 500, "1" -> 1000
    """
    if cpu_str.endswith('m'):
        return int(cpu_str[:-1])
    else:
        return int(float(cpu_str) * 1000)

def parse_memory_to_bytes(mem_str: str):
    """
    Parse memory resource string to bytes.
    Ex: "1Gi" -> 1073741824, "500Mi" -> 524288000
    """
    unit_multipliers = {
        'Ki': 1024,
        'Mi': 1024**2,
        'Gi': 1024**3,
        'Ti': 1024**4,
        'Pi': 1024**5,
        'Ei': 1024**6,
        'k': 1000,
        'M': 1000**2
    }
    if mem_str[-2:] in unit_multipliers:
        return int(float(mem_str[:-2]) * unit_multipliers[mem_str[-2:]])
    elif mem_str[-1] in unit_multipliers:
        return int(float(mem_str[:-1]) * unit_multipliers[mem_str[-1]])
    else:
        return int(mem_str)
    

#     type PodData struct {
# 	Cpu      int    `json:"cpu"`
# 	Memory   int    `json:"memory"`
# 	Priority int    `json:"priority"`
# 	Color    string `json:"color"`
# 	ExecTime int    `json:"execTime"`
# }

# type PodRequest struct {
# 	PodData
# 	Name string `json:"name"`
# }
