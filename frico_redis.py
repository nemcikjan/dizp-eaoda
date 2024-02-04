import json
import os
from typing import Optional, Any
import uuid
import redis
import logging

# task
# def objective_value(self):
#     return (self.priority.value / 5) * ((((self.node_cpu_capacity - self.cpu_requirement) / self.node_cpu_capacity) + ((self.node_memory_capacity - self.memory_requirement) / self.node_memory_capacity)) / 2)

# node
# def __lt__(self, other):
#     return (self.id, (self.cpu_capacity - self.remaining_cpu_capacity) / self.cpu_capacity, (self.memory_capacity - self.remaining_memory_capacity) / self.memory_capacity) < (other.id, (other.cpu_capacity - other.remaining_cpu_capacity) / other.cpu_capacity, (other.memory_capacity - other.remaining_memory_capacity) / other.memory_capacity)

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

r = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT),password=REDIS_PASSWORD, decode_responses=True, protocol=3)
r.execute_command('CLIENT TRACKING ON')

knapsack_key_prefix = 'meta:nodes:{node}'
task_key_prefix = 'meta:tasks:{task}:{node}'
sorted_knapsacks_key = 'nodes'
sorted_tasks_per_node_key_prefix = 'tasks:{node}'
temp_tasks_key_prefix = 'temp_tasks:{task}'
nodes_mutex_key_prefix = 'mutex:nodes:{node}'
tasks_mutex_key_prefix = 'mutex:tasks:{task}'

def node_mutex_key(node: str) -> str:
    return nodes_mutex_key_prefix.format(node=node)

def task_mutex_key(task: str) -> str:
    return tasks_mutex_key_prefix.format(task=task)

def knapsack_key(node: str) -> str:
    return knapsack_key_prefix.format(node=node)

def temp_task_key(task: str) -> str:
    return temp_tasks_key_prefix.format(task=task)

def task_key(task: str, node: str) -> str:
    return task_key_prefix.format(task=task, node=node)

def sorted_tasks_per_node_key(node: str) -> str:
    return sorted_tasks_per_node_key_prefix.format(node=node)

def calculate_capacity(node: str) -> float:
    d = r.hgetall(knapsack_key(node))
    return (((int(d['cpu_cap']) - int(d['cpu_free'])) / int(d['cpu_cap'])) + ((int(d['memory_cap']) - int(d['memory_free'])) / int(d['memory_cap']))) / 2

def calculate_obj(prio: int, node: str, cpu: int, mem: int) -> float:
    d = r.hgetall(knapsack_key(node))
    return (prio / 5) * (((( int(d['cpu_cap']) - cpu) / int(d['cpu_cap'])) + ((int(d['memory_cap']) - mem) / int(d['memory_cap']))) / 2)

def get_node(node: str) -> dict:
    return r.hgetall(knapsack_key(node))

def update_node(node: str) -> None:
    r.zadd(sorted_knapsacks_key, {node: calculate_capacity(node)}, xx=True)

def increase_capacity(node: str, cpu: int, mem: int) -> None:
    r.hincrby(knapsack_key(node), "cpu_free", cpu)
    r.hincrby(knapsack_key(node), "memory_free", mem)
    update_node(node)

def decrease_capacity(node: str, cpu: int, mem: int) -> None:
    r.hincrby(knapsack_key(node), "cpu_free", -cpu)
    r.hincrby(knapsack_key(node), "memory_free", -mem)
    update_node(node)

def allocate_task(node: str, task: str, cpu: int, mem: int, prio: int, color: int) -> None:
    logging.info(f"Allocation of task {task} to node {node}")
    try:
        r.hset(task_key(task, node), mapping={'cpu': cpu, 'p': prio, 'mem': mem, 'c': color})
        decrease_capacity(node, cpu, mem)
        r.zadd(sorted_tasks_per_node_key(node), {task: calculate_obj(prio, node, cpu, mem)})
        # update_node(node)
    except Exception as e:
        logging.warning(f"Exception occured while allocating {e}")
        raise e

def release_task(node: str, task: str) -> None:
    key = task_key(task, node)
    cpu = int(r.hget(key, 'cpu'))
    mem = int(r.hget(key, 'mem'))
    r.delete(key)
    increase_capacity(node, cpu, mem)
    r.zrem(sorted_tasks_per_node_key(node), task)
    # update_node(node)

def get_task(node: str, task: str) -> dict:
    return r.hgetall(task_key(task, node))

def get_nodes() -> list[str]:
    return r.zrange(sorted_knapsacks_key, start=0, end=-1)

def get_max() -> str:
    return r.zpopmax(sorted_knapsacks_key)[0]

def get_node_colors(node: str) -> list[str]:
    return r.hget(knapsack_key(node), 'colors').split(',')

def has_color_node(node: str, color: str) -> bool:
    return r.hexists(knapsack_key(node), color)

def get_node_tasks(node: str) -> list[str]:
    return r.zrange(sorted_tasks_per_node_key(node), start=0, end=-1)

def number_of_nodes() -> int:
    return r.zcard(sorted_knapsacks_key)

def relocate_task(task_name: str, to_node: str) -> None:
    from_node = ''
    for k in r.scan_iter(match=f'meta:tasks:{task_name}:*'):
        from_node = k.split(':')[-1]
    task = r.hgetall(task_key(task_name, from_node))
    release_task(from_node, task_name)
    allocate_task(to_node, task_name, task['cpu'], task['mem'], task['p'], task['c'])

def get_node_name_from_meta_key(key: str):
    return key.split(':')[-1]

def is_admissable(cpu: int, mem: int, color: str) -> bool:
    overall_free_cpu = 0
    overall_free_memory = 0
    for k in r.scan_iter(match='meta:nodes:*'):
        # logging.info(f"Iterating node {k}, color {color}, has task color {has_color_node(get_node_name_from_meta_key(k), color)}")
        if has_color_node(get_node_name_from_meta_key(k), color):
            node_cpu = int(r.hget(k, 'cpu_free'))
            node_mem = int(r.hget(k, 'memory_free'))
            overall_free_cpu += node_cpu
            overall_free_memory += node_mem
            if cpu <= overall_free_cpu and mem <= overall_free_memory:
                return True
    return cpu <= overall_free_cpu and mem <= overall_free_memory

def find_applicable(cpu: int, mem: int, color: str) -> Optional[str]:
    # logging.info(f"Zrange {r.zrange(sorted_knapsacks_key, start=0, end=-1)}")
    for n in r.zrange(sorted_knapsacks_key, start=0, end=-1):
        # logging.info(f"Zrange {n}")
        if has_color_node(n, color) and can_allocate(n, cpu, mem):
            return n
    return None

def can_allocate(node: str, cpu: int, mem: int) -> bool:
    node_cpu = int(r.hget(knapsack_key(node), 'cpu_free'))
    node_mem = int(r.hget(knapsack_key(node), 'memory_free'))
    return node_cpu >= cpu and node_mem >= mem

def add_node(node: str) -> None:
    r.zadd(sorted_knapsacks_key, {node: calculate_capacity(node)})

def init(nodes: dict[str, dict[str, bool | int]]) -> None:
    for k, n in nodes.items():
        mapping = {}
        for a, b in n.items():
            if a == "cpu":
                mapping["cpu_cap"] = n["cpu"]
                mapping["cpu_free"] = n["cpu"]
            elif a == "memory":
                mapping["memory_cap"] = n["memory"]
                mapping["memory_free"] = n["memory"]
            else:
                mapping[a] = str(b)


        r.hset(knapsack_key(k), mapping=mapping)
        add_node(k)

def enqueue_item(queue_name: str, item: dict[str, Any]):
    """
    Enqueue an item to the queue.
    """
    logging.info(f"Enqueueing {json.dumps(item)}")
    r.rpush(queue_name, json.dumps(item))

def dequeue_item(queue_name: str, timeout=0):
    """
    Dequeue an item from the queue. Blocks until an item is available or the timeout is reached.
    Timeout of 0 means block indefinitely.
    """
    item = r.blpop(queue_name, timeout=timeout)
    if item:
        # item is a tuple (queue_name, value), so return the value
        return json.loads(item[1])
    return None


def handle_pod(task_id: str, node_name: str):
    try:
        logging.info(f"Releasing task {task_id} from {node_name}")
        release_task(node_name, task_id)
    except Exception as e:
        logging.warning(f"Handling pod failed {e}")

def push_temp_task(t: str, task: dict[str, Any]):
    r.rpush(temp_task_key(t), json.dumps(task))

def pop_temp_task(task: str) -> dict[str, Any]:
    item = r.lpop(temp_task_key(task))
    return json.loads(item)

def temp_len(task: str):
    return r.llen(temp_task_key(task))

def flush_temp(task: str):
    r.delete(temp_task_key(task))

def acquire_task_lock(task: str, token: str, timeout=None):
    """Attempt to acquire the mutex, blocking if necessary."""
    # Try to push the token onto the list only if it's empty (mutex is free)
    acquired = r.lpushx(task_mutex_key(task), token)
    if acquired:
        return True

    # If not acquired, block until able to pop the token (mutex is acquired)
    while True:
        _, t = r.blpop(task_mutex_key(task), timeout=timeout)
        if t == token:
            return True
        # Optional: handle timeout if token is None

def release_task_lock(task: str, token: str):
    """Release the mutex."""
    # Remove the token from the list, signaling the mutex is now free
    r.lrem(task_mutex_key(task), 1, token)

def acquire_node_lock(node: str, token: str, timeout=None):
    """Attempt to acquire the mutex, blocking if necessary."""
    # Try to push the token onto the list only if it's empty (mutex is free)
    acquired = r.lpushx(node_mutex_key(node), token)
    if acquired:
        return True

    # If not acquired, block until able to pop the token (mutex is acquired)
    while True:
        _, t = r.blpop(node_mutex_key(node), timeout=timeout)
        if t == token:
            return True
        # Optional: handle timeout if token is None

def release_node_lock(node: str, token: str):
    """Release the mutex."""
    # Remove the token from the list, signaling the mutex is now free
    r.lrem(node_mutex_key(node), 1, token)

def generate_token() -> str:
    return str(uuid.uuid4())