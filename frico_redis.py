import json
import os
from typing import Optional, Any
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

def knapsack_key(node: str) -> str:
    return knapsack_key_prefix.format(node=node)

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
    r.hset(task_key(task, node), mapping={'cpu': cpu, 'p': prio, 'mem': mem, 'c': color})
    decrease_capacity(node, cpu, mem)
    r.zadd(sorted_tasks_per_node_key(node), {task: calculate_obj(prio, node, cpu, mem)})
    update_node(node)

def release_task(node: str, task: str) -> None:
    key = task_key(task, node)
    cpu = int(r.hget(key, 'cpu'))
    mem = int(r.hget(key, 'mem'))
    r.delete(key)
    increase_capacity(node, cpu, mem)
    r.zrem(sorted_tasks_per_node_key(node), task)
    update_node(node)

def get_task(node: str, task: str) -> dict:
    return r.hgetall(task_key(task, node))

def get_nodes() -> list[str]:
    return r.zrange(sorted_knapsacks_key, start=0, end=-1, byscore=True)

def get_max() -> str:
    return r.zpopmax(sorted_knapsacks_key)

def get_node_colors(node: str) -> list[str]:
    return r.hget(knapsack_key(node), 'colors').split(',')

def has_color_node(node: str, color: str) -> bool:
    return r.hexists(knapsack_key(node), color)

def get_node_tasks(node: str) -> list[str]:
    return r.zrange(sorted_tasks_per_node_key(node), start=0, end=-1, byscore=True)

def number_of_nodes() -> int:
    return r.zcard(sorted_knapsacks_key)

def relocate_task(task_name: str, to_node: str) -> None:
    from_node = ''
    for k in r.scan_iter(match=f'meta:tasks:{task_name}:*'):
        from_node = k.split(':')[-1]
    task = r.hgetall(task_key(task_name, from_node))
    release_task(from_node, task_name)
    allocate_task(to_node, task_name, task['cpu'], task['mem'], task['p'], task['c'])

def is_admissable(cpu: int, mem: int, color: str) -> bool:
    overall_free_cpu = 0
    overall_free_memory = 0
    for k in r.scan_iter(match=f'meta:nodes:*'):
        colors = r.hget(knapsack_key(k), 'colors')
        if color in colors.split(','):
            cpu = int(r.hget(knapsack_key(k), 'cpu_free'))
            mem = int(r.hget(knapsack_key(k), 'memory_free'))
            overall_free_cpu += cpu
            overall_free_memory += mem
    return cpu <= overall_free_cpu and mem <= overall_free_memory

def find_applicable(cpu: int, mem: int, color: str) -> Optional[str]:
    for n in r.zrange(sorted_knapsacks_key, start=0, end=-1, byscore=True):
        colors = r.hget(knapsack_key(n), 'colors')
        if color in colors.split(',') and can_allocate(n, cpu, mem):
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
        r.zadd(sorted_knapsacks_key, {k: calculate_capacity(k)})

def enqueue_item(queue_name: str, item: dict[str, Any]):
    """
    Enqueue an item to the queue.
    """
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
        release_task(node_name, task_id)
        logging.info(f"Releasing task {task_id} from {node_name}")
    except Exception as e:
        logging.warning(f"Handling pod failed {e}")