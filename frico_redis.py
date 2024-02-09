import json
import os
from typing import Optional, Any
from pottery import Redlock
import redis
import logging
import utils

REDIS_HOST = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")
REDIS_PASSWORD = os.environ.get("REDIS_PASSWORD", "")

queues = {
    "TASKS" : "tasks_queue",
    "CREATE": "tasks_to_create",
    "DELETE": "tasks_to_delete",
    "RESCHEDULE": "tasks_to_reschedule"
}

class RedisClient():
    def __init__(self) -> None:
        self.redis = redis.Redis(host=REDIS_HOST, port=int(REDIS_PORT),password=REDIS_PASSWORD, decode_responses=True)
        self.redis.execute_command('CLIENT TRACKING ON')

    def get_redis(self):
        return self.redis
    
class FricoRedis(RedisClient):
    def __init__(self) -> None:
        super().__init__()
    
    def calculate_capacity(self, node: str) -> float:
        d = self.redis.hgetall(utils.knapsack_key(node))
        return (((int(d['cpu_cap']) - int(d['cpu_free'])) / int(d['cpu_cap'])) + ((int(d['memory_cap']) - int(d['memory_free'])) / int(d['memory_cap']))) / 2

    def calculate_obj(self, prio: int, node: str, cpu: int, mem: int) -> float:
        d = self.redis.hgetall(utils.knapsack_key(node))
        return (prio / 5) * (((( int(d['cpu_cap']) - cpu) / int(d['cpu_cap'])) + ((int(d['memory_cap']) - mem) / int(d['memory_cap']))) / 2)

    def get_node(self, node: str) -> dict:
        return self.redis.hgetall(utils.knapsack_key(node))
    
class FricoMutex(RedisClient):
    def __init__(self) -> None:
        self.token = utils.generate_token()
        super().__init__()

    def acquire_task_lock(self, task: str, timeout=None):
        """Attempt to acquire the mutex, blocking if necessary."""
        # Try to push the token onto the list only if it's empty (mutex is free)
        acquired = r.lpushx(utils.task_mutex_key(task), self.token)
        if acquired:
            return True

        # If not acquired, block until able to pop the token (mutex is acquired)
        while True:
            _, t = r.blpop(utils.task_mutex_key(task), timeout=timeout)
            if t == self.token:
                return True
            # Optional: handle timeout if token is None

    def release_task_lock(self, task: str):
        """Release the mutex."""
        # Remove the token from the list, signaling the mutex is now free
        r.lrem(utils.task_mutex_key(task), 1, self.token)

    def acquire_node_lock(self, node: str, timeout=None):
        """Attempt to acquire the mutex, blocking if necessary."""
        # Try to push the token onto the list only if it's empty (mutex is free)
        acquired = r.lpushx(utils.node_mutex_key(node), self.token)
        if acquired:
            return True

        # If not acquired, block until able to pop the token (mutex is acquired)
        while True:
            _, t = r.blpop(utils.node_mutex_key(node), timeout=timeout)
            if t == self.token:
                return True
            # Optional: handle timeout if token is None
    def release_node_lock(self, node: str):
        """Release the mutex."""
        # Remove the token from the list, signaling the mutex is now free
        r.lrem(utils.node_mutex_key(node), 1, self.token)

r_i = RedisClient()
r = r_i.get_redis()

def calculate_capacity(node: str) -> float:
    with acquire_lock(utils.knapsack_key(node)):
        d = r.hgetall(utils.knapsack_key(node))
        return (((int(d['cpu_cap']) - int(d['cpu_free'])) / int(d['cpu_cap'])) + ((int(d['memory_cap']) - int(d['memory_free'])) / int(d['memory_cap']))) / 2

def calculate_obj(prio: int, node: str, cpu: int, mem: int) -> float:
    d = r.hgetall(utils.knapsack_key(node))
    return (prio / 5) * (((( int(d['cpu_cap']) - cpu) / int(d['cpu_cap'])) + ((int(d['memory_cap']) - mem) / int(d['memory_cap']))) / 2)

def get_node(node: str) -> dict:
    return r.hgetall(utils.knapsack_key(node))

def acquire_lock(key: str):
    return Redlock(key=key, masters={r}, auto_release_time=.5)

def release_lock(rl: Redlock):
    return rl.release()

def update_node(node: str) -> None:
    with acquire_lock(utils.sorted_knapsacks_key):
        r.zadd(utils.sorted_knapsacks_key, {node: calculate_capacity(node)}, xx=True)

def increase_capacity(node: str, cpu: int, mem: int) -> None:
    key = utils.knapsack_key(node)
    with acquire_lock(key):
        r.hincrby(key, "cpu_free", cpu)
        r.hincrby(key, "memory_free", mem)
    update_node(node)

def decrease_capacity(node: str, cpu: int, mem: int) -> None:
    key = utils.knapsack_key(node)
    with acquire_lock(key):
        r.hincrby(key, "cpu_free", -cpu)
        r.hincrby(key, "memory_free", -mem)
    update_node(node)

def allocate_task(node: str, task: str, cpu: int, mem: int, prio: int, color: int) -> None:
    logging.info(f"Allocation of task {task} to node {node}")
    try:
        with acquire_lock(utils.task_key(node, task)):
            r.hset(utils.task_key(node, task), mapping={'cpu': cpu, 'p': prio, 'mem': mem, 'c': color})
        decrease_capacity(node, cpu, mem)
        with acquire_lock(utils.sorted_tasks_per_node_key(node)):
            r.zadd(utils.sorted_tasks_per_node_key(node), {task: calculate_obj(prio, node, cpu, mem)})
    except Exception as e:
        logging.warning(f"Exception occured while allocating {e}")

def release_task(node: str, task: str) -> None:
    key = utils.task_key(node,task)
    cpu = 0
    mem = 0
    with acquire_lock(key):
        try:
            cpu = int(r.hget(key, 'cpu'))
            mem = int(r.hget(key, 'mem'))
        except Exception as e:
            logging.warning(f"Unable to get task {key} metadata: {e}")
        try:
            r.delete(key)
        except Exception as e:
            logging.warning(f"Unable to delete key {key}: {e}")

    increase_capacity(node, cpu, mem)
    try:
        with acquire_lock(utils.sorted_tasks_per_node_key(node)):
            r.zrem(utils.sorted_tasks_per_node_key(node), task)
    except Exception as e:
        logging.warning(f"Unable to remove task {task} from sorted set on node {node}: {e}")

def get_task(node: str, task: str) -> dict:
    return r.hgetall(utils.task_key(node, task))

def get_nodes() -> list[str]:
    return r.zrange(utils.sorted_knapsacks_key, start=0, end=-1)

def get_max() -> str:
    return r.zpopmax(utils.sorted_knapsacks_key)[0]

def get_node_colors(node: str) -> list[str]:
    return r.hget(utils.knapsack_key(node), 'colors').split(',')

def has_color_node(node: str, color: str) -> bool:
    return r.hexists(utils.knapsack_key(node), color)

def get_node_tasks(node: str) -> list[str]:
    return r.zrevrange(utils.sorted_tasks_per_node_key(node), start=0, end=-1)

def number_of_nodes() -> int:
    return r.zcard(utils.sorted_knapsacks_key)

def relocate_task(task_name: str, to_node: str) -> None:
    from_node = ''
    for k in r.scan_iter(match=f'meta:tasks:{task_name}:*'):
        from_node = k.split(':')[-1]
    task = r.hgetall(utils.task_key(from_node, task_name))
    release_task(from_node, task_name)
    allocate_task(to_node, task_name, task['cpu'], task['mem'], task['p'], task['c'])

def get_all_tasks():
    return r.scan_iter(match=f'meta:tasks:*')
        
def exists(key: str):
    return r.exists(key) > 0

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
    logging.debug(f"Zrange {r.zrange(utils.sorted_knapsacks_key, start=0, end=-1)}")
    with acquire_lock(utils.sorted_knapsacks_key):
        for n in r.zrange(utils.sorted_knapsacks_key, start=0, end=-1):
            logging.debug(f"Zrange {n}")
            if has_color_node(n, color) and can_allocate(n, cpu, mem):
                return n
    return None

def can_allocate(node: str, cpu: int, mem: int) -> bool:
    key = utils.knapsack_key(node)
    with acquire_lock(key):
        node_cpu = int(r.hget(utils.knapsack_key(node), 'cpu_free'))
        node_mem = int(r.hget(utils.knapsack_key(node), 'memory_free'))
        return node_cpu >= cpu and node_mem >= mem

def add_node(node: str) -> None:
    r.zadd(utils.sorted_knapsacks_key, {node: calculate_capacity(node)})

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

        r.hset(utils.knapsack_key(k), mapping=mapping)
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
        logging.warning(f"Handling pod {task_id} failed {e}")
        raise e

def push_temp_task(t: str, task: dict[str, Any]):
    r.rpush(utils.temp_task_key(t), json.dumps(task))

def pop_temp_task(task: str) -> dict[str, Any]:
    item = r.lpop(utils.temp_task_key(task))
    return json.loads(item)

def temp_len(task: str):
    return r.llen(utils.temp_task_key(task))

def flush_temp(task: str):
    r.delete(utils.temp_task_key(task))

def remove_from_queue(queue: str, value):
    r.lrem(queue, 1, json.dumps(value))