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

def temp_task_key(task: str = '') -> str:
    return temp_tasks_key_prefix.format(task=task)

def task_key(node: str, task: str = '') -> str:
    return task_key_prefix.format(task=task, node=node)

def sorted_tasks_per_node_key(node: str) -> str:
    return sorted_tasks_per_node_key_prefix.format(node=node)

def generate_token() -> str:
    return str(uuid.uuid4())