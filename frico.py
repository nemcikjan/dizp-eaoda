import logging
from frico_redis import calculate_obj, get_node, allocate_task, has_color_node, release_task, can_allocate, find_applicable, get_nodes, get_task, get_node_colors, get_max, get_node_tasks, number_of_nodes, add_node, push_temp_task, pop_temp_task, flush_temp, temp_len
from typing import Optional

class Task(object):
    def __init__(self, id: str, name: str, cpu_requirement: int, memory_requirement: int, priority: int, color: str, arrival_time: int, exec_time: int):
        self.priority = priority
        self.name = name
        self.node_cpu_capacity = 0
        self.node_memory_capacity = 0
        self.arrival_time = arrival_time
        self.exec_time = exec_time
        self.cpu_requirement = cpu_requirement
        self.memory_requirement = memory_requirement
        self.color = color
        self.id = id
    
    def objective_value(self):
        return (self.priority / 5) * ((((self.node_cpu_capacity - self.cpu_requirement) / self.node_cpu_capacity) + ((self.node_memory_capacity - self.memory_requirement) / self.node_memory_capacity)) / 2)

class FRICO:
    realloc_threshold: int
    offloaded_tasks: int
    current_objective: int
    def __init__(self, realloc_threshold: int) -> None:
        self.realloc_threshold = realloc_threshold
        self.offloaded_tasks = 0
        self.current_objective = 0

    def get_current_objective(self):
        return self.current_objective

    def get_offloaded_tasks(self):
        return self.offloaded_tasks

    def solve(self, task: Task) -> tuple[str, dict[str, str]]:
        tasks_to_reschedule: dict[str, str] = {}
        suitable_node = self.frico_find_applicable(task)

        logging.info(f"Suitable {suitable_node}")

        if suitable_node is not None:
            allocate_task(suitable_node, task.id, task.cpu_requirement, task.memory_requirement, task.priority, task.color)
            return (suitable_node, tasks_to_reschedule)
        else:
            allocated = False
            choosen_node: Optional[str] = None
            # searched_knapsacks: list[str] = []
            for knapsack in get_nodes():
            # while number_of_nodes() > 0 and not choosen_node:
                # knapsack = get_max()
                if has_color_node(knapsack, task.color):
                    for t in get_node_tasks(knapsack):
                        t_task = get_task(knapsack, t)
                        logging.info(f"Suspect {t}:{knapsack} {t_task}")
                        for o_k in get_nodes():
                            try:
                                if o_k is not knapsack and has_color_node(o_k, t_task['c']) and can_allocate(knapsack, int(t_task['cpu']), int(t_task['mem'])):
                                    release_task(knapsack, t)
                                    allocate_task(o_k, t, int(t_task['cpu']), int(t_task['mem']), int(t_task['p']), t_task['c'])
                                    tasks_to_reschedule[t] = o_k
                                    break
                            except Exception as e:
                                logging.warning(f"Found random 'c' access {e}")
                                logging.info(f"Releasing {t} from {knapsack} due error")
                                release_task(knapsack, t)

                        applicable = self.frico_find_applicable(task)    
                        if (applicable is not None):
                            allocated = True
                            choosen_node = applicable
                            break            

                # searched_knapsacks.append(knapsack)

            # for n in searched_knapsacks:
            #     try:
            #         add_node(n)
            #     except Exception as e:
            #         logging.warning(f"Error while addig node {n}: {e}")

            if choosen_node is not None:
                self.frico_allocate_task(choosen_node, task)
                return (choosen_node, tasks_to_reschedule)
            elif allocated and choosen_node is None:
                raise Exception("Something gone wrong")
            else: 
                # tasks = SortedList()
                tasks: list[str] = []
                s_allocated = False
                allocated_node = ''
                # s_searched_knapsacks : list[str] = []
                for knapsack in get_nodes():
                # while number_of_nodes() > 0 and not s_allocated:
                    # knapsack = get_max()
                    # colors = get_node_colors(knapsack)
                    if has_color_node(knapsack, task.color):
                        tasks = []
                        flush_temp(task.name)
                        cummulative_cpu = 0
                        cummulatice_memory = 0
                        has_enough_space = False
                        k_n = get_node(knapsack)
                        logging.info(f"Got node {k_n}")
                        for t in get_node_tasks(knapsack):
                            try:
                                if calculate_obj(task.priority, knapsack, task.cpu_requirement, task.memory_requirement) <= self.calculate_potential_objective(task, int(k_n['cpu_cap']), int(k_n['memory_cap'])):
                                    t_t = get_task(knapsack, t)
                                    t_t["name"] = t
                                    cummulative_cpu += int(t_t['cpu'])
                                    cummulatice_memory += int(t_t['mem'])
                                    tasks.append(t)
                                    push_temp_task(task.name, t_t)
                                if cummulative_cpu >= task.cpu_requirement and cummulatice_memory >= task.memory_requirement:
                                    # there are already enough tasks to relax node N in favor of task T
                                    has_enough_space = True
                                    break
                                if len(tasks) == self.realloc_threshold:
                                    break
                            except Exception as e:
                                logging.warning(f"Error while trying to find space {e}")
                                logging.info(f"Releasing {t} from {knapsack} due error")
                                release_task(knapsack, t)
                        
                        if has_enough_space:
                            # here we know that all tasks in the list must be offloaded in order to relax node N for task T
                            for t in tasks:
                                release_task(knapsack, t)
                            self.frico_allocate_task(knapsack, task)
                            s_allocated = True
                            allocated_node = knapsack
                    # s_searched_knapsacks.append(knapsack)
                
                # for n in s_searched_knapsacks:
                #     add_node(n)

                if s_allocated:
                    while temp_len(task.name) > 0:
                    # for t in tasks:
                        # l_searched_knapsacks: list[str] = []
                        task_allocated = False
                        t = pop_temp_task(task.name)
                        logging.info(f"Phase 3: task {t}")
                        for knapsack in get_nodes():
                        # while number_of_nodes() > 0 and not task_allocated:
                            # knapsack = get_max()
                            logging.info(f"Phase 3: node {knapsack}")                        
                            try:
                                if has_color_node(knapsack, t['c']) and can_allocate(knapsack,int(t['cpu']), int(t['mem'])):
                                    allocate_task(knapsack, t['name'], int(t['cpu']), int(t['mem']), int(t['p']), t['c'])
                                    task_allocated = True
                                    tasks_to_reschedule[t['name']] = knapsack
                                    break
                            except Exception as e:
                                logging.warning(f"Error while allocating in last phase {e}")
                            
                            # l_searched_knapsacks.append(knapsack)
                        
                        if not task_allocated:
                            tasks_to_reschedule[t['name']] = None
                            self.offloaded_tasks += 1
                        
                        # for n in l_searched_knapsacks:
                        #     add_node(n) 
                return (allocated_node, tasks_to_reschedule)
    
    def calculate_potential_objective(self, task: Task, cpu_capacity: int, memory_capacity: int):
        return task.priority / ((((task.cpu_requirement / cpu_capacity) + (task.memory_requirement / memory_capacity)) / 2))
    
    def frico_find_applicable(self, task: Task) -> Optional[str]:
        return find_applicable(task.cpu_requirement, task.memory_requirement, task.color)
    
    def frico_allocate_task(self, node: str, task: Task):
        return allocate_task(node, task.id, task.cpu_requirement, task.memory_requirement, task.priority, task.color)
