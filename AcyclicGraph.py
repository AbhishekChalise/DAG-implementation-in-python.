class Task:
    def __init__(self, task_id, func):

        self.task_id = task_id
        self.func = func
        self.dependencies = []
        self.executed = False

    def add_dependencies(self, task):

        self.dependencies.append(task)

    def run_task(self):

        if not self.executed:
            for dependency in self.dependencies:
                dependency.run_task()
                print(f"Running tasks: {self.task_id}")
                self.func()
                self.executed = True


class DAG:

    def __init__(self, dag_name):
        self.dag_name = dag_name 
        self.tasks = {} 

    def add_tasks(self, task):
        self.tasks[task.task_id] = task

    def run(self, tasks_name):
        print(f"Starting DAG : {self.dag_name}")
        task = self.tasks.get(tasks_name)
        if task:
            task.run_task()
        else:
            print(f"Task {tasks_name} not found")


def extract_data_from_api():
    print("Extracting data from API...")

def extract_data_from_db():
    print("Extracting data from Database...")

def load_data():
    print("Loading data into Data Warehouse...")

def transform_data():
    print("Transforming and cleaning the data...")


task1 = Task("extract_data_api",extract_data_from_api)
task2 = Task("extract_data_db",extract_data_from_db)
task3 = Task("load_data", load_data)
task4 = Task("transform_data", transform_data)

task3.add_dependencies(task1)
task3.add_dependencies(task2)

task4.add_dependencies(task3)

# After creating task exceution create DAG

dag = DAG("etl_pipeline")

dag.add_tasks(task1)
dag.add_tasks(task2)
dag.add_tasks(task3)
dag.add_tasks(task4)


dag.run("transform_data")






