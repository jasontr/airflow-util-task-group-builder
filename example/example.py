from datetime import timedelta, datetime

from airflow import DAG

from task_group_builder import TaskGroupBuilder, task_builder
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator


class CustomTaskGroupBuilder(TaskGroupBuilder):
    def __init__(self, group_id, **kwargs):
        super().__init__(group_id, **kwargs)
        self.ordered_task_builders = [self.task_1, self.complex_task_builder, self.task_2]

    @task_builder
    def task_1(self, dag):
        return EmptyOperator(task_id=self.generate_task_id("task_1"), dag=dag)

    @task_builder
    def task_2(self, dag):
        return PythonOperator(
            task_id=self.generate_task_id("task_2"),
            python_callable=lambda: print("Hello, World!"),
            dag=dag,
        )

    def complex_task_builder(self, dag):
        task_a = EmptyOperator(task_id=self.generate_task_id("task_a"), dag=dag)
        task_b1 = EmptyOperator(task_id=self.generate_task_id("task_b1"), dag=dag)
        task_b2 = EmptyOperator(task_id=self.generate_task_id("task_b2"), dag=dag)
        task_c1 = EmptyOperator(task_id=self.generate_task_id("task_c1"), dag=dag)
        task_c2 = EmptyOperator(task_id=self.generate_task_id("task_c2"), dag=dag)
        task_d = EmptyOperator(task_id=self.generate_task_id("task_d"), dag=dag)

        # Define dependencies between tasks
        task_a >> task_b1 >> task_c1 >> task_d
        task_a >> task_b2 >> task_c2 >> task_d
        task_b1 >> task_c2
        task_b2 >> task_c1

        return task_a, task_d


dag = DAG(
    dag_id='task_group_builder_example',
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    schedule=timedelta(days=1),
    start_date=datetime.today() - timedelta(days=2)
)

CustomTaskGroupBuilder(group_id="")