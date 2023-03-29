from datetime import timedelta, datetime
from typing import Tuple

import pytest
from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator

from task_group_builder import TaskGroupBuilder, task_builder, TaskGroupBuilderException


class TestTaskGroupBuilder(TaskGroupBuilder):
    def __init__(self, group_id, **kwargs):
        super().__init__(group_id, **kwargs)

        @task_builder
        def test1(dag):
            return EmptyOperator(task_id='_'.join((self.task_group_id, 'bash1')), dag=dag)

        @task_builder
        def test2(dag):
            return EmptyOperator(task_id='_'.join((self.task_group_id, 'bash2')), dag=dag)

        self.ordered_task_builders = [test1, test2]


test_dag = DAG(
    dag_id='test',
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

tb = TaskGroupBuilder('test')


def part_of_test(task_group_id):
    return TestTaskGroupBuilder(group_id=task_group_id).as_task_builder()


tb.ordered_task_builders = [
    [part_of_test('t1'), part_of_test('t2')],
    [part_of_test('t3'), part_of_test('t4')],
    [part_of_test('t5'), part_of_test('t6')],
]

tb.build(test_dag)


def create_a_dag(dag_id):
    return DAG(
        dag_id=dag_id,
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


def create_a_non_task_builder():
    def a_non_task_builder(_):
        return None, None

    return a_non_task_builder


def create_a_task_builder(task_id):
    @task_builder
    def a_task_builder(dag):
        return BaseOperator(task_id=task_id, dag=dag)

    return a_task_builder


def create_a_two_tasks_builder(start_task_id, end_task_id):
    def a_two_tasks_builder(dag):
        start_task = BaseOperator(task_id=start_task_id, dag=dag)
        end_task = BaseOperator(task_id=end_task_id, dag=dag)
        start_task >> end_task
        return start_task, end_task

    return a_two_tasks_builder


def test__flatten():
    test_case1 = [1, [2, 3], [4, [5, 6]]]
    expected1 = [1, 2, 3, 4, 5, 6]
    test_case2 = (1, (2, 3), (4, (5, '6')))
    expected2 = (1, 2, 3, 4, 5, '6')
    actual1 = list(TaskGroupBuilder._flatten(test_case1))
    actual2 = tuple(TaskGroupBuilder._flatten(test_case2))
    assert actual1 == expected1
    assert actual2 == expected2


def test__build_task_builder__multiple_task_builder():
    task_ids = [f'test_task{i}' for i in range(6)]
    task_start_ids = [f'test_task_start{i}' for i in range(3)]
    task_end_ids = [f'test_task_end{i}' for i in range(3)]
    single_task_builders = [create_a_task_builder(task_id=_id) for _id in task_ids]
    two_tasks_builders = [create_a_two_tasks_builder(start_id, end_id)
                          for start_id, end_id in zip(task_start_ids, task_end_ids)]
    task_builders = [
        single_task_builders[0], single_task_builders[1], single_task_builders[2],
        [
            single_task_builders[3], single_task_builders[4],
            [
                single_task_builders[5], two_tasks_builders[0]
            ],
        ],
        [
            two_tasks_builders[1], two_tasks_builders[2]
        ]
    ]
    expected_task_start_ids = task_ids + task_start_ids
    expected_task_end_ids = task_ids + task_end_ids

    actual_start, actual_end = TaskGroupBuilder._build_task_builder(task_builders, create_a_dag('test_multi'))
    assert [task.task_id for task in actual_start] == expected_task_start_ids
    assert [task.task_id for task in actual_end] == expected_task_end_ids


def test__build_task_builder__single_task_builder():
    def test_case(_task_builder, start_task_id, end_task_id, dag=None):
        actual_start, actual_end = TaskGroupBuilder._build_task_builder(_task_builder, dag)
        assert actual_start.task_id == start_task_id
        assert actual_end.task_id == end_task_id

    expected_task_id = 'test_task'
    test_task_builder = create_a_task_builder(expected_task_id)
    test_case(test_task_builder, expected_task_id, expected_task_id)

    expected_start_task_id = 'test_task_start'
    expected_end_task_id = 'test_task_end'
    test_two_tasks_builder = create_a_two_tasks_builder(expected_start_task_id, expected_end_task_id)
    test_case(test_two_tasks_builder, expected_start_task_id, expected_end_task_id, dag=create_a_dag('test_two_tasks'))


def test__build_task_builder__no_task_builder():
    actual_start, actual_end = TaskGroupBuilder._build_task_builder([], None)
    assert actual_start is None
    assert actual_end is None


def test__build_task_builder__invalid_task_builder():
    expected_task_id = 'test_task'

    def invalid_task_builder(dag):
        task = BaseOperator(task_id=expected_task_id, dag=dag)
        return task, None

    def test_template(_task_builder):
        with pytest.raises(TaskGroupBuilderException) as info:
            _, _ = TaskGroupBuilder._build_task_builder(_task_builder, None)
        exception_msg = info.value.args[0]
        assert exception_msg == f'Task builder {invalid_task_builder.__name__} is invalid.'

    test_template(invalid_task_builder)
    test_template([invalid_task_builder])
    test_template([[], invalid_task_builder])


def test_as_task_builder():
    dag_id = 'dag_test_tgb'
    group_id = 'test_tgb'

    def test_case(ordered_task_builders):
        task_group_builder = TaskGroupBuilder(group_id)
        task_group_builder.ordered_task_builders = ordered_task_builders
        return task_group_builder.as_task_builder()(create_a_dag(dag_id))

    start, end = test_case([])
    assert start is None and end is None

    a_ordered_task_builders = [create_a_non_task_builder(), [create_a_non_task_builder(), create_a_non_task_builder()]]
    start, end = test_case(a_ordered_task_builders)
    assert start is None and end is None

    task_ids = [f'task{i}' for i in range(4)]
    a_ordered_task_builders = [create_a_non_task_builder(),
                               [
                                   create_a_task_builder(task_ids[0]),
                                   create_a_two_tasks_builder(start_task_id=task_ids[1], end_task_id=task_ids[2]),
                                   create_a_non_task_builder()
                               ],
                               create_a_task_builder(task_id=task_ids[3])
                               ]
    start, end = test_case(a_ordered_task_builders)
    task_ids = [f'{group_id}.{tid}' for tid in task_ids]
    assert {task.task_id for task in start} == {task_ids[0], task_ids[1]}
    dag = start[0].dag
    task0 = dag.get_task(task_ids[0])
    task1 = dag.get_task(task_ids[1])
    assert task1.downstream_task_ids == {task_ids[2]}
    task2 = dag.get_task(task_ids[2])
    assert task0.downstream_task_ids == {task_ids[3]}
    assert task2.downstream_task_ids == {task_ids[3]}
    assert task0.downstream_list == [end]
    for tid in task_ids:
        task: BaseOperator = dag.get_task(tid)
        assert task.task_group.group_id == group_id


def test_build__no_empty_operator_start_end():
    group_id = 'test_build1'
    dag_id = 'dag_test_build1'
    dag = create_a_dag(dag_id)
    task_ids = [f'task{i}' for i in range(3)]
    task_group_builder = TaskGroupBuilder(group_id)
    task_group_builder.ordered_task_builders = [[create_a_two_tasks_builder(task_ids[0], task_ids[1]),
                                                 create_a_task_builder(task_ids[2])]]
    task_ids = [f'{group_id}.{tid}' for tid in task_ids]
    start, end = task_group_builder.build(dag)
    assert dag.roots == [dag.get_task(task_ids[0]), dag.get_task(task_ids[2])]
    assert dag.leaves == [dag.get_task(task_ids[1]), dag.get_task(task_ids[2])]
    assert dag.roots == start
    assert dag.leaves == end


def test_build__create_empty_operator_start_end():
    group_id = 'test_build1'
    dag_id = 'dag_test_build1'
    dag = create_a_dag(dag_id)
    expected_start_task_id = f'{group_id}_start'
    expected_end_task_id = f'{group_id}_end'
    task_ids = [f'task{i}' for i in range(3)]
    task_group_builder = TaskGroupBuilder(group_id)
    task_group_builder.ordered_task_builders = [[create_a_two_tasks_builder(task_ids[0], task_ids[1]),
                                                 create_a_task_builder(task_ids[2])]]
    start, end = task_group_builder.build(dag, create_empty_start_end_task=True)
    assert dag.roots == [start]
    assert dag.leaves == [end]
    assert start.task_id == expected_start_task_id
    assert end.task_id == expected_end_task_id


def test_build__no_task_include_raise_error():
    group_id = 'test_build1'
    dag_id = 'dag_test_build1'
    dag = create_a_dag(dag_id)
    task_group_builder = TaskGroupBuilder(group_id)
    with pytest.raises(TaskGroupBuilderException) as info:
        _, _ = task_group_builder.build(dag, create_empty_start_end_task=True)
        exception_msg = info.value.args[0]
        assert exception_msg == 'No task in this task group builder.'


@pytest.fixture
def task_group_builder():
    return TaskGroupBuilder(group_id="example_group")


@pytest.fixture
def task_group_builder_empty_group_id():
    return TaskGroupBuilder(group_id="")


def test_generate_task_id(task_group_builder, task_group_builder_empty_group_id):
    # Test case without providing a connector
    generated_name = task_group_builder.generate_task_id("task_id")
    assert generated_name == "example_group_task_id"

    # Test case with a custom connector
    generated_name = task_group_builder.generate_task_id("task_id", connector="-")
    assert generated_name == "example_group-task_id"

    # Test case with an empty connector
    generated_name = task_group_builder.generate_task_id("task_id", connector="")
    assert generated_name == "example_grouptask_id"

    # Test case with an empty group_id
    generated_name = task_group_builder_empty_group_id.generate_task_id("task_id")
    assert generated_name == "task_id"


@pytest.fixture(scope="function")
def example_dag():
    return DAG(
        dag_id="example_dag",
        default_args={"owner": "airflow", "retries": 0},
        start_date=datetime(2023, 1, 1),
        end_date=datetime(2023, 1, 2),
        catchup=False,
    )


def test_build_task_sequence(example_dag):
    task_group_builder = TaskGroupBuilder("test_group")

    @task_builder
    def task_1(dag):
        return EmptyOperator(task_id="task_1", dag=dag)

    @task_builder
    def task_2(dag):
        return EmptyOperator(task_id="task_2", dag=dag)

    task_group_builder.ordered_task_builders = [task_1, task_2]

    built_task_pairs = filter(
        lambda pair: pair[0] is not None,
        map(lambda tb: task_group_builder._build_task_builder(tb, example_dag), task_group_builder.ordered_task_builders),
    )

    start, end = task_group_builder._build_task_sequence(built_task_pairs, example_dag)

    assert start.task_id == "task_1"
    assert end.task_id == "task_2"


@pytest.mark.parametrize("use_task_group", [True, False])
def test_as_task_builder(example_dag, use_task_group):
    task_group_builder = TaskGroupBuilder("test_group")

    @task_builder
    def task_1(dag):
        return EmptyOperator(task_id="task_1", dag=dag)

    @task_builder
    def task_2(dag):
        return EmptyOperator(task_id="task_2", dag=dag)

    task_group_builder.ordered_task_builders = [task_1, task_2]

    task_builder_func = task_group_builder.as_task_builder(use_task_group=use_task_group)
    start, end = task_builder_func(example_dag)

    if use_task_group:
        assert start.task_id == "test_group.task_1"
        assert end.task_id == "test_group.task_2"
        assert start.task_group.group_id == "test_group"
        assert end.task_group.group_id == "test_group"
    else:
        assert start.task_id == "task_1"
        assert end.task_id == "task_2"
        assert start.task_group.group_id is None
        assert end.task_group.group_id is None
