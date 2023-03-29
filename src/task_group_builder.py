from functools import partial, wraps
from typing import Iterable, NewType, Callable, Tuple, Optional, List, Any, Sequence, Union

from airflow import DAG
from airflow.models import BaseOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

TaskAtStart = NewType('TaskAtStart', Optional[BaseOperator])
TasksAtStart = NewType('TasksAtStart', Union[TaskAtStart, Iterable[TaskAtStart]])
TaskAtEnd = NewType('TaskAtEnd', Optional[BaseOperator])
TasksAtEnd = NewType('TasksAtEnd', Union[TaskAtEnd, Iterable[TaskAtEnd]])
TaskBuilder = NewType('TaskBuilder', Callable[[DAG], Tuple[TaskAtStart, TaskAtEnd]])


def task_builder(func):
    @wraps(func)
    def _task_builder(dag):
        task = func(dag)
        return task, task

    return _task_builder


class TaskGroupBuilder:
    def __init__(self, group_id, *, config=None, **kwargs):
        self.config = config
        self.task_group_id = group_id
        self.__dict__.update(kwargs)
        self.ordered_task_builders: List[TaskBuilder | List[TaskBuilder]] = []

    @staticmethod
    def _flatten(iterable: Iterable) -> Iterable:
        for item in iterable:
            if isinstance(item, Iterable) and not isinstance(item, (bytes, str)):
                yield from TaskGroupBuilder._flatten(item)
            else:
                yield item

    @staticmethod
    def _build_task_builder(task_builders: Union[TaskBuilder, Iterable[TaskBuilder]],
                            dag: DAG) -> Tuple[TasksAtStart, TasksAtEnd]:
        if isinstance(task_builders, Callable):
            start, end = task_builders(dag)
            if None in (start, end) and start != end:
                raise TaskGroupBuilderException(f'Task builder {task_builders.__name__} is invalid.')
            return start, end

        start, end = [], []
        for builder in task_builders:
            next_start, next_end = TaskGroupBuilder._build_task_builder(builder, dag)
            if next_start and next_end:
                start.append(next_start)
                end.append(next_end)
        start = list(TaskGroupBuilder._flatten(start))
        end = list(TaskGroupBuilder._flatten(end))
        return start or None, end or None

    def as_task_builder(self) -> TaskBuilder:
        def builder(dag: DAG) -> Tuple[TaskAtStart, TaskAtEnd]:
            task_start_end_pair_builder = partial(self._build_task_builder, dag=dag)
            built_task_pairs = filter(lambda pair: pair[0] is not None,
                                      map(task_start_end_pair_builder, self.ordered_task_builders))
            with TaskGroup(group_id=self.task_group_id, dag=dag, prefix_group_id=True):
                try:
                    start, current_end = next(built_task_pairs)
                except StopIteration:
                    return None, None

                for next_start, next_end in built_task_pairs:
                    if self._both_sequence(current_end, next_start):
                        connector = EmptyOperator(task_id=self.connector_name(next_start), dag=dag)
                        current_end >> connector >> next_start
                    else:
                        current_end >> next_start
                    current_end = next_end
            assert (not start) ^ (not current_end) is False
            return start, current_end

        return builder

    def connector_name(self, tasks: Sequence[TaskAtStart]):
        first_task_id = sorted(task.task_id for task in tasks)[0]
        return '_'.join([self.task_group_id, 'connector', str(hash(first_task_id))[:8]])

    @staticmethod
    def _both_sequence(one: Any, another: Any) -> bool:
        return isinstance(one, Sequence) and isinstance(another, Sequence)

    def build(self, dag: DAG, create_empty_start_end_task=False) -> Tuple[TasksAtStart, TasksAtEnd]:
        start, end = self.as_task_builder()(dag)
        if start is None or end is None:
            raise TaskGroupBuilderException('No task in this task group builder.')
        if create_empty_start_end_task:
            _start, _end = start, end
            start = EmptyOperator(task_id=f'{self.task_group_id}_start', dag=dag)
            end = EmptyOperator(task_id=f'{self.task_group_id}_end', dag=dag)
            start >> _start
            end << _end
        return start, end

    def generate_task_id(self, task_id, connector='_'):
        builder = []
        if self.task_group_id:
            builder.append(self.task_group_id)
        builder.append(task_id)
        return connector.join(builder)


class TaskGroupBuilderException(Exception):
    ...
