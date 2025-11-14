import os

import pytest
from airflow.models import DagBag

DAG_PATH = os.path.join(os.path.dirname(__file__), "../src/dags")


@pytest.fixture
def dag_bag():
    return DagBag(dag_folder=DAG_PATH, include_examples=False)


@pytest.fixture
def hello_world_dag(dag_bag):
    return dag_bag.get_dag("hello_world")

def test_dag_loaded():
    from dags.hello_world import return_name
    expected_name = "John Doe"

    assert  return_name(expected_name) == expected_name

def test_bash_operator_command(hello_world_dag):
    """
    Operator가 올바르게 설정되었는지 확인
    """

    print_hello_task = hello_world_dag.get_task("print_hello")

    assert print_hello_task.operator_name == "PythonOperator"

def test_task_dependencies(hello_world_dag):
    """
    Task 간의 업스트림/다운스트림 관계가 올바른지 확인
    """
    process_task = hello_world_dag.get_task("print_hello")

    assert len(process_task.upstream_task_ids) == 0

    assert "print_hello2" in process_task.downstream_task_ids


def test_task_count(hello_world_dag):
    """
    DAG에 정의된 총 Task의 개수가 맞는지 확인
    """
    assert len(hello_world_dag.tasks) == 2

def test_tasks_exist(hello_world_dag):
    """
    필수 Task ID들이 DAG에 존재하는지 확인
    """
    expected_task_ids = ["print_hello", "print_hello2"]

    # task_ids는 list 자료형
    for task_id in expected_task_ids:
        assert task_id in hello_world_dag.task_ids

