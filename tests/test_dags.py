import glob
import os
import pytest
from airflow.models import DagBag

DAG_PATH = os.path.join(os.path.dirname(__file__), "../src/dags")
DAG_FILE_PATH = os.path.join(DAG_PATH, "hello_world.py")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)

@pytest.fixture
def dag_bag():
    return DagBag(dag_folder=DAG_PATH, include_examples=False)


def test_dag_loaded(dag_bag):

    assert not dag_bag.import_errors, f"DAG Import Errors: {dag_bag.import_errors}"

def test_dag_id_exists_in_dagbag(dag_bag):
    """
    예상하는 DAG ID가 DagBag에 존재하는지 확인
    """
    expected_dag_id = "hello_world" # 실제 DAG 파일에 정의된 ID

    dag = dag_bag.get_dag(expected_dag_id)

    assert dag is not None, f"DAG '{expected_dag_id}' not found in DagBag."
    assert dag.dag_id == expected_dag_id
    print(dag.tasks)

def test_dag_default_args(dag_bag):

    """
    DAG의 default_args가 올바르게 설정되었는지 확인
    """
    my_dag = dag_bag.get_dag("hello_world")
    assert my_dag.default_args['owner'] == 'data_team'
    assert my_dag.default_args.get('retries') == 3

def test_dag_schedule_and_tags(dag_bag):
    """
    스케줄 간격, 태그 등이 올바르게 설정되었는지 확인
    """
    my_dag = dag_bag.get_dag("hello_world")
    assert my_dag.schedule_interval == "0 12 * * *"
    assert "production" in my_dag.tags
    assert not my_dag.catchup


