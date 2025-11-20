import glob
import os

import pytest
from airflow.hooks.base import BaseHook
from airflow.models import Connection, DagBag

DAG_PATH = os.path.join(os.path.dirname(__file__), "../src/dags")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)


@pytest.fixture
def dag_bag(mocker):
    mocker.patch.object(
        BaseHook,
        "get_conn",
        return_value=(
            Connection(
                conn_id="test_conn", login="fake_access_key", password="fake_secret"
            )
        ),
    )

    yield DagBag(dag_folder=DAG_PATH, include_examples=False)


def test_dag_loaded(dag_bag):
    assert not dag_bag.import_errors, f"DAG Import Errors: {dag_bag.import_errors}"