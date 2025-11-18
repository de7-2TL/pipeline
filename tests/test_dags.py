import glob
import os
from unittest import mock

import pytest
from airflow.hooks.base import BaseHook
from airflow.models import Connection, DagBag
from cosmos import ProjectConfig

patcher = mock.patch("cosmos.ProjectConfig.validate_project", return_value=None)
patcher.start()

import dbt_dags  # noqa: E402

DAG_PATH = os.path.join(os.path.dirname(__file__), "../src/dags")
DAG_FILES = glob.glob(DAG_PATH, recursive=True)


@pytest.fixture
def dag_bag(mocker):
    mocker.patch.object(
        dbt_dags,
        "get_project_config",
        side_effect=lambda project_name: ProjectConfig(
            f"{DAG_PATH}/dbt/{project_name}"
        ),
    )

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

    patcher.stop()


def test_dag_loaded(dag_bag):
    assert not dag_bag.import_errors, f"DAG Import Errors: {dag_bag.import_errors}"
