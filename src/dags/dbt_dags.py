from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)


def get_project_config(project_name: str) -> ProjectConfig:
    return ProjectConfig(f"opt/airflow/dags/dbt/{project_name}")


def get_profile(project_name: str, conn_id: str = "snowflake_conn"):
    return ProfileConfig(
        profile_name=project_name,
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id=conn_id,
        ),
    )


dbt_demo_dag = DbtDag(
    project_config=get_project_config("news"),
    profile_config=get_profile("news", "snowflake_conn"),
    execution_config=execution_config,
    schedule_interval="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="news_dbt_demo_dag",
)
