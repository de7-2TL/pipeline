from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",
)


project_config = ProjectConfig("/opt/airflow/dags/dbt/stock_sector")


profile_config = ProfileConfig(
    profile_name="stock_sector",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
    ),
)

dbt_max_sector_high_dag = DbtDag(
    execution_config=execution_config,
    project_config=project_config,
    profile_config=profile_config,
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 1),
    catchup=False,
    dag_id="dbt_max_sector_high",
    tags=["stock", "dbt", "sector"],
    render_config=RenderConfig(
        select=["+max_sector_high"],
    ),
)