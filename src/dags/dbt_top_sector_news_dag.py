
import pendulum
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

execution_config = ExecutionConfig(
    dbt_executable_path="dbt",
)


project_config = ProjectConfig("/opt/airflow/dags/dbt/news")


profile_config = ProfileConfig(
    profile_name="news",
    target_name="dev",
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id="snowflake_conn",
    ),
)


dbt_max_sector_high_dag = DbtDag(
    project_config=project_config,
    profile_config=profile_config,
    execution_config=execution_config,
    schedule_interval="@daily",
    start_date=pendulum.datetime(2025, 11, 1),
    catchup=False,
    dag_id="dbt_max_sector_high",
    render_config=RenderConfig(
        select=["+top_sector_company_news", "+top_sector", "+top_sector_news"],
    ),
)
