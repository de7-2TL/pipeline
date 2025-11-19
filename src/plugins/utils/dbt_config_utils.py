from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping


def get_dbt_config_facade(project_name: str, conn_id: str = "snowflake_conn"):
    """
    project_config=get_project_config("project_name"),
    profile_config=get_profile("project_name", "conn_id"),
    execution_config=execution_config,
    TODO: 해당 Method 사용 시 Airflow DAG 인식이 안 됨
    :param project_name:
    :type project_name:
    :param conn_id:
    :type conn_id:
    :return: {
        "execution_config": execution_config,
        "project_config": project_config,
        "profile_config": profile_config,
    }
    :rtype:
    """
    execution_config = ExecutionConfig(
        dbt_executable_path="/home/airflow/.local/bin/dbt",
    )

    project_config = ProjectConfig(f"/opt/airflow/dags/dbt/{project_name}")

    profile_config = ProfileConfig(
        profile_name=project_name,
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id=conn_id,
        ),
    )

    return {
        "execution_config": execution_config,
        "project_config": project_config,
        "profile_config": profile_config,
    }


def get_execution_config() -> ExecutionConfig:
    return ExecutionConfig(
        dbt_executable_path="/home/airflow/.local/bin/dbt",
    )


def get_project_config(project_name: str) -> ProjectConfig:
    return ProjectConfig(f"/opt/airflow/dags/dbt/{project_name}")


def get_profile_config(project_name: str, conn_id: str = "snowflake_conn"):
    return ProfileConfig(
        profile_name=project_name,
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(
            conn_id=conn_id,
        ),
    )
