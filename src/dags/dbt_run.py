from datetime import datetime
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping

# DBT 실행 경로 설정
execution_config = ExecutionConfig(
    dbt_executable_path="/home/airflow/.local/bin/dbt",  # DBT 실행 경로
)

# 프로젝트와 프로파일 설정
def get_project_config(project_name: str) -> ProjectConfig:
    return ProjectConfig(f"/opt/airflow/dags/dbt/{project_name}")

def get_profile(project_name: str, conn_id: str = "snowflake_conn"):
    return ProfileConfig(
        profile_name=project_name,
        target_name="dev",
        profile_mapping=SnowflakeUserPasswordProfileMapping(  # Snowflake 연결 설정
            conn_id=conn_id,
        ),
    )

# Cosmos DbtDag 정의 (DBT 모델을 실행)
dbt_demo_dag = DbtDag(
    project_config=get_project_config("news"),
    profile_config=get_profile("news", "snowflake_conn"),
    execution_config=execution_config,
    schedule_interval="@daily",  # 매일 실행
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dag_id="news_dbt",
)

# 모델 실행 순서 정의
# Cosmos는 `ref()`를 사용하여 모델 간의 종속성을 자동으로 처리합니다.
# 예를 들어, `stg_company_detail` 모델은 `company_info`, `industry_info`, `sector_info`를 참조하기 때문에,
# Cosmos는 이 모델들이 먼저 실행되도록 처리합니다.
