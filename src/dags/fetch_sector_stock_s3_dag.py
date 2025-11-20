import logging
from datetime import UTC, datetime, timedelta
from io import BytesIO

import yfinance as yf
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

log = logging.getLogger(__name__)

SECTOR_LIST = ["basic-materials", "communication-services", 
            "consumer-cyclical", "consumer-defensive",
            "energy", "financial-services", "healthcare", 
            "industrials", "real-estate", "technology", "utilities"]


S3_BUCKET = "de07-project03"
S3_PREFIX = "stock"
AWS_CONN_ID = "aws_conn"
TARGET_FOLDER_NAME = 'Sector'

# 목적한 날짜와 시간의 데이터가 없을 때 발생시킬 오류
class DataNotFoundException(AirflowException):
    pass

# S3 조회에 실패할 경우 발생시킬 오류
class S3CheckError(AirflowException):
    pass

# s3의 stock폴더 아래 폴더가 있는지 확인하는 함수.
# 폴더가 있다면 첫 적재가 아니므로 한 달간의 stock 지수를 전부 가져오고 아닐 경우엔 새로운 레코드만 적재하기 위함.
def check_deep_subfolder_existence(bucket_name, base_path, target_folder_name, aws_conn_id, **context):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    # base_path가 슬래시로 끝나도록 보장
    if not base_path.endswith('/'):
        base_path += '/'
    
    # 찾을 폴더 이름 뒤에 슬래시를 붙여 프리픽스 패턴을 만듭니다.
    target_pattern = f"{target_folder_name}/"
    found_paths = []
    current_time = datetime.now(UTC)

    log.info(f"S3 버킷 '{bucket_name}'의 경로 '{base_path}' 아래에서")
    log.info(f"임의의 깊이에 '{target_pattern}' 폴더 패턴 존재 확인 중...")

    try:
        all_keys = s3_hook.list_keys(
            bucket_name=bucket_name,
            prefix=base_path # base_path 아래 모든 객체 탐색
        )
        
        if all_keys is None: # list_keys가 None을 반환할 수 있는 경우 처리
            log.info(f"'{base_path}' 아래에 객체가 없습니다.")
            return {'exists': False, 'found_paths': []}

        potential_folders = set()

        for key in all_keys:
            if target_pattern in key:
                idx = key.find(target_pattern)
                if idx != -1: # 패턴이 발견된 경우
                    full_folder_path = key[:idx + len(target_pattern)]
                    potential_folders.add(full_folder_path)

        if potential_folders:
            found_paths = list(potential_folders)
            log.info(f"'{base_path}' 아래에서 '{target_folder_name}' 폴더 패턴이 발견되었습니다.")
            return {'exists': True, 'found_paths': found_paths, 'current_time':current_time.isoformat()}
        else:
            log.info(f"'{base_path}' 아래에서 '{target_folder_name}' 폴더 패턴을 찾을 수 없습니다.")
            return {'exists': False, 'found_paths': [], 'current_time':current_time.isoformat()}

    except Exception as e:
        log.error(f"S3 버킷 조회 중 오류가 발생했습니다: {e}")
        raise S3CheckError(f"S3 폴더 확인 중 알 수 없는 오류 발생: {e}") from e

def _fetch_and_upload_sector_data_from_df_api(s3_bucket, s3_base_prefix, sector_name, aws_conn_id, **kwargs) -> None:
    """
    주어진 섹터 이름으로 API를 호출하고 (결과가 DataFrame),
    그 DataFrame을 Parquet 파일로 S3에 적재합니다.
    폴더 구조 확인 결과를 XCom으로부터 받아 S3 Key 생성에 활용합니다.
    """

    # 이전 태스크(check_subfolders_task)의 XCom 값 가져오기
    check_result = kwargs['ti'].xcom_pull(task_ids='check_s3_folder')
    current_time = check_result.get('current_time')
    current_time = datetime.fromisoformat(current_time)

    try:
        # s3에 하위 폴더가 존재할 경우
        if check_result and check_result.get('exists'):
            price_df = yf.Sector(sector_name).ticker.history(period='1d', interval='15m')
            price_df['Sector'] = sector_name
            target_time = current_time.astimezone(price_df.tail(1).index[0].tz) - timedelta(minutes=19)

            # 목표로 하는 데이터만 남김
            price_df = price_df.loc[price_df.index.strftime('%Y-%m-%d %H-%M') == target_time.strftime('%Y-%m-%d %H-%M')]

            if price_df.empty:
                log.warning(f"[{sector_name}] Fetched DataFrame is empty. Skipping Parquet upload.")
                raise DataNotFoundException(f"[{sector_name}] Data for {target_time.strftime('%Y-%m-%d %H%M')} not found.")
            elif target_time.strftime('%Y-%m-%d %H%M') == price_df.tail(1).index[0].strftime('%Y-%m-%d %H%M'):
                price_df = price_df.tail(1)
            elif target_time.strftime('%Y-%m-%d %H%M') != price_df.index[0].strftime('%Y-%m-%d %H%M'):
                log.warning(f"{sector_name}의 {target_time.strftime('%Y-%m-%d %H%M')} 데이터가 없습니다.")
                raise DataNotFoundException(f"[{sector_name}] Data for {target_time.strftime('%Y-%m-%d %H%M')} not found.")
        
        # 첫 적재로 한 달치를 적재할 때 목적한 레코드가 제대로 들어와 있는지 확인
        else:
            price_df = yf.Sector(sector_name).ticker.history(period='1mo', interval='15m')
            price_df['Sector'] = sector_name
            # 가져온 한달 데이터의 마지막 날짜를 target_time으로 잡음. 첫 적재를 언제 하더라도 데이터를 가져오기 위함
            target_time = price_df.tail(1).index[0]

            if price_df.empty:
                log.warning(f"[{sector_name}] Fetched DataFrame is empty. Skipping Parquet upload.")
                raise DataNotFoundException(f"[{sector_name}] Data for {target_time.strftime('%Y-%m-%d %H%M')} not found.")

    except Exception as e:
        log.error(f"[{sector_name}] Error processing data or API response: {e}")
        raise

    s3_key = f"{s3_base_prefix}/{target_time.strftime('%Y-%m-%d')}/{target_time.strftime('%H%M')}/Sector/{sector_name}_{current_time.strftime('%Y-%m-%d %H-%M-%S')}.parquet"

    parquet_buffer = BytesIO()
    price_df.to_parquet(parquet_buffer, engine='pyarrow')
    parquet_buffer.seek(0)

    # 4. Parquet 데이터를 S3에 업로드
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_bytes(
        bytes_data=parquet_buffer.getvalue(),
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )
    log.info(f"[{sector_name}] Successfully uploaded Parquet data to S3: {s3_key}")


with DAG(
    dag_id='fetch_sector_stock_s3', 
    start_date=datetime(2025, 11, 17),
    schedule_interval='4,19,34,49 * * * *',
    catchup=False,
    tags=['s3', 'api', 'dynamic-mapping', 'parquet', 'dataframe'],
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(seconds=10),
    }
) as dag:
    # 1. S3 폴더 구조 확인 태스크
    check_s3_folder_task = PythonOperator(
        task_id='check_s3_folder',
        python_callable=check_deep_subfolder_existence,
        op_kwargs={
            'bucket_name': S3_BUCKET,
            'base_path': S3_PREFIX, # 확인하고 싶은 기본 폴더 경로
            'aws_conn_id': AWS_CONN_ID,
            'target_folder_name': TARGET_FOLDER_NAME
        },
        provide_context=True # XCom을 사용하기 위해 True 설정
    )

    # 2. 각 섹터별 데이터 추출 및 S3 업로드 태스크
    # check_s3_folder_task의 결과에 따라 s3_key가 동적으로 변경될 수 있도록 수정된 함수를 사용합니다.
    upload_tasks = []
    for sector in SECTOR_LIST:
        upload_task = PythonOperator(
            task_id=f'fetch_and_upload_sector_stock_{sector}',
            python_callable=_fetch_and_upload_sector_data_from_df_api,
            op_kwargs={
                's3_bucket': S3_BUCKET,
                's3_base_prefix': S3_PREFIX,
                'sector_name': sector,
                'aws_conn_id': AWS_CONN_ID
            },
            provide_context=True # XCom을 사용하기 위해 True 설정
        )
        upload_tasks.append(upload_task)


    # 태스크 의존성 설정
    check_s3_folder_task >> upload_tasks
