from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.exceptions import AirflowException
from hooks.company_hook import get_hook

from datetime import datetime, UTC
from datetime import timedelta
from airflow.decorators import task
from airflow.operators.python import get_current_context

from io import BytesIO
import yfinance as yf
import logging
import pytz

log = logging.getLogger(__name__)

S3_BUCKET = "de07-project03"
S3_PREFIX = "stock"
SNOWFLAKE_CONN = "snowflake_conn"
AWS_CONN_ID = "aws_conn"
TARGET_FOLDER_NAME = "Company"
CHUNK_SIZE = 20


# 목적한 날짜와 시간의 데이터가 없을 때 발생시킬 오류
class DataNotFoundException(AirflowException):
    pass


class S3CheckError(AirflowException):
    pass


# s3의 stock폴더 아래 폴더가 있는지 확인하는 함수.
# 폴더가 있다면 첫 적재가 아니므로 한 달간의 stock 지수를 전부 가져오고 아닐 경우엔 새로운 레코드만 적재하기 위함.
def check_deep_subfolder_existence(
    bucket_name, base_path, target_folder_name, aws_conn_id, **context
):
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)

    if not base_path.endswith("/"):
        base_path += "/"

    target_pattern = f"{target_folder_name}/"
    found_paths = []
    current_time = datetime.now(UTC)

    log.info(f"S3 버킷 '{bucket_name}'의 경로 '{base_path}' 아래에서")
    log.info(f"임의의 깊이에 '{target_pattern}' 폴더 패턴 존재 확인 중...")

    try:
        all_keys = s3_hook.list_keys(
            bucket_name=bucket_name,
            prefix=base_path,  # base_path 아래 모든 객체 탐색
        )

        if all_keys is None:  # list_keys가 None을 반환할 수 있는 경우 처리
            log.info(f"'{base_path}' 아래에 객체가 없습니다.")
            return {"exists": False, "found_paths": []}

        potential_folders = set()

        for key in all_keys:
            if target_pattern in key:
                idx = key.find(target_pattern)
                if idx != -1:  # 패턴이 발견된 경우
                    full_folder_path = key[: idx + len(target_pattern)]
                    potential_folders.add(full_folder_path)

        if potential_folders:
            found_paths = list(potential_folders)
            log.info(
                f"'{base_path}' 아래에서 '{target_folder_name}' 폴더 패턴이 발견되었습니다."
            )
            return {
                "exists": True,
                "found_paths": found_paths,
                "current_time": current_time.isoformat(),
            }
        else:
            log.info(
                f"'{base_path}' 아래에서 '{target_folder_name}' 폴더 패턴을 찾을 수 없습니다."
            )
            return {
                "exists": False,
                "found_paths": [],
                "current_time": current_time.isoformat(),
            }

    except Exception as e:
        log.error(f"S3 버킷 조회 중 오류가 발생했습니다: {e}")
        raise S3CheckError(f"S3 폴더 확인 중 알 수 없는 오류 발생: {e}") from e


# .expand_kwargs(company_keys)로 Mapped Tasks를 구성하기 위해 @task 사용.
@task
def _get_company_keys_from_snowflake(snowflake_conn_id: str, chunk_size) -> list[str]:
    hook = get_hook("prod", conn_id=snowflake_conn_id)
    data_list = hook.get_company_info()

    # chunk_size에 맞춰 data_list를 쪼개어 담음.
    # "company_symbols"라는 동일한 키의 밸류로 담기며, 딕셔너리들의 리스트 구조로 최종 반환.
    chunks = [
        data_list[i : i + chunk_size] for i in range(0, len(data_list), chunk_size)
    ]
    return [{"company_symbols": chunk} for chunk in chunks]


# (pool='sector_upload_pool')는 Mapped Task의 동작 개수를 지정하기 위한 옵션, Mapped Task로 구성하면서 xcom을 받기 위해 @task 사용
@task(pool="sector_upload_pool")
def _fetch_and_upload_sector_data_from_df_api(
    s3_bucket, s3_base_prefix, company_symbols, aws_conn_id, **kwargs
) -> None:
    """
    주어진 섹터 이름으로 API를 호출하고 (결과가 DataFrame),
    그 DataFrame을 Parquet 파일로 S3에 적재합니다.
    폴더 구조 확인 결과를 XCom으로부터 받아 S3 Key 생성에 활용합니다.
    """
    context = get_current_context()
    check_result = context["ti"].xcom_pull(
        task_ids="check_s3_folder", key="return_value"
    )
    # xcom_pull 받은 시간 데이터를 datetime 형식으로 복원
    current_time = check_result.get("current_time")
    current_time = datetime.fromisoformat(current_time)

    try:
        if check_result and check_result.get("exists"):
            term = "1d"
        else:
            term = "1mo"

        price_df = yf.Tickers(company_symbols).history(period=term, interval="15m")

        for company_symbol in company_symbols:
            try:
                mtal_df = price_df.loc[
                    :, price_df.columns.get_level_values("Ticker") == company_symbol
                ]
                mtal_df.columns = mtal_df.columns.get_level_values("Price")
                mtal_df.index.name = "Datetime"
                ohlcv_df = mtal_df[
                    [
                        "Open",
                        "High",
                        "Low",
                        "Close",
                        "Volume",
                        "Dividends",
                        "Stock Splits",
                    ]
                ].copy()
                ohlcv_df.index = ohlcv_df.index.tz_convert("America/New_York")
                ohlcv_df["Company_symbol"] = company_symbol

                # 최초 적재가 아닌 업데이트일 경우 목표한 레코드만 추출
                if term == "1d":
                    # 목표로 하는 시간의 데이터만 남김
                    new_york_tz = pytz.timezone("America/New_York")
                    target_time = current_time.astimezone(new_york_tz) - timedelta(
                        minutes=19
                    )
                    ohlcv_df = ohlcv_df.loc[
                        ohlcv_df.index.strftime("%Y-%m-%d %H-%M")
                        == target_time.strftime("%Y-%m-%d %H-%M")
                    ]
                else:
                    # 가져온 한달 데이터의 마지막 날짜를 target_time으로 잡음. 언제 호출해도 첫 적재는 한 달의 데이터를 가져오도록
                    target_time = ohlcv_df.tail(1).index[0]

                if ohlcv_df.empty:
                    log.warning(f"[{company_symbol}] Fetched DataFrame is empty.")
                    raise DataNotFoundException(
                        f"[{company_symbol}] Data for {target_time.strftime('%Y-%m-%d %H%M')} not found."
                    )
                elif target_time.strftime("%Y-%m-%d %H%M") != ohlcv_df.tail(1).index[
                    0
                ].strftime("%Y-%m-%d %H%M"):
                    log.warning(f"{company_symbol}의 {target_time} 데이터가 없습니다. ")
                    raise DataNotFoundException(
                        f"[{company_symbol}] Data for {target_time} not found."
                    )

                s3_key = f"{s3_base_prefix}/{target_time.strftime('%Y-%m-%d')}/{target_time.strftime('%H%M')}/Company/{company_symbol}_{current_time.strftime('%Y-%m-%d %H-%M-%S')}.parquet"

                with BytesIO() as parquet_buffer:
                    ohlcv_df.to_parquet(
                        parquet_buffer, engine="pyarrow", compression="snappy"
                    )
                    parquet_buffer.seek(0)

                    # Parquet 데이터를 S3에 업로드
                    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
                    s3_hook.load_bytes(
                        bytes_data=parquet_buffer.getvalue(),
                        key=s3_key,
                        bucket_name=s3_bucket,
                        replace=True,
                    )
                    log.info(
                        f"[{company_symbol}] Successfully uploaded Parquet data to S3: {s3_key}"
                    )

            except Exception as e:
                log.warning(f"[{company_symbol}] Failed to process: {e}")
                raise

    except Exception as e:
        log.error(f"Batch fetch failed: {e}")
        raise


with DAG(
    dag_id="fetch_company_stock_s3_dag",
    start_date=datetime(2025, 11, 1),
    schedule_interval="4,19,34,49 * * * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["s3", "company", "stock"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 3,
        "retry_delay": timedelta(seconds=10),
    },
) as dag:
    # 1. S3 폴더 구조 확인 태스크
    check_s3_folder_task = PythonOperator(
        task_id="check_s3_folder",
        python_callable=check_deep_subfolder_existence,
        op_kwargs={
            "bucket_name": S3_BUCKET,
            "base_path": S3_PREFIX,  # 확인하고 싶은 기본 폴더 경로
            "aws_conn_id": AWS_CONN_ID,
            "target_folder_name": TARGET_FOLDER_NAME,
        },
        provide_context=True,  # XCom을 사용하기 위해 True 설정
    )

    # 2. company symbol 가져오는 태스크
    company_keys = _get_company_keys_from_snowflake(
        snowflake_conn_id=SNOWFLAKE_CONN, chunk_size=CHUNK_SIZE
    )

    # 3. 각 섹터별 데이터 추출 및 S3 업로드 태스크
    fetch_and_upload_sector_task = _fetch_and_upload_sector_data_from_df_api.partial(
        s3_bucket=S3_BUCKET,
        s3_base_prefix=S3_PREFIX,
        aws_conn_id=AWS_CONN_ID,
        # Mapped Task의 동작 개수를 지정하기 위한 옵션
        pool="sector_upload_pool",
    ).expand_kwargs(company_keys)

    # 태스크 의존성 설정
    check_s3_folder_task >> company_keys >> fetch_and_upload_sector_task
