import logging

import pandas as pd
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models import DagRun, TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from hooks.company_hook import (
    get_hook,
)
from hooks.s3_hook import S3ParquetHook
from hooks.yfinance_hook import YfinanceNewsHook
from utils.file_utils import mkdirs_if_not_exists
from utils.yfinance_df_cleaner import YFinanceNewsCleaner

YFINANCE_NEWS_DIR_TEMP_PATH = "/opt/airflow/data/temp/yfinance_news"
YFINANCE_NEWS_META_TEMP_PATH = "/opt/airflow/data/temp/yfinance_news/meta"
S3_NEWS_PATH = "news"
S3_NEWS_META_PATH = "meta/news"


def _get_company_keys() -> list[str]:
    """
    회사 티커 목록을 가져옵니다.
    get_hook: CompanyHook (
    dev -> 5개의 테스트용 티커 반환 MockHook을 반환합니다.
    prod: SnowflakeHook을 반환합니다.
    )
    :return:
    :rtype:
    """

    conn = get_hook("prod", "snowflake_conn")

    return conn.get_company_info()


def _extract_yfinance_news(ti: TaskInstance) -> str:
    company_keys = ti.xcom_pull(task_ids="get_company_keys", key="return_value")
    logging.info(f"fetch yfinance news data for {len(company_keys)}")
    """
    Yfinance에서 특정 회사의 뉴스를 가져와서 저장합니다.
    :param company_key: 회사 티커
    :return: output_path
    :rtype:
    """

    hook = YfinanceNewsHook(company_keys)

    news_json_list = hook.get_news()
    logging.info(f"success fetched news count: {len(news_json_list)}")
    if (not news_json_list) or (len(news_json_list) == 0):
        logging.warning(f"no news found for company: {company_keys}")
        raise AirflowSkipException(f"no news found for company: {company_keys}")

    news_df = pd.json_normalize(news_json_list)
    df = (
        YFinanceNewsCleaner(news_df)
        .rename_columns(
            {
                "content.id": "content_id",
                "content.title": "title",
                "content.summary": "summary",
                "content.canonicalUrl.url": "url",
                "content.pubDate": "pubDate",
                "company_key": "company_key",
            }
        )
        .set_date("pubDate")
        .select(
            [
                "content_id",
                "title",
                "summary",
                "url",
                "company_key",
                "pubDate",
                "date",
            ]
        )
    )

    output_path = f"{YFINANCE_NEWS_DIR_TEMP_PATH}/{ti.execution_date.strftime('%Y-%m-%d')}.parquet"
    mkdirs_if_not_exists(YFINANCE_NEWS_DIR_TEMP_PATH)

    df.to_parquet(output_path)
    logging.info(f"success saved news to {output_path}")

    return output_path


def _transform_news(s3_conn_id: str, ti: TaskInstance) -> str:
    """
    뉴스 데이터프레임을 정제합니다.
    - 중복된 뉴스 정보를 제거합니다.
    :param s3_conn_id: S3 연결 ID
    :return: 정제된 뉴스 Parquet 파일 경로
    :rtype:
    """
    mkdirs_if_not_exists(YFINANCE_NEWS_META_TEMP_PATH)
    df = pd.read_parquet(YFINANCE_NEWS_DIR_TEMP_PATH)
    hook = S3ParquetHook(conn_id=s3_conn_id or "aws_conn")
    meta_df = hook.get_files(S3_NEWS_META_PATH)
    output_path = f"{YFINANCE_NEWS_META_TEMP_PATH}/{ti.execution_date.strftime('%Y-%m-%d')}.parquet"

    cleaner = YFinanceNewsCleaner(df)
    cleaner.filter_duplicates_by_meta(meta_df, keys=["content_id", "company_key"])

    cleaner.target.to_parquet(output_path)
    return output_path


def _load_temp_to_s3(conn_id: str, ti: TaskInstance) -> str:
    """
    추출된 yfinance 뉴스를 통합하여 저장합니다.
    :param kwargs:
    :type kwargs:
    :return: output_path
    :rtype:
    """

    hook = S3ParquetHook(conn_id)

    df = pd.read_parquet(ti.xcom_pull(key="return_value"))
    meta_df = df[["content_id", "company_key"]]

    if not meta_df.empty:
        hook.upload_df(meta_df, path=S3_NEWS_META_PATH, mode="append")

    if not df.empty:
        hook.upload_split_df(df, path=S3_NEWS_PATH, partition_cols=["date"], mode="append")




def _teardown(dag_run: DagRun):
    import shutil

    logging.info(f"teardown dag run Type: {dag_run.run_type}")

    logging.info("teardown yfinance news data")
    shutil.rmtree(YFINANCE_NEWS_DIR_TEMP_PATH)


with DAG(
    dag_id="fetch_yfinance_news",
    schedule_interval="@hourly",
    start_date=pendulum.datetime(2025, 11, 1),
    tags=['s3', 'news', 'company'],
    catchup=False,
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    get_company_keys = PythonOperator(
        task_id="get_company_keys",
        python_callable=_get_company_keys,
    )
    
    
    with TaskGroup("YFinance_News_ETL_Group") as yfinance_news_etl_group:
        

        extract_yfinance_news = PythonOperator(
        task_id="extract_yfinance_news",
        python_callable=_extract_yfinance_news,
        )

        transform_news = PythonOperator(
        task_id="transform_news",
        python_callable=_transform_news,
        op_kwargs={"s3_conn_id": "aws_conn"},
        )

        load_temp_to_s3 = PythonOperator(
        task_id="load_temp_to_s3",
        python_callable=_load_temp_to_s3,
        op_kwargs={"conn_id": "aws_conn"},
        trigger_rule=TriggerRule.NONE_FAILED,
        )
        
        extract_yfinance_news >> transform_news >> load_temp_to_s3

    teardown = PythonOperator(
        task_id="teardown",
        python_callable=_teardown,
        doc_md="임시로 저장된 yfinance 뉴스를 삭제합니다.",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def watcher():
        raise AirflowException(
            "Failing task because one or more upstream tasks failed."
        )

    (
        start_task
        >> get_company_keys
        >> yfinance_news_etl_group
        >> teardown
    )
    list(dag.tasks) >> watcher()
