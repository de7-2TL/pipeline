import logging
import os

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from hooks.company_hook import (
    SnowflakeCompanyHook,
)
from hooks.s3_hook import S3ParquetHook
from hooks.yfinance_hook import YfinanceNewsHook
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

YFINANCE_NEWS_DIR_PATH = "/opt/airflow/data/temp/yfinance_news"
S3_NEWS_PATH = "s3://de07-project03/news"


def _extract_yfinance_news(company_keys: list[str]) -> str:
    logging.info(f"fetch yfinance news data for {company_keys}")
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
    df = _clear_news_df(news_df)
    output_path = f"{YFINANCE_NEWS_DIR_PATH}/{'-'.join(company_keys)}.parquet"

    df.to_parquet(output_path)
    logging.info(f"success saved news to {output_path}")

    return output_path


def _clear_news_df(df: pd.DataFrame) -> pd.DataFrame:
    result = (
        df.rename(
            columns={
                "content.id": "content_id",
                "content.title": "title",
                "content.summary": "summary",
                "content.canonicalUrl.url": "url",
                "content.pubDate": "pubDate",
                "company_key": "company_key",
            }
        )
        .assign(
            datetime_tz=lambda x: pd.to_datetime(
                x["pubDate"], errors="coerce", utc=True
            ),
        )
        .assign(date=lambda x: x["datetime_tz"].dt.strftime("%Y-%m-%d"))
        .drop(columns=["datetime_tz"])[
            [
                "content_id",
                "title",
                "summary",
                "url",
                "company_key",
                "pubDate",
                "date",
            ]
        ]
    )
    return result


def _create_yfinance_news_extract_task(**kwargs):
    """
    조회 대상 회사 목록을 조회하여 각각에 대해 뉴스를 추출하는 태스크를 생성합니다.

    :param kwargs:
    :type kwargs:
    :return:
    :rtype:
    """
    if not os.path.exists(YFINANCE_NEWS_DIR_PATH):
        os.makedirs(YFINANCE_NEWS_DIR_PATH)

    conn = SnowflakeCompanyHook("snowflake_conn")

    def chunk_list(data_list, n):
        chunk_size = max(1, len(data_list) // n)

        return [
            data_list[i : i + chunk_size] for i in range(0, len(data_list), chunk_size)
        ]

    company_chunks = chunk_list(conn.get_company_info(), 10)

    return [
        PythonOperator(
            task_id=f"save_yfinance_news_group_{i}",
            python_callable=_extract_yfinance_news,
            op_kwargs={"company_keys": chunk},
            pool="yfinance_news_pool",
        )
        for i, chunk in enumerate(company_chunks)
    ]


def _load_temp_to_s3(conn_id: str, **kwargs) -> str:
    """
    추출된 yfinance 뉴스를 통합하여 저장합니다.
    :param kwargs:
    :type kwargs:
    :return: output_path
    :rtype:
    """

    hook = S3ParquetHook(conn_id)
    df = pd.read_parquet(YFINANCE_NEWS_DIR_PATH)

    hook.upload_split_df(df, s3_path=S3_NEWS_PATH, partition_cols=["date"])


with DAG(
    dag_id="yfinance_news_dag",
    schedule_interval="*/15 * * * *",
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    with TaskGroup(
        "extract_yfinance_news_task_group"
    ) as extract_yfinance_news_task_group:
        _create_yfinance_news_extract_task()

    load_temp_to_s3 = PythonOperator(
        task_id="load_temp_to_s3",
        python_callable=_load_temp_to_s3,
        op_kwargs={"conn_id": "aws_conn"},
        trigger_rule=TriggerRule.NONE_FAILED
    )



    start_task >> extract_yfinance_news_task_group >> load_temp_to_s3
