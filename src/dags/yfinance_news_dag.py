import logging

import pendulum
from airflow import DAG
from airflow.exceptions import AirflowSkipException, AirflowException
from airflow.models import TaskInstance
from airflow.utils.trigger_rule import TriggerRule

from hooks.company_hook import (
    SnowflakeCompanyHook,
)
from hooks.s3_hook import S3ParquetHook
from hooks.yfinance_hook import YfinanceNewsHook
import pandas as pd
from airflow.operators.python import PythonOperator, task
from airflow.operators.empty import EmptyOperator


YFINANCE_NEWS_DIR_TEMP_PATH = "/opt/airflow/data/temp/yfinance_news"
S3_NEWS_PATH = "s3://de07-project03/news"


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
    df = _clear_news_df(news_df)
    output_path = f"{YFINANCE_NEWS_DIR_TEMP_PATH}/{ti.execution_date}.parquet"

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


def _get_company_keys() -> list[str]:
    conn = SnowflakeCompanyHook("snowflake_conn")
    return conn.get_company_info()




def _load_temp_to_s3(conn_id: str) -> str:
    """
    추출된 yfinance 뉴스를 통합하여 저장합니다.
    :param kwargs:
    :type kwargs:
    :return: output_path
    :rtype:
    """

    hook = S3ParquetHook(conn_id)
    df = pd.read_parquet(YFINANCE_NEWS_DIR_TEMP_PATH)

    hook.upload_split_df(df, s3_path=S3_NEWS_PATH, partition_cols=["date"])

def _teardown():
    import shutil
    shutil.rmtree(YFINANCE_NEWS_DIR_TEMP_PATH)

    logging.info(f"teardown yfinance news data")

with DAG(
    dag_id="yfinance_news_dag",
    schedule_interval="*/15 * * * *",
    start_date=pendulum.datetime(2025, 11, 1, tz="UTC"),
    catchup=False,
) as dag:
    start_task = EmptyOperator(task_id="start_task")

    get_company_keys = PythonOperator(
        task_id="get_company_keys",
        python_callable=_get_company_keys,
    )

    extract_yfinance_news = PythonOperator(
        task_id="extract_yfinance_news",
        python_callable=_extract_yfinance_news,
    )


    load_temp_to_s3 = PythonOperator(
        task_id="load_temp_to_s3",
        python_callable=_load_temp_to_s3,
        op_kwargs={"conn_id": "aws_conn"},
        trigger_rule=TriggerRule.NONE_FAILED
    )

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



    start_task >> get_company_keys >> extract_yfinance_news >> load_temp_to_s3 >> teardown
    list(dag.tasks) >> watcher()
