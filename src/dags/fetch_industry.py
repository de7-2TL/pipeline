import pandas as pd
import yfinance as yf
import os

from pendulum import timezone
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator, task

from hooks.snowflake_dev_raw_data_hook import SnowflakeDevRawDataHook
from operators.snowflake_full_refresh_operator import SnowflakeFullRefreshOperator


TMP_DIR = "/tmp/industry_information_dag"
os.makedirs(TMP_DIR, exist_ok=True)

###############################
# Get & Fetch Functions
###############################

def get_sector_from_dw(**context) -> pd.DataFrame:
    '''
    DW에서 sector 정보를 가져와서 xcom에 industry_df로 저장

    Returns:
        pd.DataFrame: sector 정보가 담긴 DataFrame
    '''
    ti = context["ti"]

    hook = SnowflakeDevRawDataHook()
    conn = hook.get_conn()

    query = """
        SELECT sector_key, sector_name, sector_symbol
        FROM dev.raw_data.SECTOR_INFO;
    """

    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [col[0] for col in cur.description]

    sector_df = pd.DataFrame(rows, columns=cols)
    print(f"[Sector] Loaded from Snowflake: {len(sector_df)} rows")


    # XCom push
    ti.xcom_push(key="sector_list", value=list(sector_df["SECTOR_KEY"]))

    return sector_df

def fetch_industry_df(**context) -> pd.DataFrame:
    '''
    industry 데이터를 yfinance에서 가져옴
    xcom에 industry_df로 저장

    Returns:
        pd.DataFrame: industry 정보가 담긴 DataFrame
    '''

    ti = context["ti"]
    sector_list = ti.xcom_pull(key="sector_list", task_ids="get_sector")

    industry_data = []
    processed = set()

    for sector_key in sector_list:
        sec = yf.Sector(sector_key)
        industries = sec.industries
        industries.reset_index(inplace=True)

        for _, ind in industries.iterrows():
            key = ind["key"]
            if key in processed:
                continue

            industry_data.append({
                "industry_key": key,
                "industry_name": ind["name"],
                "industry_symbol": ind["symbol"],
                "market_weight": ind["market weight"],
                "sector_key": sector_key,
            })
            processed.add(key)

    df = pd.DataFrame(industry_data).drop_duplicates(subset=["industry_key"]).reset_index(drop=True)
    df.columns = df.columns.str.upper()

    # Save to temp parquet
    filepath = f"{TMP_DIR}/industry.parquet"
    df.to_parquet(filepath, index=False)

    ti.xcom_push(key="industry_parquet_path", value=filepath)
    print(f"[Industry] {len(df)} rows loaded.")
    return df


# Cleanup Task
@task
def clean_parquet_files(**context):
    ti = context["ti"]
    industry_parquet_path = ti.xcom_pull(key="industry_parquet_path", task_ids="fetch_industry")

    if os.path.exists(industry_parquet_path):
        print(f"Removing temp file: {industry_parquet_path}")
        os.remove(industry_parquet_path)
        print(f"Removed temp file: {industry_parquet_path}")
            
    if os.path.exists(TMP_DIR):
        os.rmdir(TMP_DIR)
        print(f"Removed temp directory: {TMP_DIR}")

#########################
# DAG
#########################
with DAG(
    dag_id="fetch_industry_from_snowflake_dag",
    start_date=datetime(2025, 10, 1, tzinfo=timezone("America/New_York")),
    schedule_interval="*/15 9-15 * * 1-5", # 월-금, 9:00 ~ 15:45, 15분 간격
    catchup=False,
    tags=["information", "snowflake", "full_refresh","industry"]
) as dag:

    # get
    get_sector = PythonOperator(
        task_id="get_sector",
        python_callable=get_sector_from_dw,
        provide_context=True
    )

    # fetch
    fetch_industry = PythonOperator(
        task_id="fetch_industry",
        python_callable=fetch_industry_df,
        provide_context=True
    )

    # load
    load_industry = SnowflakeFullRefreshOperator(
        task_id="load_industry",
        source_task_id="fetch_industry",
        xcom_key="industry_parquet_path",
        table_name="INDUSTRY_INFO"
    )


    get_sector >> fetch_industry >> load_industry >> clean_parquet_files()