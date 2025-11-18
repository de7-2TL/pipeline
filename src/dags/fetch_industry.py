import pandas as pd
import yfinance as yf

from pendulum import timezone
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from hooks.snowflake_dev_raw_data_hook import SnowflakeDevRawDataHook
from operators.snowflake_full_refresh_operator import SnowflakeFullRefreshOperator

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
    
    ti.xcom_push(key="sector_df", value=sector_df)

    return sector_df

def fetch_industry_df(**context) -> pd.DataFrame:
    '''
    industry 데이터를 yfinance에서 가져옴
    xcom에 industry_df로 저장

    Returns:
        pd.DataFrame: industry 정보가 담긴 DataFrame
    '''

    ti = context["ti"]
    sector_df = ti.xcom_pull(key="sector_df", task_ids="get_sector")

    industry_data = []
    processed = set()

    for _, row in sector_df.iterrows():
        sec = yf.Sector(row["SECTOR_KEY"])
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
                "sector_key": row["SECTOR_KEY"],
            })
            processed.add(key)

    df = pd.DataFrame(industry_data).drop_duplicates(subset=["industry_key"]).reset_index(drop=True)
    df.columns = df.columns.str.upper()

    ti.xcom_push(key="industry_df", value=df)
    print(f"[Industry] {len(df)} rows loaded.")
    return df

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
        xcom_key="industry_df",
        table_name="INDUSTRY_INFO"
    )


    get_sector >> fetch_industry >> load_industry