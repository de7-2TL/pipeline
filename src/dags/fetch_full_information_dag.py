import os

import pandas as pd
import pendulum
import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator, task
from airflow.utils.trigger_rule import TriggerRule
from operators.snowflake_full_refresh_operator import SnowflakeFullRefreshOperator

TMP_DIR = "/tmp/full_information_dag"
os.makedirs(TMP_DIR, exist_ok=True)

SECTOR_LIST = [
    "basic-materials", "communication-services", "consumer-cyclical",
    "consumer-defensive", "energy", "financial-services", "healthcare",
    "industrials", "real-estate", "technology", "utilities"
]

############################
# Fetch Functions
#############################

def fetch_sector_df(**context) -> pd.DataFrame:
    '''
    sector 데이터를 yfinance에서 가져옴
    xcom에 sector_df로 저장

    Returns:
        pd.DataFrame: sector 정보가 담긴 DataFrame
    '''
    ti = context["ti"]
    sector_data = []
    for sector in SECTOR_LIST:
        sec = yf.Sector(sector)
        sector_data.append({
            "sector_key": sector,
            "sector_name": sec.name,
            "sector_symbol": sec.symbol
        })

    df = pd.DataFrame(sector_data).drop_duplicates(subset=["sector_key"]).reset_index(drop=True)
    df.columns = df.columns.str.upper()

    # Save to temp parquet
    filepath = f"{TMP_DIR}/sector.parquet"
    df.to_parquet(filepath, index=False)

    # XCom push
    ti.xcom_push(key="sector_parquet_path", value=filepath)
    ti.xcom_push(key="sector_list", value=list(df["SECTOR_KEY"]))

    print(f"[Sector] {len(df)} rows loaded.")
    return df


def fetch_industry_df(**context) -> pd.DataFrame:
    '''
    industry 데이터를 yfinance에서 가져옴
    xcom에 industry_df로 저장

    Returns:
        pd.DataFrame: industry 정보가 담긴 DataFrame
    '''

    ti = context["ti"]
    sector_list = ti.xcom_pull(key="sector_list", task_ids="fetch_sector")

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
    ti.xcom_push(key="industry_list", value=list(df["INDUSTRY_KEY"]))
    print(f"[Industry] {len(df)} rows loaded.")
    return df


def fetch_company_df(**context) -> pd.DataFrame:
    '''
    company 데이터를 yfinance에서 가져옴
    xcom에 company_df로 저장

    Returns:
        pd.DataFrame: company 정보가 담긴 DataFrame

    '''
    ti = context["ti"]
    industry_list = ti.xcom_pull(key="industry_list", task_ids="fetch_industry")

    company_data = []
    skipped = set()

    for industry_key in industry_list:
        key = industry_key

        try:
            ind_api = yf.Industry(key)
            top_companies = ind_api.top_companies

            if top_companies is None or top_companies.empty:
                skipped.add(key)
                continue

            top_companies = top_companies[:5].reset_index()
            for _, row in top_companies.iterrows():
                sym = row.get("symbol")
                name = row.get("name")
                if sym:
                    company_data.append({
                        "company_symbol": sym,
                        "company_name": name,
                        "industry_key": key
                    })
        except Exception:
            skipped.add(key)

    df = pd.DataFrame(company_data).drop_duplicates(subset=["company_symbol"]).reset_index(drop=True)
    df.columns = df.columns.str.upper()

    # Save to temp parquet
    filepath = f"{TMP_DIR}/company.parquet"
    df.to_parquet(filepath, index=False)

    ti.xcom_push(key="company_parquet_path", value=filepath)
    print(f"[Company] {len(df)} rows loaded. Skipped industries: {len(skipped)}")
    return df


# Cleanup Task
@task(trigger_rule=TriggerRule.ALL_DONE)
def clean_parquet_files(**context):
    ti = context["ti"]
    sector_parquet_path = ti.xcom_pull(key="sector_parquet_path", task_ids="fetch_sector")
    industry_parquet_path = ti.xcom_pull(key="industry_parquet_path", task_ids="fetch_industry")
    company_parquet_path = ti.xcom_pull(key="company_parquet_path", task_ids="fetch_company")

    files = [
        sector_parquet_path,
        industry_parquet_path,
        company_parquet_path
    ]
    for file in files:
        if os.path.exists(file):
            print(f"Removing temp file: {file}")
            os.remove(file)
            print(f"Removed temp file: {file}")
            
    if os.path.exists(TMP_DIR):
        os.rmdir(TMP_DIR)
        print(f"Removed temp directory: {TMP_DIR}")

#########################
# DAG
#########################

with DAG(
    dag_id="fetch_full_information",
    start_date=pendulum.datetime(2025, 11, 1),
    schedule_interval="@daily", 
    catchup=False,
    tags=["information", "sector", "industry", "company", "snowflake"],
) as dag:

    # Fetch
    fetch_sector = PythonOperator(
        task_id="fetch_sector",
        python_callable=fetch_sector_df,
        provide_context=True
    )

    fetch_industry = PythonOperator(
        task_id="fetch_industry",
        python_callable=fetch_industry_df,
        provide_context=True
    )

    fetch_company = PythonOperator(
        task_id="fetch_company",
        python_callable=fetch_company_df,
        provide_context=True
    )

    # Load (Full Refresh)
    load_sector = SnowflakeFullRefreshOperator(
        task_id="load_sector",
        source_task_id="fetch_sector",
        xcom_key="sector_parquet_path",
        table_name="SECTOR_INFO"
    )

    load_industry = SnowflakeFullRefreshOperator(
        task_id="load_industry",
        source_task_id="fetch_industry",
        xcom_key="industry_parquet_path",
        table_name="INDUSTRY_INFO"
    )

    load_company = SnowflakeFullRefreshOperator(
        task_id="load_company",
        source_task_id="fetch_company",
        xcom_key="company_parquet_path",
        table_name="COMPANY_INFO"
    )

    ##
    fetch_sector >> fetch_industry >> fetch_company >> [
        load_sector, load_industry, load_company
    ] >> clean_parquet_files()
