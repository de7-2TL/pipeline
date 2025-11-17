import pandas as pd
import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator, task
from airflow.utils.trigger_rule import TriggerRule
from airflow.exceptions import AirflowException

from pendulum import timezone
from datetime import datetime

from operators.snowflake_full_refresh_operator import SnowflakeFullRefreshOperator


SECTOR_LIST = [
    "basic-materials", "communication-services", "consumer-cyclical",
    "consumer-defensive", "energy", "financial-services", "healthcare",
    "industrials", "real-estate", "technology", "utilities"
]

############################
# Fetch Functions
#############################

def fetch_sector_df(**context):
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

    # XCom push
    context["ti"].xcom_push(key="sector_df", value=df)

    print(f"[Sector] {len(df)} rows loaded.")
    return df


def fetch_industry_df(**context):
    ti = context["ti"]
    sector_df = ti.xcom_pull(key="sector_df", task_ids="fetch_sector")

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


def fetch_company_df(**context):
    ti = context["ti"]
    industry_df = ti.xcom_pull(key="industry_df", task_ids="fetch_industry")

    company_data = []
    skipped = set()

    for _, ind in industry_df.iterrows():
        key = ind["INDUSTRY_KEY"]

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

    ti.xcom_push(key="company_df", value=df)
    print(f"[Company] {len(df)} rows loaded. Skipped industries: {len(skipped)}")
    return df


# watcher task
@task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
def watcher():
    raise AirflowException(
        "Failing task because one or more upstream tasks failed."
    )


#########################
# DAG
#########################

with DAG(
    dag_id="company_full_refresh_dag",
    start_date=datetime(2025, 10, 1, tzinfo=timezone("America/New_York")),
    schedule_interval="0 9 * * 1-5",   # 평일 오전 9시
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
        xcom_key="sector_df",
        table_name="SECTOR_INFO"
    )

    load_industry = SnowflakeFullRefreshOperator(
        task_id="load_industry",
        source_task_id="fetch_industry",
        xcom_key="industry_df",
        table_name="INDUSTRY_INFO"
    )

    load_company = SnowflakeFullRefreshOperator(
        task_id="load_company",
        source_task_id="fetch_company",
        xcom_key="company_df",
        table_name="COMPANY_INFO"
    )

    ##
    fetch_sector >> fetch_industry >> fetch_company >> [
        load_sector, load_industry, load_company
    ] >> watcher()
