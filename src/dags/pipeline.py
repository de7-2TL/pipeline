from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from plugins.s3 import S3
from plugins.to_snowflake import SnowflakeLoader





def get_time_context(exec_dt):
    """
        Get current Dag execution time
    """
    ymd = exec_dt.strftime("%Y-%m-%d")
    hm = exec_dt.strftime("%H%M")
    return ymd, hm



def init_ext_news_table(**context):
    """
        Create external table for News data 
        This function will run only once at first exeucution within DAG
    """

    exec_dt = context["execution_date"]
    ymd, hm = get_time_context(exec_dt)

    sf = SnowflakeLoader(ymd, hm)
    sf.create_news_external_table()



# ----------
#  S3 Part
# ----------

def load_and_clean_sector(**context):
    """
        Clean and Merge data set from raw data to restore in cleaned folder
    """

    exec_dt = context["execution_date"]
    ymd, hm = get_time_context(exec_dt)

    s3 = S3(ymd, hm)
    path = f'stock/{ymd}/{hm}/Sector'

    df = s3.read_parquet(path)
    cleaned_df = s3.cleaning_stocks(df, 'sector')
    s3.save_to_cleaned(cleaned_df, 'sector')


def load_and_clean_company(**context):
    """
        Clean and Merge data set from raw data to restore in cleaned folder
    """

    exec_dt = context["execution_date"]
    ymd, hm = get_time_context(exec_dt)

    s3 = S3(ymd, hm)
    path = f'stock/{ymd}/{hm}/Company'

    df = s3.read_parquet(path)
    cleaned_df = s3.cleaning_stocks(df, 'company')
    s3.save_to_cleaned(cleaned_df, 'company')




# ----------------
#  Snowflake Part
# ----------------

def copy_sector_snowflake(**context):
    """
        Read data set from S3 cleaned folder with given 
        execution date, copy (append) into Snowflake raw_data.stock_sector
    """

    exec_dt = context["execution_date"]
    ymd , hm = get_time_context(exec_dt)

    sf = SnowflakeLoader(ymd, hm)
    sf.copy_stock_sector_from_s3()


def copy_company_snowflake(**context):
    """
        Read data set from S3 cleaned folder with given 
        execution date, copy (append) into Snowflake raw_data.stock_company
    """
    exec_dt = context["execution_date"]
    ymd , hm = get_time_context(exec_dt)

    sf = SnowflakeLoader(ymd, hm) 
    sf.copy_stock_company_from_s3()


# DAG INIT


with DAG(
    dag_id='Project_Pipeline',
    start_date=datetime(2025, 11, 17, 17, 30),
    schedule_interval = '*/15 * * * *',
    catchup=False,
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }
) as dag:
    
    # Empty Operator to announce Dag just started
    start = EmptyOperator(task_id = 'start')


    # Initialize Task Group
    # Create External Table for News -> execute only once
    with TaskGroup('init_group') as init_group:
        init_news = PythonOperator(
            task_id = "init_news_table",
            python_callable = init_ext_news_table,
            provide_context = True
        )

        [init_news]


    # S3 work group
    # Read all parquet in each folder then clean and merge to store in cleaned folder
    with TaskGroup('s3_work_group') as s3_work_group:
        load_clean_sector = PythonOperator(
            task_id = 'load_and_clean_sector',
            python_callable = load_and_clean_sector,
            provide_context=True
        )

        load_clean_company = PythonOperator(
            task_id = 'load_and_clean_company',
            python_callable = load_and_clean_company,
            provide_context=True
        )

        [load_clean_sector, load_clean_company]


    # Snowflake work group
    # Copy data from cleaned folder in S3 to Snowflake raw_data schema
    with TaskGroup('snowflake_work_group') as snowflake_work_group:
        copy_sector = PythonOperator(
            task_id = 'copy_sector_to_snowflake',
            python_callable = copy_sector_snowflake,
            provide_context=True
        )

        copy_company = PythonOperator(
            task_id = 'copy_company_to_snowflake',
            python_callable = copy_company_snowflake,
            provide_context=True
        )

        [copy_sector, copy_company] 


    # Empty Operator to announce Dag just ended
    end = EmptyOperator(task_id = 'end')


    # Dependecies
    start >> init_group >> s3_work_group >> snowflake_work_group >> end
