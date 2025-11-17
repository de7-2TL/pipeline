from airflow.models import BaseOperator
from snowflake.connector.pandas_tools import write_pandas
from hooks.snowflake_dev_raw_data_hook import SnowflakeDevRawDataHook

class SnowflakeFullRefreshOperator(BaseOperator):

    def __init__(self, source_task_id, xcom_key, table_name, **kwargs):
        super().__init__(**kwargs)
        self.source_task_id = source_task_id
        self.xcom_key = xcom_key
        self.table_name = table_name

    def execute(self, context):
        ti = context['ti']

        df = ti.xcom_pull(task_ids=self.source_task_id, key=self.xcom_key)
        if df is None:
            raise ValueError("❌ XCom에서 DataFrame을 가져오지 못했습니다.")

        # full refresh 로직 동일
        hook = SnowflakeDevRawDataHook()
        conn = hook.get_conn()

        temp = f"{self.table_name}_TEMP"
        write_pandas(conn, df, table_name=temp, overwrite=True)

        with conn.cursor() as cur:
            cur.execute(
                f"CREATE OR REPLACE TABLE dev.raw_data.{self.table_name} AS "
                f"SELECT * FROM dev.raw_data.{temp}"
            )
            cur.execute(f"DROP TABLE IF EXISTS dev.raw_data.{temp}")

        return f"Refreshed: {self.table_name}"
