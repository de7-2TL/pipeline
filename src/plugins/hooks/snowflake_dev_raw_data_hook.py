from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

class SnowflakeDevRawDataHook(SnowflakeHook):
    """
    Snowflake dev.raw_data 스키마에 연결하는 Hook.

    - 세션 DB를 dev로 설정
    - 세션 Schema를 raw_data로 설정
    """

    def __init__(self, snowflake_conn_id: str = "snowflake_conn"):
        super().__init__(snowflake_conn_id=snowflake_conn_id)
        self.conn_id = snowflake_conn_id

    def get_conn(self):
        """
        Snowflake 연결 생성 후 Session-level DB/SCHEMA 설정
        """
        conn = super().get_conn()

        with conn.cursor() as cur:
            cur.execute("USE DATABASE dev")
            cur.execute("USE SCHEMA raw_data")

        return conn
