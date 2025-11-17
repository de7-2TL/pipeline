from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeCompanyHook(BaseHook):
    """
    Snowflake의 COMPANY_INFO 테이블에서
    회사의 company_symbol 리스트를 반환하는 Hook
    """

    def __init__(self, conn_id: str = "snowflake_conn") -> None:
        super().__init__("company.task.hooks")
        self.conn_id = conn_id

    def get_company_info(self) -> list[str]:
        """
        company symbol list 조회

        Returns:
            list[str]: 회사의 company_symbol 리스트
        """

        query = """
            SELECT company_symbol
            FROM dev.raw_data.COMPANY_INFO;
        """

        hook = SnowflakeHook(snowflake_conn_id=self.conn_id)
        df = hook.get_pandas_df(query)

        # company_symbol column → list[str]
        return df["COMPANY_SYMBOL"].dropna().tolist()
