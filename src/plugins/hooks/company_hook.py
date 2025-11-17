from abc import ABC, abstractmethod
from typing import Any, Literal

from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class AbstractCompanyHook(BaseHook, ABC):
    """
    회사 정보를 조회하는 Hook의 추상 클래스
    """

    conn_id = None

    def __init__(self, logger_name: str | None = None, conn_id: str = "") -> None:
        super().__init__(logger_name)
        self.conn_id = conn_id

    @abstractmethod
    def get_company_info(self) -> list[str]:
        raise NotImplementedError()


def get_hook(hook_type: Literal["dev", "prod"], conn_id) -> AbstractCompanyHook:
    if hook_type == "prod":
        return SnowflakeCompanyHook(conn_id)
    elif hook_type == "dev":
        return TestCompanyHook(conn_id)
    else:
        raise ValueError(f"Unknown hook type: {hook_type}")


class TestCompanyHook(AbstractCompanyHook):
    """
    테스트용 Hook
    """

    def __init__(self, conn_id) -> None:
        super().__init__("test.company.hooks")
        self.conn_id = conn_id

    def get_conn(self) -> Any:
        return None

    def get_company_info(self) -> list[str]:
        """
        테스트용 회사 심볼 리스트 반환

        Returns:
            list[str]: 테스트용 회사 심볼 리스트
        """
        return ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


class SnowflakeCompanyHook(AbstractCompanyHook):
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
