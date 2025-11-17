import inspect
from typing import Any

from airflow.hooks.base import BaseHook
import yfinance as yf


class YfinanceNewsHook(BaseHook):
    """
    각 Company(ticker)의 뉴스를 가져오는 훅
    """

    _companies = None
    _company_info = None
    session = None

    def __init__(self, companies: list[str]) -> None:

        if not companies:
            raise ValueError("Company ticker must be provided")

        super().__init__("yfinance.task.hooks")
        self._companies = companies


    def get_conn(self) -> Any:
        """
        Do not use this method directly, use get_news instead.
        :return:
        :rtype:
        """
        self._is_called_internally()

        if not self.session:
            from utils.session_utils import get_session

            self.session = get_session()

        return self.session

    def _is_called_internally(self) -> bool:
        caller_self = inspect.currentframe().f_back.f_locals.get("self")
        return isinstance(caller_self, self.__class__)


    def get_news(self) -> list[dict[str, Any]]:
        """
        Get news for the specified company.
        :param count: page size
        :return: news data with company info

        :rtype: {
            "company_info": dict,
            "news": list
        }
        """
        session = self.get_conn()
        tickers = yf.Tickers(self._companies, session=session)

        all_news_records = [
            {**news_item, "company_key": ticker_symbol}
            for ticker_symbol, news_list in tickers.news().items()
            for news_item in news_list
        ]
        return all_news_records

