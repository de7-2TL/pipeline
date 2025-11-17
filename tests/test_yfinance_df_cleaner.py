import pytest

from utils.yfinance_df_cleaner import YFinanceNewsCleaner
import pandas as pd


@pytest.fixture(scope="module")
def news_df():
    import pandas as pd

    return pd.read_parquet("./data/news_mock.parquet")


def test_rename_columns(news_df):
    """
    Test rename columns in YFinanceNewsCleaner
    :param news_df:
    :type news_df:
    :return:
    :rtype:
    """
    expected_columns = {
        "content_id",
        "title",
        "summary",
        "url",
        "company_key",
        "pubDate",
    }
    cleaner = YFinanceNewsCleaner(news_df)
    cleaner.rename_columns(
        {
            "content.id": "content_id",
            "content.title": "title",
            "content.summary": "summary",
            "content.canonicalUrl.url": "url",
            "content.pubDate": "pubDate",
            "company_key": "company_key",
        }
    )
    target = cleaner.target.columns

    assert expected_columns <= set(target)


def test_set_date(news_df):
    """
    Test set date column in YFinanceNewsCleaner
    :param news_df:
    :type news_df:
    :return:
    :rtype:
    """
    cleaner = YFinanceNewsCleaner(news_df)
    cleaner.set_date("content.pubDate")
    target = cleaner.target

    assert "date" in target.columns
    assert pd.api.types.is_datetime64_any_dtype(
        target["date"]
    ) or pd.api.types.is_object_dtype(target["date"])
