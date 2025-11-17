import pandas as pd
from pandas import DataFrame


class YFinanceNewsCleaner:
    target = None

    def __init__(self, target: DataFrame) -> None:
        self.target: DataFrame = target

    def rename_columns(self, columns_map: dict) -> "YFinanceNewsCleaner":
        self.target = self.target.rename(columns=columns_map)

        return self

    def set_date(
        self, target_column: str, date_format: str = "%Y-%m-%d"
    ) -> "YFinanceNewsCleaner":
        """
        set Date Column from target_column
        :param target_column:
        :type target_column:
        :param date_format:
        :type date_format:
        :return:
        :rtype:
        """
        self.target = (
            self.target.assign(
                datetime_tz=lambda x: pd.to_datetime(
                    x[target_column], errors="coerce", utc=True
                ),
            )
            .assign(date=lambda x: x["datetime_tz"].dt.strftime(date_format))
            .drop(columns=["datetime_tz"])
        )

        return self

    def drop_columns(self, columns: list[str]) -> "YFinanceNewsCleaner":
        self.target = self.target.drop(columns)

        return self

    def select(self, columns: list[str]) -> DataFrame:
        return self.target[columns]

    def clear(self, fn, *args, **kwargs) -> "YFinanceNewsCleaner":
        """
        General DataFrame cleaning function
        :param fn:
        :type fn:
        :param args:
        :type args:
        :param kwargs:
        :type kwargs:
        :return:
        :rtype:
        """
        self.target = fn(self.target, *args, **kwargs)

        return self
