import logging
from typing import Literal, Optional

import boto3
from airflow.hooks.base import BaseHook
import awswrangler as wr
from pandas.core.interchange.dataframe_protocol import DataFrame


class S3ParquetHook(BaseHook):
    BASE_PATH = "s3://de07-project03/"
    conn_id = None
    REGION_NAME = "ap-northeast-2"
    _config = None
    conn = None

    def __init__(self, conn_id: str) -> None:
        super().__init__("s3.parquet.hooks")
        self.conn_id = conn_id

    def get_conn(self) -> None:
        if not self.conn:
            if not self._config:
                self._config = self.get_connection(self.conn_id)
            self.conn = boto3.Session(
                aws_access_key_id=self._config.login,
                aws_secret_access_key=self._config.password,
                region_name=self.REGION_NAME,
            )

        return self.conn

    def get_files(self, path: str) -> Optional[DataFrame]:
        """
        S3에서 Parquet 파일을 읽어 DataFrame으로 반환합니다.
        :param path: S3 path
        :type path:
        :return: DataFrame or None if no files found
        :rtype:
        """
        try:
            obj = wr.s3.read_parquet(
                path=self._get_s3_full_path(path),
                boto3_session=self.get_conn(),
            )
            return obj
        except Exception as e:
            logging.warning(
                f"No files found in the specified S3 path: {self._get_s3_full_path(path)}"
            )
            logging.error("error details:", exc_info=e)
            return None

    def upload_df(
        self,
        df,
        path: str,
        mode: Literal["append", "overwrite", "overwrite_partitions"] | None = None,
    ) -> None:
        """
        DataFrame을 S3에 Parquet 형식으로 업로드합니다.
        :param mode: 업로드 모드
        :type mode:
        :param df: 업로드할 DataFrame
        :param path: S3 경로 (예: s3://bucket_name/path/to/file.parquet)
        :return: None
        """

        wr.s3.to_parquet(
            df=df,
            path=self._get_s3_full_path(path),
            boto3_session=self.get_conn(),
            dataset=True,
            index=False,
            mode=mode,
        )

    def upload_split_df(
        self,
        df,
        path: str,
        *,
        partition_cols: list,
        mode: Literal["append", "overwrite", "overwrite_partitions"] = "overwrite",
    ) -> None:
        """
        DataFrame을 S3에 Parquet 형식으로 파티셔닝하여 업로드합니다.
        :param df: 업로드할 DataFrame
        :param path: S3 경로 (예: s3://bucket_name/path/to/directory/)
        :param partition_cols: 파티셔닝할 컬럼 리스트
        :param mode: 업로드 모드 (기본값: "overwrite")
        :return: None
        """

        wr.s3.to_parquet(
            df=df,
            path=self._get_s3_full_path(path),
            boto3_session=self.get_conn(),
            dataset=True,
            partition_cols=partition_cols,
            index=False,
            mode=mode,
        )

    def _get_s3_full_path(self, path):
        return self.BASE_PATH + path
