import logging

import boto3
from airflow.hooks.base import BaseHook
import awswrangler as wr


class S3ParquetHook(BaseHook):
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
                logging.info(
                    "Fetching S3 connection configuration for conn_id: %s", self.conn_id
                )
                self._config = self.get_connection(self.conn_id)
                logging.info("S3 connection configuration: %s", self._config.login)
                logging.info("S3 connection configuration: %s", self._config.password)
            self.conn = boto3.Session(
                aws_access_key_id=self._config.login,
                aws_secret_access_key=self._config.password,
                region_name=self.REGION_NAME,
            )

        return self.conn

    def upload_df(self, df, s3_path: str) -> None:
        """
        DataFrame을 S3에 Parquet 형식으로 업로드합니다.
        :param df: 업로드할 DataFrame
        :param s3_path: S3 경로 (예: s3://bucket_name/path/to/file.parquet)
        :return: None
        """

        wr.s3.to_parquet(
            df=df, path=s3_path, boto3_session=self.get_conn(), index=False
        )

    def upload_split_df(self, df, s3_path: str, partition_cols: list) -> None:
        """
        DataFrame을 S3에 Parquet 형식으로 파티셔닝하여 업로드합니다.
        :param df: 업로드할 DataFrame
        :param s3_path: S3 경로 (예: s3://bucket_name/path/to/directory/)
        :param partition_cols: 파티셔닝할 컬럼 리스트
        :return: None
        """

        wr.s3.to_parquet(
            df=df,
            path=s3_path,
            boto3_session=self.get_conn(),
            dataset=True,
            partition_cols=partition_cols,
            index=False,
        )
