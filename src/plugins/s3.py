from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
import pyarrow as pa
from io import BytesIO

from datetime import datetime


class S3:

    def __init__(self, ymd, hm):
        self.hook = S3Hook(aws_conn_id = 'aws_conn')
        self.bucket = Variable.get('AWS_BUCKET')
        self.ymd = ymd
        self.hm = hm

        credential = self.hook.get_credentials()
        self.fs = pa_fs.S3FileSystem(
            access_key = credential.access_key,
            secret_key = credential.secret_key,
            region='ap-northeast-2'
        )



    def read_parquet(self, path):
        s3_key_path = f"{self.bucket}/{path}"
        dataset = ds.dataset(
                    s3_key_path,
                    filesystem=self.fs,
                    format="parquet"
                )
        return dataset.to_table().to_pandas(ignore_metadata=True)
    



    def save_to_cleaned(self, df, obj='news'):
        obj = obj.lower()

        if obj == 'news':
            prefix = f"cleaned/news/{self.ymd}/cleaned.parquet"

        elif obj == 'sector':
            prefix = f"cleaned/stocks/Sector/{self.ymd}/{self.hm}.parquet"

        elif obj == 'company':
            prefix = f"cleaned/stocks/Company/{self.ymd}/{self.hm}.parquet"

        else:
            raise KeyError('Wrong object : object should be either news or sector or company')
        
        table = pa.Table.from_pandas(df)
        buf = BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)


        self.hook.load_bytes(
            bytes_data = buf.getvalue(),
            key=prefix,
            bucket_name=self.bucket,
            replace=True
        )


    def cleaning_stocks(self, df, obj='sector'):
        '''
            Convert timezone to Korea/Seoul
            Depend on type of stock data, differently take each usefull columns 
            then return cleaned data frame
        '''

        obj = obj.lower()

        col = "Datetime"
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        df[col] = df[col].str.replace(r'(\+|\-)(\d{2})(\d{2})$', r'\1\2:\3', regex=True)

        if obj == 'sector':
            cols = ['Datetime', 'Sector', 'Open', 'High', 'Low', 'Close']
        elif obj == 'company':
            cols = ['Datetime', 'Company_symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
        else:
            raise KeyError('Wrong object : object should be either sector or company')

        return df[cols]



    def cleaning_news(self, df):
        '''
            pick only id, title, summary, url, company_key, pubDate the conver pubDate into DATE format
        '''

        col = "pubDate"
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        df[col] = df[col].str.replace(r'(\+|\-)(\d{2})(\d{2})$', r'\1\2:\3', regex=True)

        cols = ['content_id', 'title', 'summary', 'url', 'company_key', 'pubDate']

        return df[cols]




if __name__ == '__main__':
    time = datetime.strptime("2025-11-17 17:30:00", "%Y-%m-%d %H:%M:%S")
    ymd = time.strftime("%Y-%m-%d")
    hm = time.strftime("%H%M")

    s3 = S3(ymd, hm)
    news = f'news/date={ymd}'
    stock_sector = f'stock/{ymd}/{hm}/Sector'

    s3.save_to_cleaned(s3.cleaning_news(s3.read_parquet(news)))
    s3.save_to_cleaned(s3.cleaning_stocks(s3.read_parquet(stock_sector), 'sector'), 'sector')