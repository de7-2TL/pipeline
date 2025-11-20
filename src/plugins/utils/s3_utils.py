from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.fs as pa_fs
import pyarrow.parquet as pq
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


class S3:
    '''
        PlugIn Class 
        Read the Parquet files depends on the date/time in S3
        Clean the datasets and merge into one dataset then load into 2nd layer of S3
    '''

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
    



    def save_to_cleaned(self, df, obj='sector'):
        '''
            Save cleaned and merged dataset into cleaned folder which will be used for
            loading data into Snowflake
        '''
        obj = obj.lower()

        if obj == 'sector':
            prefix = f"cleaned/stocks/Sector/{self.ymd}/{self.hm}.parquet"

        elif obj == 'company':
            prefix = f"cleaned/stocks/Company/{self.ymd}/{self.hm}.parquet"

        else:
            raise KeyError('Wrong object : object should be either sector or company')
        
        table = pa.Table.from_pandas(df)
        buf = BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        # Save as parquet
        self.hook.load_bytes(
            bytes_data = buf.getvalue(),
            key=prefix,
            bucket_name=self.bucket,
            replace=True
        )


    def cleaning_stocks(self, df, obj='sector'):
        '''
            Depend on type of stock data, differently take each usefull columns 
            then return cleaned data frame
        '''

        obj = obj.lower()

        # Set the time format fits to Snowflake
        col = "Datetime"
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
        df[col] = df[col].dt.strftime("%Y-%m-%dT%H:%M:%S%z")
        df[col] = df[col].str.replace(r'(\+|\-)(\d{2})(\d{2})$', r'\1\2:\3', regex=True)

        # Extract only usefull columns
        if obj == 'sector':
            cols = ['Datetime', 'Sector', 'Open', 'High', 'Low', 'Close']
        elif obj == 'company':
            cols = ['Datetime', 'Company_symbol', 'Open', 'High', 'Low', 'Close', 'Volume']
        else:
            raise KeyError('Wrong object : object should be either sector or company')

        return df[cols]
