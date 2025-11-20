
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeLoader:
    """
        Load dataset from cleaned folder in S3 and store into Snowflake
    """

    def __init__(self, ymd, hm):
        self.hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        self.ymd = ymd
        self.hm = hm
        self.init_stage()

        


    def init_stage(self):
        aws_conn = BaseHook.get_connection("aws_conn")
        stage_cleaned = f"""
            create stage if not exists my_s3_stage
            url='s3://de07-project03/cleaned'
            CREDENTIALS = (
                AWS_KEY_ID='{aws_conn.login}',
                AWS_SECRET_KEY='{aws_conn.password}'
            )
            FILE_FORMAT = (TYPE=PARQUET)
        """
        self.hook.run(stage_cleaned)

        stage_news = f"""
            create stage if not exists my_s3_news_stage
            url='s3://de07-project03/news'
            CREDENTIALS = (
                AWS_KEY_ID='{aws_conn.login}',
                AWS_SECRET_KEY='{aws_conn.password}'
            )
            FILE_FORMAT = (TYPE=PARQUET);
        """
        self.hook.run(stage_news)






    def copy_stock_sector_from_s3(self):
        # Create table if not exist in raw_data schema
        create_table = """
            create table if not exists raw_data.stock_sector (
                Datetime TIMESTAMP_TZ,
                Sector STRING,
                Open FLOAT,
                High FLOAT,
                Low FLOAT,
                Close FLOAT
            );
        """
        self.hook.run(create_table)


        # Append (Copy)
        copy_table = f"""
            copy into raw_data.stock_sector
            from @my_s3_stage/stocks/Sector/{self.ymd}/{self.hm}.parquet
            file_format = (type = parquet)
            match_by_column_name = CASE_INSENSITIVE;
        """

        self.hook.run(copy_table)


    def copy_stock_company_from_s3(self):
        # Create table if not already exist in raw_data schema
        create_table = """
            create table if not exists raw_data.stock_company (
                Datetime TIMESTAMP_TZ,
                Company_symbol STRING,
                Open FLOAT,
                High FLOAT,
                Low FLOAT,
                Close FLOAT,
                Volume INT
            );
        """
        self.hook.run(create_table)


        # Append (Copy)
        copy_table = f"""
            copy into raw_data.stock_company
            from @my_s3_stage/stocks/Company/{self.ymd}/{self.hm}.parquet
            file_format = (type = parquet)
            match_by_column_name = CASE_INSENSITIVE;
        """

        self.hook.run(copy_table)



    def create_news_external_table(self):
        create_external_table = """
            CREATE EXTERNAL TABLE IF NOT EXISTS raw_data.news (
                content_id     STRING AS (VALUE:content_id::STRING),
                title          STRING AS (VALUE:title::STRING),
                summary        STRING AS (VALUE:summary::STRING),
                url            STRING AS (VALUE:url::STRING),
                company_key    STRING AS (VALUE:company_key::STRING),
                pubDate        TIMESTAMP AS (VALUE:pubDate::TIMESTAMP)
            )
            
            with location = @my_s3_news_stage
            auto_refresh = True
            file_format = (type = parquet)
            pattern = '.*[.]parquet';
        """
        self.hook.run(create_external_table)