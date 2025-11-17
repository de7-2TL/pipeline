from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from datetime import datetime

class SnowflakeLoader:

    def __init__(self, tm):
        self.hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
        self.tm = datetime.strptime(tm, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        self.init_stage()

        


    def init_stage(self):
        aws_conn = BaseHook.get_connection("aws_conn")
        stage_init = f"""
            create stage if not exists my_s3_stage
            url='s3://de07-project03/cleaned'
            CREDENTIALS = (
                AWS_KEY_ID='{aws_conn.login}',
                AWS_SECRET_KEY='{aws_conn.password}'
            )
            FILE_FORMAT = (TYPE=PARQUET);
        """
        self.hook.run(stage_init)


    def copy_news_from_s3(self):
        # Create table if not exist in raw_data schema
        create_table = """
            create table if not exists raw_data.news (
                content_id STRING,
                title STRING,
                summary STRING, 
                url STRING,
                company_key String,
                pubDate TIMESTAMP_TZ 
            );
        """
        self.hook.run(create_table)

        # Fully refresh everyday
        self.hook.run("truncate table raw_data.news")

        # Append (Copy)
        copy_table = f"""
            copy into raw_data.news
            from @my_s3_stage/news/{self.tm}/
            file_format = (type = parquet)
            match_by_column_name = CASE_INSENSITIVE;
        """

        self.hook.run(copy_table)




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
            from @my_s3_stage/stocks/Sector/{self.tm}/
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
            from @my_s3_stage/stocks/Company/{self.tm}/
            file_format = (type = parquet)
            match_by_column_name = CASE_INSENSITIVE;
        """

        self.hook.run(copy_table)





if __name__ == "__main__":
    tm = "2025-11-17 17:30:00"
    sf = SnowflakeLoader(tm)
    sf.copy_news_from_s3()
    sf.copy_stock_sector_from_s3()
    sf.copy_stock_company_from_s3()