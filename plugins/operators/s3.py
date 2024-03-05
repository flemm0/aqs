from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from airflow.contrib.hooks.aws_hook import AwsHook
# from airflow.hooks.postgres_hook import PostgresHook

from airflow.models import Variable

import duckdb

from typing import Sequence


motherduck_token = Variable.get('MOTHERDUCK_TOKEN')

class S3ToMotherDuckInsertOperator(BaseOperator):
    """
    Operator to copy data from s3 parquet file into MotherDuck
    """
    template_fields: Sequence[str] = ("s3_key",)

    def __init__(
        self,
        s3_bucket,
        s3_key,
        table,
        *args, **kwargs
    ):
        super(S3ToMotherDuckInsertOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table

    def execute(self, context):
        conn = duckdb.connect(f'md:airnow_aqs?motherduck_token={motherduck_token}')
        insert_query = f"""
            INSERT OR IGNORE INTO {self.table}
            SELECT * FROM 's3://{self.s3_bucket}/{self.s3_key}';
        """
        conn.execute(insert_query)

class S3ToMotherDuckInsertNewRowsOperator(BaseOperator):
    """
    Operator to copy data from s3 parquet file into MotherDuck
    """
    template_fields: Sequence[str] = ("date", "s3_key",) 

    def __init__(
        self,
        s3_bucket,
        s3_key,
        temp_table,
        table,
        date,
        *args, **kwargs
    ):
        super(S3ToMotherDuckInsertNewRowsOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.temp_table = temp_table
        self.date = date

    def execute(self, context):
        conn = duckdb.connect(f'md:airnow_aqs?motherduck_token={motherduck_token}')

        create_temp_table_query = f"""
            CREATE OR REPLACE TABLE {self.temp_table}
            AS ( SELECT * FROM 's3://{self.s3_bucket}/{self.s3_key}' );
        """
        print(f'Executing query: {create_temp_table_query}')
        conn.execute(create_temp_table_query)

        table_update_query = f"""
            INSERT INTO {self.table}
            SELECT *, '{self.date}' AS ValidDate
            FROM {self.temp_table}
            WHERE NOT EXISTS (
                SELECT * EXCLUDE (ValidDate) FROM {self.table}
            );
        """
        print(f'Executing query: {table_update_query}')
        conn.execute(table_update_query)

        drop_temp_table_query = f"""
            DROP TABLE IF EXISTS {self.temp_table}
        """
        print(f'Executing query: {drop_temp_table_query}')
        conn.execute(drop_temp_table_query)
