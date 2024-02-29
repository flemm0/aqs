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



# class S3ToRedshiftInsertNewRowsOperator(BaseOperator):
#     '''
#     Operator to insert new rows from S3 bucket files into Redshift staging table
#     '''
#     @apply_defaults
#     def __init__(self,
#                  s3_bucket,
#                  s3_key,
#                  redshift_conn_id,
#                  redshift_table,
#                  primary_key,
#                  *args, **kwargs):
#         super(S3ToRedshiftInsertNewRowsOperator, self).__init__(*args, **kwargs)
#         self.s3_bucket = s3_bucket
#         self.s3_key = s3_key
#         self.redshift_conn_id = redshift_conn_id
#         self.redshift_table = redshift_table
#         self.primary_key = primary_key

# def execute(self, context):
#     # Initialize S3 and Redshift connections
#     s3_hook = S3Hook()
#     redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

#     # Load data from S3
#     s3_object = s3_hook.get_key(self.s3_key, bucket_name=self.s3_bucket)
#     if s3_object is None:
#         raise ValueError(f"S3 object '{self.s3_key}' not found in bucket '{self.s3_bucket}'")

#     # Load data from S3 into a temporary staging table
#     with redshift_hook.get_conn() as conn:
#         with conn.cursor() as cursor:
#             copy_query = f"""
#                 COPY temp_staging_table
#                 FROM 's3://{self.s3_bucket}/{self.s3_key}'
#                 IAM_ROLE 'arn:aws:iam::YOUR_AWS_ACCOUNT_ID:role/YOUR_REDSHIFT_ROLE'
#                 DELIMITER ',' IGNOREHEADER 1;
#             """
#             cursor.execute(copy_query)

#     # Perform upsert operation
#     upsert_query = f"""
#         INSERT INTO {self.redshift_table}
#         SELECT *
#         FROM temp_staging_table
#         WHERE NOT EXISTS (
#             SELECT 1
#             FROM {self.redshift_table}
#             WHERE {self.primary_key} = temp_staging_table.{self.primary_key}
#         );

#         UPDATE {self.redshift_table}
#         SET col1 = temp_staging_table.col1,
#             col2 = temp_staging_table.col2,
#             ...
#         FROM temp_staging_table
#         WHERE {self.redshift_table}.{self.primary_key} = temp_staging_table.{self.primary_key};

#         DROP TABLE IF EXISTS temp_staging_table;
#     """
#     with redshift_hook.get_conn() as conn:
#         with conn.cursor() as cursor:
#             cursor.execute(upsert_query)
        
# import redshift_connector
# conn = redshift_connector.connect(
#     host='examplecluster.abc123xyz789.us-west-1.redshift.amazonaws.com',
#     port=5439,
#     database='dev',
#     user='awsuser',
#     password='my_password'
#  )