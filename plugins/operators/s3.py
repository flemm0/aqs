from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
# from airflow.contrib.hooks.aws_hook import AwsHook
# from airflow.hooks.postgres_hook import PostgresHook

from airflow.models import Variable

import duckdb


motherduck_token = Variable.get('MOTHERDUCK_TOKEN')

class S3ToMotherDuckOperator(BaseOperator):
    """
    Operator to copy data from s3 parquet file into MotherDuck
    """
    @apply_defaults
    def __init__(
        self,
        s3_bucket,
        s3_key,
        # motherduck_token,
        table,
        *args, **kwargs
    ):
        super(S3ToMotherDuckOperator, self).__init__(*args, **kwargs)
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        # self.motherduck_token = motherduck_token
        self.table = table

    def execute(self, context):
        ti = context['ti']
        s3_key = ti.xcom_pull(task_ids='write_daily_aqobs_data_to_s3_task', key='aqobs_s3_object_path')
        s3_object = f's3://{self.s3_bucket}/{s3_key}'
        conn = duckdb.connect(f'md:airnow_aqs?motherduck_token={motherduck_token}')
        insert_query = f"""
            INSERT INTO {self.table}
            SELECT * FROM '{s3_object}';
        """
        conn.execute(insert_query)



# class S3ToRedshiftOperator(BaseOperator):
#     """
#     Operator to copy data from Parquet files in S3 to Redshift staging table.
#     """

#     @apply_defaults
#     def __init__(
#         self,
#         s3_bucket,
#         s3_key,
#         redshift_conn_id,
#         table,
#         copy_options='',
#         *args, **kwargs
#     ):
#         super(S3ToRedshiftOperator, self).__init__(*args, **kwargs)
#         self.s3_bucket = s3_bucket
#         self.s3_key = s3_key
#         self.redshift_conn_id = redshift_conn_id
#         self.table = table
#         self.copy_options = copy_options

#     def execute(self, context):
#         aws_hook = AwsHook()
#         credentials = aws_hook.get_credentials()
#         redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

#         s3_path = f's3://{self.s3_bucket}/{self.s3_key}'
#         copy_query = f"""
#             COPY {self.table}
#             FROM '{s3_path}'
#             CREDENTIALS 'aws_access_key_id={credentials.access_key};aws_secret_access_key={credentials.secret_key}'
#             FORMAT AS PARQUET
#             {self.copy_options}
#         """

#         self.log.info(f'Copying data from {s3_path} to Redshift table {self.table}')
#         redshift_hook.run(copy_query)