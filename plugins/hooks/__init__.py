from airflow.hooks.base import BaseHook
import snowflake.connector
from airflow.models import Variable



class SnowflakeHook(BaseHook):
    '''
    returns connection to Snowflake
    '''

    user = Variable.get('SNOWFLAKE_USERNAME')
    password = Variable.get('SNOWFLAKE_PASSWORD')
    account = Variable.get('SNOWFLAKE_ACCOUNT')
    database = 'airnow_aqs'
    schema = 'staging'

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, *kwargs)
    
    def get_conn(self):
        conn = snowflake.connector.connect(
            user=self.user,
            password=self.password,
            database=self.database,
            account=self.account,
            schema=self.schema
        )
        return conn