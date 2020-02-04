from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CreateTableOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 sql_statment1="",
                 sql_statment2="",
                 sql_statment3="",
                 sql_statment4="",
                 sql_statment5="",
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.sql_statment1 = sql_statment1
        self.sql_statment2 = sql_statment2
        self.sql_statment3 = sql_statment3
        self.sql_statment4 = sql_statment4
        self.sql_statment5 = sql_statment5
        

    def execute(self, context):
        self.log.info('Creating the tables on Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Creating now tables')
        redshift.run(self.sql_statment1)
        redshift.run(self.sql_statment2)
        redshift.run(self.sql_statment3)
        redshift.run(self.sql_statment4)
        redshift.run(self.sql_statment5)
        
        
