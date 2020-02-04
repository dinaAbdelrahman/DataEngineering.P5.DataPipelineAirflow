from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from create_tables_sql import CREATE_ALL_TABLES_SQL


class CreateTableOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id=""
                 *args, **kwargs):

        super(CreateTableOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info('Creating the tables on Redshift')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for create_table in CREATE_ALL_TABLES_SQL:
            if create_table.rstrip() != '':
                redshift.run(create_table)
        
        
        
