from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_statment="",
                 table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_statment = sql_statment
        self.table = table

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redsift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info('Loading now table {}'.format(self.table))
        redshift.run(self.sql_statment)
        
