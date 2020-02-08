from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 sql_statment="",
                 table="",
                 mode="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_statment = sql_statment
        self.table = table
        self.mode = mode

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redsift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        if self.mode == 'append-only':
            ## Mode append-only was selected ##
            self.log.info('Loading now table {}'.format(self.table))
            redshift.run(self.sql_statment)
        else:
            ## Mode delete-load was selected ##
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("TRUNCATE table {}".format(self.table))
            self.log.info('Data was cleared from table {}'.format(self.table))
            self.log.info('Loading now table {}'.format(self.table))
            redshift.run(self.sql_statment)
            
        
        
