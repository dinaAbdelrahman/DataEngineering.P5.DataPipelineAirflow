from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 aws_credentials_id="",
                 redshift_conn_id="",
                 tables=[],
                 min_records=1,
                 sql_check="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.tables = tables
        self.min_records = min_records
        self.sql_check = sql_check
        

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        
        redsift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Running Quality checks on the tables {}".format(self.tables))
        
        for table in self.table:
            self.log.info("Checking table: {}".format(table))
            records = redshift_hook.get_records(f"{self.sql_check} {table}")
            num_records=records[0][0]
            if len(records) < self.min_records or len(records[0]) < self.min_records:
                raise ValueError(f"Data quality check failed. {table} returned no results")
            elif num_records < self.min_records:
                raise ValueError(f"Data quality check failed. {table} contains {num_records} row")
            else:
                self.log.info(f"Data quality on table {table} check passed with {records[0][0]} records")
                
                
            
            
            