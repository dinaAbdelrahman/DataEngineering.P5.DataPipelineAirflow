from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    copy_sql="""
        COPY {}
        FROM {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        json '{}' 
              
    """
  
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 file_format="JSON",
                 json_path="",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region
        self.file_format = file_format
        self.json_path = json_path
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        """
            Copy the Data from S3 to redshift, parameters should be passed from dag runtime
                -redshift_conn_id : parameters of the redshift connection
                -aws_credentials_id : IAM role credentials to use AWS
                -table: the table name used to copy into it the S3 files
                -s3_bucket: the S3 bucket containings the files
                -s3_key: the subfolder inside the S3 bucket to load either song or log files
                -region: the region where we setup in it the redshift cluster
                -file_format: JSON files but optional can be other format
                -execution_date: passed from the kwargs
            
        """
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redsift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
           
        
        self.log.info("Copying Data from S3 to Redsfift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )
        redshift.run(formatted_sql)





