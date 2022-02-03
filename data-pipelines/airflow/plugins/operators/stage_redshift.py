from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    s3_copy = """
        copy {}
        from '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        region 'us-west-2'
        json '{}';
    """
    
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 table,
                 s3_path,
                 json_path,
                 redshift_conn_id='redshift',
                 aws_credentials_id="aws_credentials",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_path = s3_path
        self.json_path = json_path
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        self.log.info("Starting execution of StageToRedshiftOperator")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        
        redshift_hook = PostgresHook(self.redshift_conn_id)
        redshift_hook.run(StageToRedshiftOperator.s3_copy.format(self.table, self.s3_path, credentials.access_key, credentials.secret_key, self.json_path))
        self.log.info("Ending execution of StageToRedshiftOperator")
        