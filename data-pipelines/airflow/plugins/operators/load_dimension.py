from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 table,
                 sql_query,
                 redshift_conn_id='redshift',
                 truncate_insert=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id
        self.truncate_insert = truncate_insert

    def execute(self, context):
        self.log.info("Starting execution of LoadDimensionOperator")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate_insert:
            self.log.info(f"Truncate-Insert option was selected for table {self.table}")
            redshift_hook.run("TRUNCATE TABLE {}".format(self.table))
            self.log.info(f"Table {self.table} was truncated")
        
        query = self.sql_query.format(self.table)
        redshift_hook.run(query)
        self.log.info(f"{self.table} was properly populated")
        self.log.info("Ending execution of LoadDimensionOperator")
        