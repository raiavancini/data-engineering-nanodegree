from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table,
                 sql_query,
                 redshift_conn_id='redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_query = sql_query
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        self.log.info("Starting execution of LoadFactOperator")
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        query = self.sql_query.format(self.table)
        redshift_hook.run(query)
        self.log.info(f"{self.table} was properly populated")
        self.log.info("Ending execution of LoadFactOperator")
        