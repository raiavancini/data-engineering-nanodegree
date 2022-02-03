from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        self.log.info("Starting execution of DataQualityOperator")
        redshift_hook = PostgresHook(self.redshift_conn_id)
        
        for dq_check in self.dq_checks:
            records = redshift_hook.get_records(dq_check['check_sql'])
            
            if(dq_check['comparison'] == 'gt'):
                if len(records) < 1 or len(records[0]) < 1 or records[0][0] == dq_check['expected_result']:
                    raise ValueError("Data quality check failed. Table is empty.")

            if(dq_check['comparison'] == 'eq'):
                if len(records) < 1 or len(records[0]) < 1 or records[0][0] > dq_check['expected_result']:
                    raise ValueError("Data quality check failed. Table has NULL values.")
            
        self.log.info("Ending execution of DataQualityOperator")
        