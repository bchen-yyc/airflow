from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import operator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dq_checks='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        ops = {
            '=': operator.eq,
            '>': operator.gt,
            '<': operator.lt
        }
        self.log.info("=" * 100)
        self.log.info('DataQualityOperator starts')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_queries = []
        for dq_check in self.dq_checks:
            records = redshift.get_records(dq_check['check_sql'])
            
            # Convert '=' / '>' to operator functions
            ops_func = ops[dq_check['comparison']]
            if not ops_func(records[0][0], dq_check['expected_result']):
                error_queries.append(dq_check['check_sql'])
            
            if error_queries:
                for query in error_queries:
                    self.log.info(f'Query {query} failed!')
                raise ValueError('Data quality check failed!')
            else:
                self.log.info('Data quality check is successful.')
            
        self.log.info("=" * 100)