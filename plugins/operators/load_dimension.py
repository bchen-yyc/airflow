from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 trunc_bf_insert=False,
                 sql_query='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.trunc_bf_insert = trunc_bf_insert
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info('=' * 120)
        self.log.info('LoadDimensionOperator starts')
        
        # Get row number in original table    
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records_before = redshift.get_records(f'SELECT COUNT(*) FROM {self.table}')
        
        # Empty table if trunc_bf_insert is set to True
        if self.trunc_bf_insert:
            self.log.info(f'Empty table {self.table} before insert')
            redshift.run(f'TRUNCATE TABLE {self.table}')
            
        # Load new data
        query = f'INSERT INTO {self.table} ' + self.sql_query
        redshift.run(query)
        
        # Get row number after insert and get the difference
        records_after = redshift.get_records(f'SELECT COUNT(*) FROM {self.table}')
        diff = records_after[0][0] - records_before[0][0]*(not self.trunc_bf_insert)
        self.log.info(f'Totally {diff} rows are loaded')
        self.log.info('=' * 120)
                      
