from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
        self,
        table,
        sql_select_query,
        mode='delete-load',
        *args, **kwargs,
    ):
        '''
        Creates an operator to load data into a dimension table in Redshift

        Parameters:
            table: SQL table to be loaded into
            sql_select_query: SQL query to select the data to be inserted
            mode: The mode of the load: 'append-only' or 'delete-load'
        '''
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_select_query = sql_select_query
        self.mode = mode

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id='redshift')

        if self.mode == 'delete-load':
          self.log.info(f'Clearing data from {self.table} dimension table...')
          formatted_sql = '''
              TRUNCATE TABLE {}
          '''.format(
              self.table
          )
          redshift_hook.run(formatted_sql)
          self.log.info(f'Clearing data from {self.table} dimension table complete')
        
        self.log.info(f'Loading data into {self.table} dimension table...')
        formatted_sql = '''
            INSERT INTO {}
            {};
        '''.format(
            self.table,
            self.sql_select_query
        )
        redshift_hook.run(formatted_sql)
        self.log.info(f'Loading data into {self.table} dimension table complete')
