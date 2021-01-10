from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
        self,
        table,
        sql_select_query,
        *args, **kwargs,
    ):
        '''
        Creates an operator to load data into a fact table in Redshift

        Parameters:
            table: SQL table to be loaded into
            sql_select_query: SQL query to select the data to be inserted
        '''
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.sql_select_query = sql_select_query

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id='redshift')

        self.log.info(f'Loading data into {self.table} fact table...')
        formatted_sql = '''
            INSERT INTO {}
            {};
        '''.format(
            self.table,
            self.sql_select_query
        )
        redshift_hook.run(formatted_sql)
        self.log.info(f'Loading data into {self.table} dimension table complete')
