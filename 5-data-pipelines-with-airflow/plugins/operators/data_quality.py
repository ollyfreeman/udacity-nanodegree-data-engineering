from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
        self,
        tests=[],
        *args, **kwargs,
    ):
        '''
        Creates an operator to test the quality of the data in Redshift

        Parameters:
            tests: a list of tests to perform, where each test has the following properties:
            - sql: a SQL statement that calculates a numerical value
            - operator: a logical operator (i.e. ==, !=, >, >=, < or <=)
            - value: a target value (such that the result of the sql statement should {operator} the value)
        '''
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tests = tests

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id='redshift')

        for test in self.tests:
            sql = test['sql']
            operator = test['operator']
            value = test['value']
            
            records = redshift_hook.get_records(sql)
            
            self.log.info(f'Data quality check: {sql} {operator} {value}')
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError('Data quality check failed - returned no results')

            result = records[0][0]

            if operator == '==':
                if not result == value:
                    raise ValueError(f'Data quality check failed: value {result}')

            if operator == '!=':
                if not result != value:
                    raise ValueError(f'Data quality check failed: value {result}')
             
            if operator == '>':
                if not result > value:
                    raise ValueError(f'Data quality check failed: value {result}')
                    
            if operator == '>=':
                if not result >= value:
                    raise ValueError(f'Data quality check failed: value {result}')
                    
            if operator == '<':
                if not result < value:
                    raise ValueError(f'Data quality check failed: value {result}')
                    
            if operator == '<=':
                if not result <= value:
                    raise ValueError(f'Data quality check failed: value {result}')

            self.log.info('Data quality check succeeded')
