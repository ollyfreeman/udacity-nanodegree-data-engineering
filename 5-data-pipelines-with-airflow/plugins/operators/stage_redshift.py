from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):

    ui_color = '#358140'

    @apply_defaults
    def __init__(
        self,
        table,
        s3_bucket,
        s3_key,
        s3_region='us-west-2',
        format_as='auto',
        stage_execution_date_only=False,
        *args, **kwargs,
    ):
        '''
        Creates an operator to load source JSON data from S3 into a staging table in Redshift

        Parameters:
            table: SQL table to be loaded into
            s3_bucket: S3 bucket of the source data
            s3_key: S3 key of the source data
            s3_region: S3 region of the source data
            s3_region: S3 region of the source data
            format_as: the schema to format the source data
            stage_execution_date_only: whether the source data should only be loaded for the hour of the execution date
        '''
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.format_as = format_as
        self.stage_execution_date_only = stage_execution_date_only


    def execute(self, context):
        aws_hook = AwsHook('aws_credentials')
        credentials = aws_hook.get_credentials()

        redshift_hook = PostgresHook(postgres_conn_id='redshift')

        self.log.info(f'Clearing data from {self.table} staging table...')
        formatted_sql = '''
            TRUNCATE TABLE {}
        '''.format(
            self.table
        )
        redshift_hook.run(formatted_sql)
        self.log.info(f'Clearing data from {self.table} staging table complete')

        self.log.info(f'Loading data into {self.table} staging table from S3...')
        s3_path = 's3://{}/{}'.format(self.s3_bucket, self.s3_key)
        
        if self.stage_execution_date_only:
            execution_date = kwargs.get('execution_date')
            s3_path = '{}/{}/{}/{}'.format(
                s3_path,
                str(execution_date.strftime('%Y')), # year
                str(execution_date.strftime('%m')), # month
                str(execution_date.strftime('%d')), # day
            )

        formatted_sql = '''
            COPY {}
            FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            FORMAT AS JSON '{}'
        '''.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.s3_region,
            self.format_as,
        )
        redshift_hook.run(formatted_sql)
        self.log.info(f'Loading data into {self.table} staging table from S3 complete')
