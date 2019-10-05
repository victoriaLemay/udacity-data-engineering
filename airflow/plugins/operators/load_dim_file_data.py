from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimFileDataOperator(BaseOperator):
    """ LoadDimFileDataOperator

    Custom Airflow Operator for loading dimension tables from files in an S3 bucket
    """

    copy_sql = """
        COPY {} 
        FROM 's3://{}/{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_format="",
                 *args, **kwargs):
        """
        LoadDimFileDataOperator init

        :param redshift_conn_id: Name of Redshift connection in Airflow
        :param aws_credentials_id: Name of AWS IAM Credentials connection in Airflow
        :param table: Name of table to load data into
        :param s3_bucket: Name of S3 bucket where data files are stored
        :param s3_key: Name of S3 key of data file to load in
        :param copy_format: SQL string for copy format (CSV or JSON)
        :param args:
        :param kwargs:
        """
        super(LoadDimFileDataOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_format = copy_format

    def execute(self, context):
        """
        LoadDimFileDataOperator execute

        Loads a data file in JSON or CSV format from an S3 bucket into a Redshift table. This
        operator will only load data into a table if it is empty.

        :param context:
        """
        self.log.info("Using data file {} from S3 bucket {}".format(self.s3_key, self.s3_bucket))

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # only copy data if dimension table is empty
        records = redshift.get_records("SELECT COUNT(*) FROM {}".format(self.table))
        num_records = records.pop(0)[0]

        if num_records == 0:
            self.log.info("Inserting into destination Redshift table {}".format(self.table))

            formatted_sql = LoadDimFileDataOperator.copy_sql.format(
                self.table,
                self.s3_bucket,
                self.s3_key,
                credentials.access_key,
                credentials.secret_key,
                self.copy_format
            )
            redshift.run(formatted_sql)
        else:
            self.log.info("Found {} records in {}".format(num_records, self.table))
            self.log.info("{} table data is already set".format(self.table))

        self.log.info("Data copy complete")


