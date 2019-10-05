from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class CopyS3ToRedshiftOperator(BaseOperator):
    """ CopyS3ToRedshiftOperator

    Custom Airflow Operator for copying data from an S3 bucket into a Redshift table
    """

    copy_sql = """
        COPY {} 
        FROM 's3://{}/{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """

    parquet_copy_sql = """
        COPY {}
        FROM 's3://{}/'
        IAM_ROLE '{}'
        FORMAT AS PARQUET
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 aws_credentials_id="aws_credentials",
                 parquet_arn="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 copy_format="",
                 truncate=True,
                 *args, **kwargs):
        """
        CopyS3ToRedshiftOperator init

        :param redshift_conn_id: Name of Redshift connection in Airflow
        :param aws_credentials_id: Name of AWS IAM Credentials connection in Airflow
        :param parquet_arn: Name of AWS IAM ARN connection in Airflow
        :param table: Name of table to load data into
        :param s3_bucket: Name of S3 bucket where data files are stored
        :param s3_key: Name of S3 key of data file to load in
        :param copy_format: SQL string for copy format (CSV or JSON)
        :param truncate: Determines whether table should be truncated before inserts
        :param args:
        :param kwargs:
        """
        super(CopyS3ToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.parquet_arn = parquet_arn
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.copy_format = copy_format
        self.truncate = truncate

    def execute(self, context):
        """
        CopyS3ToRedshiftOperator execute

        Loads a data file in JSON or CSV format, or a directory of Parquet files, from
        an S3 bucket into a Redshift table. Use the truncate_table flag (True or False)
        to remove data from the table before inserting.

        :param context:
        """
        self.log.info("Using data file {} from S3 bucket {}".format(self.s3_key, self.s3_bucket))
        self.log.info("Inserting into destination Redshift table {}".format(self.table))

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            redshift.run("TRUNCATE TABLE {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")

        if not self.parquet_arn:
            formatted_sql = CopyS3ToRedshiftOperator.copy_sql.format(
                self.table,
                self.s3_bucket,
                self.s3_key,
                credentials.access_key,
                credentials.secret_key,
                self.copy_format
            )
        else:
            formatted_sql = CopyS3ToRedshiftOperator.parquet_copy_sql.format(
                self.table,
                self.s3_bucket,
                self.parquet_arn
            )

        redshift.run(formatted_sql)

        self.log.info("Data copy complete")


