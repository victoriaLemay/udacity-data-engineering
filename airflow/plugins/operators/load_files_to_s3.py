from airflow.hooks.S3_hook import S3Hook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import glob
import os


class LoadLocalFilesToS3Operator(BaseOperator):
    """ LoadLocalFilesToS3Operator

    Custom Airflow Operator for loading local data files into an S3 bucket
    """

    @apply_defaults
    def __init__(self,
                 s3_conn_id="s3",
                 s3_bucket="",
                 directory="",
                 *args, **kwargs):
        """
        LoadLocalFilesToS3Operator init

        :param s3_conn_id: Name of S3 connection in Airflow
        :param s3_bucket: Name of S3 bucket (will be created if it doesn't exist)
        :param directory: Local directory where data files are located
        :param args:
        :param kwargs:
        """
        super(LoadLocalFilesToS3Operator, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_bucket = s3_bucket
        self.directory = directory

    def execute(self, context):
        """
        LoadLocalFilesToS3Operator execute

        This method finds or creates the provided S3 bucket. Files are uploaded to the S3
        bucket only if their keys are not already present.

        :param context:
        """
        s3 = S3Hook(self.s3_conn_id)

        # if bucket does not exist, create
        if not s3.check_for_bucket(self.s3_bucket):
            s3.create_bucket(self.s3_bucket, region_name='us-west-2')

        self.log.info("Loading to S3 bucket {}".format(self.s3_bucket))
        self.log.info("Checking {} directory for files".format(self.directory))

        file_list = glob.glob(self.directory + '/*')

        for file in file_list:
            self.log.info("Found file {}".format(file))

            key = os.path.basename(file)
            self.log.info("  Using S3 key {}".format(key))

            # if file is not found in bucket, upload it
            if not s3.check_for_key(key, self.s3_bucket):
                s3.load_file(file, key, self.s3_bucket)
                self.log.info("  Uploaded new S3 key {} to {} bucket".format(key, self.s3_bucket))
            else:
                self.log.info("  File already exists in bucket")

        self.log.info("S3 file load complete!")
