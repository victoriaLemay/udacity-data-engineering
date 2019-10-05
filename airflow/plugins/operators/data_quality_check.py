from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityCheckOperator(BaseOperator):
    """ DataQualityCheckOperator

    Custom Airflow Operator for performing data quality checks on Redshift tables
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 tables={},
                 check_functions=[],
                 *args, **kwargs):
        """
        DataQualityCheckOperator init

        :param redshift_conn_id: Name of Redshift connection in Airflow
        :param tables: Dictionary of tables (in format {table : column})
        :param check_functions: List of data quality methods to check against
        :param args:
        :param kwargs:
        """
        super(DataQualityCheckOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.check_functions = check_functions

    def execute(self, context):
        """
        DataQualityCheckOperator execute

        Run each method listed in the check_functions parameter against all of the tables
        listed in the tables dictionary parameter

        :param context:
        """
        redshift_hook = PostgresHook(self.redshift_conn_id)

        for check_function in self.check_functions:
            getattr(self, check_function)(redshift_hook)

        self.log.info('Data quality check complete')

    def has_nulls(self, redshift_hook):
        """
        DataQualityCheckOperator has_nulls

        Check whether there are NULL values found in a specified table column

        :param redshift_hook:
        """
        for table, column in self.tables.items():
            self.log.info("Checking data in {} column of {} table".format(column, table))

            records = redshift_hook.get_records("SELECT COUNT(*) FROM {} WHERE {} IS NULL".format(table, column))

            if len(records) > 0:
                self.log.info("Found {} NULL records".format(records))
                raise ValueError("NULL records were found for the {} column".format(column))

            self.log.info("Found no NULL records for the {} column in the {} table".format(column, table))

    def has_zero_rows(self, redshift_hook):
        """
        DataQualityCheckOperator has_zero_rows

        Check whether a specified table has rows

        :param redshift_hook:
        """
        for table, column in self.tables.items():
            self.log.info("Checking data in {} column of {} table".format(column, table))

            records = redshift_hook.get_records("SELECT COUNT({}) FROM {}".format(column, table))

            if len(records) == 0:
                self.log.info("Found 0 records in {}".format(table))
                raise ValueError(
                    "Zero records were found for the {} column in the {} table".format(column, table))

            self.log.info("Found records in {}".format(table))
