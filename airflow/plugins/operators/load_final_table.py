from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFinalTableOperator(BaseOperator):
    """ LoadFinalTableOperator

    Custom Airflow Operator for loading data in Redshift from staging tables into final tables
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='redshift',
                 table='',
                 columns=[],
                 select_sql='',
                 truncate_table=False,
                 *args, **kwargs):
        """
        LoadFinalTableOperator init

        :param redshift_conn_id: Name of Redshift connection in Airflow
        :param table: Name of final table to load data into
        :param columns: Name of columns covered by select_sql query
        :param select_sql: Select query for gathering data
        :param truncate_table: Determines whether table should be truncated before inserts
        :param args:
        :param kwargs:
        """
        super(LoadFinalTableOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns
        self.select_sql = select_sql
        self.truncate_table = truncate_table

    def execute(self, context):
        """
        LoadFinalTableOperator execute

        Loads data from a staging table to a final table in Redshift, using a
        provided SQL SELECT statement. Use the truncate_table flag (True or False)
        to remove data from the final table before inserting.

        :param context:
        """
        self.log.info('Loading data into {} table'.format(self.table))
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if self.truncate_table:
            self.log.info("Truncating {} table before inserting data".format(self.table))
            redshift_hook.run("TRUNCATE TABLE {}".format(self.table))

        columns = ''
        if self.columns:
            columns = '(' + ", ".join(self.columns) + ')'
            self.log.info("Using columns: {}".format(", ".join(self.columns)))

        redshift_hook.run("INSERT INTO {} {} {}".format(self.table, columns, self.select_sql))

        self.log.info('Data load complete')
