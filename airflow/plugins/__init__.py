from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
import helpers


# Defining the plugin class
class Plugin(AirflowPlugin):
    name = "plugins"
    operators = [
        operators.CopyS3ToRedshiftOperator,
        operators.DataQualityCheckOperator,
        operators.LoadLocalFilesToS3Operator,
        operators.LoadDimFileDataOperator,
        operators.LoadFinalTableOperator
    ]
    helpers = [
        helpers.SqlQueries
    ]
