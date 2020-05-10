from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators
#import helpers

# Define the plugin class
class UdacityPlugin(AirflowPlugin):
	name = "udacity_plugin"
	operators = [
    operators.CreateS3BucketOperator,
    operators.UploadFileS3Operator,
    operators.CheckS3FileCount
    ]
