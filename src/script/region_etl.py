import configparser
from datetime import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
from pyspark.sql.functions import year
from pyspark.sql.functions import month
from pyspark.sql.functions import dayofyear
from pyspark.sql.functions import dayofweek
from pyspark.sql.functions import dayofmonth
from pyspark.sql.functions import weekofyear
from pyspark.sql.functions import hour
from pyspark.sql.functions import col

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark

def process_region_data(spark, input_data, output_data):
	region_data = os.path.join(input_data, 'region_data/*.csv')

	df = spark.read.csv(region_data, header=True)

	df.show(5)

	# Write region table
	df.write.parquet(os.path.join(output_data, 'regions'), 'overwrite')

def main():
	if len(sys.argv) == 3:
		input_data = sys.argv[1]
		output_data = sys.argv[2]

	else:
		config = configparser.ConfigParser()
		config.read('../../config.cfgs')
		os.environ['AWS_ACCESS_KEY_ID'] = config['AWS']['AWS_ACCESS_KEY_ID']
		os.environ['AWS_SECRET_ACCESS_KEY'] = config['AWS']['AWS_SECRET_ACCESS_KEY']
		input_data = 's3a://' + config['S3']['RAW_DATALAKE_BUCKET'] + '/'
		output_data = 's3a://' + config['S3']['ACCIDENTS_DATALAKE_BUCKET'] + '/'

	spark = create_spark_session()

	process_region_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
