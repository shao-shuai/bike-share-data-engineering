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

def process_station_data(spark, input_data, output_data):
	station_data = os.path.join(input_data, 'station_data/*.csv')

	df = spark.read.csv(station_data, header=True)

	# Extract columns to create staion table
	station_table = df.select(
    col('short_name').alias('station_short_name'),
    col('external_id').alias('station_external_id'),
    col('has_kiosk').alias('station_has_kiosk'),
    col('rental_methods').alias('station_rental_methods'),
    col('capacity').alias('station_capacity'),
    col('region_id').alias('station_region_id'),
    col('station_type').alias('station_type'),
    col('name').alias('station_name'),
    col('lon').alias('station_longitude'),
    col('station_id').alias('station_id'),
    col('lat').alias('station_latitude'),
    col('weather_station_id')
    
)

	station_table.show(5)

	# Write station table
	station_table.write.parquet(os.path.join(output_data, 'stations'), 'overwrite')

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

	process_station_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
