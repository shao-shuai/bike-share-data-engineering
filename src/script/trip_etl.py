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

def process_trip_data(spark, input_data, output_data):
	trip_data = os.path.join(input_data, 'trip_data/*.csv')

	df = spark.read.csv(trip_data, header=True)

	df = df.where(col("start_station_id").isNotNull()) \
		.where(col("end_station_id").isNotNull()) \
		.select(['duration_sec', 'start_time', 'end_time', 'start_station_id', 'end_station_id', 'bike_id', 'user_type'])

	df.show(10)

	# Create start_time_table
	start_time_table = df.select(["start_time"]) \
            .withColumn("start_year", year("start_time")) \
            .withColumn("start_month", month("start_time")) \
            .withColumn("start_dayofyear", dayofyear("start_time")) \
            .withColumn("start_dayofmonth", dayofmonth("start_time")) \
            .withColumn("start_week", weekofyear("start_time")) \
            .withColumn("start_dayofweek", dayofweek("start_time")) \
            .withColumn("start_hour", hour("start_time"))

	start_time_table.show(5)

	# Create end_time_table
	end_time_table = df.select(["end_time"]) \
            .withColumn("end_year", year("end_time")) \
            .withColumn("end_month", month("end_time")) \
            .withColumn("end_dayofyear", dayofyear("end_time")) \
            .withColumn("end_dayofmonth", dayofmonth("end_time")) \
            .withColumn("end_week", weekofyear("end_time")) \
            .withColumn("end_dayofweek", dayofweek("end_time")) \
            .withColumn("end_hour", hour("end_time"))

	end_time_table.show(5)   
	
	# Write trip talbe
	df.write.partitionBy(['start_station_id']).parquet(os.path.join(output_data, 'trips'), 'overwrite')     

	# Write start time table
	start_time_table.write.partitionBy(['start_year', 'start_month']).parquet(os.path.join(output_data, 'start_time'), 'overwrite')

	# Write end time table
	end_time_table.write.partitionBy(['end_year', 'end_month']).parquet(os.path.join(output_data, 'end_time'), 'overwrite')

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

	process_trip_data(spark, input_data, output_data)

if __name__ == "__main__":
    main()
