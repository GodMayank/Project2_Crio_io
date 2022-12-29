from pyspark.sql import SparkSession
import subprocess

jobname = "mySQL_load"

spark = SparkSession.builder.master("local").appName(jobname).getOrCreate()

"""
	In this job we are doing
	1. Loading all 3 files from hdfs to mysql 
	2. Deleting files from hdfs.
"""

url = "jdbc:mysql://localhost:3306/source"
user = "root"
password = "datamaking"
table_list = ["week_level_aggregation_of_sales_of_each_vendor", "Distribution_of_percentage_of_trips_at_each_hour_of_day", "top_3_payment_types_users_used_in_each_day"]

for table_name in table_list:
	df = spark.read.option("header", True).option("inferSchema", True).csv("hdfs://localhost:9000/datamaking/projects/pyspark/data/input/sales/" + table_name + ".csv")
	df.write\
		.format("jdbc")\
		.mode("overwrite")\
		.option("driver", "com.mysql.cj.jdbc.Driver")\
		.option("url", url)\
		.option("dbtable", table_name)\
		.option("user", user)\
		.option("password", password)\
		.option("truncate", "true")\
		.save()
		
	hdfs_path = "hdfs://localhost:9000/datamaking/projects/pyspark/data/input/sales/" + table_name + ".csv"
	subprocess.call(["hadoop", "fs", "-rm", "-r", "-f", hdfs_path])
	
print("Job Completed: " + jobname)