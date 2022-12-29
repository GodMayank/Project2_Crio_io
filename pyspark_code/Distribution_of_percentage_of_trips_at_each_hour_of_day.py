from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql.functions import hour

jobname = "Distribution_of_percentage_of_trips_at_each_hour_of_day_py2"

spark = SparkSession.\
	builder.\
	master("local").\
	appName(jobname).\
	getOrCreate()
"""
	In this job we are:
	1. reading an orc file from hadoop 
	2. doing transformation to find percentage of trips at each hours a day
	3. saving the output back to hdfs.
	4. Later we will laod this output to mysql using pyspark script.
"""

df = spark.read.orc("hdfs://localhost:9000/datamaking/projects/pyspark/data/input/sales/yellow_taxi_dataset.orc")
df1 = df.withColumn("tpep_dropoff_date", to_date("tpep_dropoff_datetime")).withColumn("hours", hour(col("tpep_dropoff_datetime")))
df2 = df1.groupBy("tpep_dropoff_date").sum("trip_distance").orderBy("tpep_dropoff_date").withColumnRenamed("sum(trip_distance)", "sum_trip_distance_per_day").orderBy("tpep_dropoff_date")
df3 = df1.groupBy("tpep_dropoff_date", "hours").sum("trip_distance").orderBy("tpep_dropoff_date").withColumnRenamed("sum(trip_distance)", "sum_trip_distance_per_hour_per_day").orderBy("tpep_dropoff_date", "hours")
df4 = df2.join(df3, df2.tpep_dropoff_date == df3.tpep_dropoff_date, "left").drop(df3.tpep_dropoff_date).withColumn("trip_each_hour_per", col("sum_trip_distance_per_day")/col("sum_trip_distance_per_hour_per_day")).select("tpep_dropoff_date", "hours", "sum_trip_distance_per_day", "sum_trip_distance_per_hour_per_day", "trip_each_hour_per").orderBy("tpep_dropoff_date", "hours")
df4.write.option("header", True).mode("overwrite").csv("hdfs://localhost:9000/datamaking/projects/pyspark/data/input/sales/Distribution_of_percentage_of_trips_at_each_hour_of_day.csv")

print("Job Completed: " + jobname)
