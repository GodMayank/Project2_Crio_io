from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import weekofyear
from pyspark.sql.functions import col

jobname = "week_level_aggregation_of_sales_of_each_vendor_py1"

spark = SparkSession.\
	builder.\
	master('local').\
	appName(jobname).\
	getOrCreate()
"""
	In this script we are doing the following things 
	1. reading one parquet file from our local.
	2. coverting that file into orc format and writing the output into hdfs
	3. Doing the required transformation using the hdfs orc file.
	4. Saving the output back into hdfs.
	5. Later other script will load this output to mysql table.
"""

df = spark.read.parquet("/home/datamaking/Documents/Project2_Crio_io/yellow_tripdata_2022-01.parquet")
df.printSchema()
df.write.format('orc').mode('overwrite').save('hdfs://localhost:9000/datamaking/projects/pyspark/data/input/sales/yellow_taxi_dataset.orc')
df1 = spark.read.orc("hdfs://localhost:9000/datamaking/projects/pyspark/data/input/sales/yellow_taxi_dataset.orc")
data_df1 = df1.withColumn('week_of_year', weekofyear(col('tpep_pickup_datetime')))
data_df2 =  data_df1.groupBy("VendorID","week_of_year").sum("total_amount", "trip_distance").withColumnRenamed("sum(total_amount)", "sum_total_amount").withColumnRenamed("sum(trip_distance)", "sum_trip_distance").orderBy("VendorID")
data_df3 = data_df2.withColumn("amount_per_distance", lit(data_df2.sum_total_amount/data_df2.sum_trip_distance))
data_df3.write.option("header", True).mode("overwrite").csv("hdfs://localhost:9000/datamaking/projects/pyspark/data/input/sales/week_level_aggregation_of_sales_of_each_vendor.csv")
data_df3.printSchema()
print("Job Completed" + jobname)


