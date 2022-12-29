
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
from pyspark.sql.functions import col
from pyspark.sql import Window
from pyspark.sql.functions import rank

jobname = "top_3_payment_types_users_used_in_each_day_py3"

spark = SparkSession.\
        builder.\
        master("local").\
        appName(jobname).\
        getOrCreate()

"""
	In this job we are doing
	1. Reading an orc file from hdfs.
	2. doing transformation to find top 3 payment type used by user each day.
	3. reading lookup file from mysql.
	4. saving the final output back to hdfs after applying lookup.
	5. Later the final output will be loaded to mysql table using a pyspark script.
"""


df = spark.read.orc("hdfs://localhost:9000/datamaking/projects/pyspark/data/input/sales/yellow_taxi_dataset.orc")
df1 = df.withColumn("tpep_pickup", to_date("tpep_pickup_datetime"))
df2 = df1.groupBy("tpep_pickup", "payment_type").count().orderBy(col("tpep_pickup").asc(), col("count").desc())
window = Window.partitionBy(df2['tpep_pickup']).orderBy(df2['count'].desc())
df3 = df2.select('*', rank().over(window).alias('rank_')).filter(col('rank_')<=3)
mysql_jdbc_url='jdbc:mysql://localhost:3306/source'
table='payment_otc'
user='root'
password='datamaking'
df_table = spark.read.format("jdbc")\
	.option("url", mysql_jdbc_url)\
	.option("driver", "com.mysql.jdbc.Driver")\
	.option("dbtable", table)\
	.option("user", user)\
	.option("password", password).load()
df_result = df3.join(df_table, df3.payment_type == df_table.payment_type, "inner").drop(df_table.payment_type).orderBy("tpep_pickup", "rank_")
df_result.write.option("header", True).mode("overwrite").csv("hdfs://localhost:9000/datamaking/projects/pyspark/data/input/sales/top_3_payment_types_users_used_in_each_day.csv")

print("Job Completed: " + jobname)
