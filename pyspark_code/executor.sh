#!/bin/bash
echo "Script 1 started at:" `date +%r`
spark-submit --master local[*] Project2_Crio_io/pyspark_code/week_level_aggregation_of_sales_of_each_vendor.py
echo "Script 1 ended at: "  `date +%r`
#echo "Script 2 started at:" `date +%r`
#spark-submit /home/datamaking/Documents/Project2_Crio_io/pyspark_code/Distribution_of_percentage_of_trips_at_each_hour_of_day.py
#echo "Script 2 ended at: "  `date +%r`
#echo "Script 3 started at:" `date +%r`
#spark-submit sql:mysql-connector-java:8.0.31 /home/datamaking/Documents/Project2_Crio_io/pyspark_code/top_3_payment_types_users_used_in_each_day.py
#echo "Script 3 ended at: "  `date +%r`
#echo "Script 4 started at:" `date +%r`
#spark-submit sql:mysql-connector-java:8.0.31 /home/datamaking/Documents/Project2_Crio_io/pyspark_code/mySQL_load.py
#echo "Script 4 ended at: "  `date +%r`
#echo "JOB ENDED" 