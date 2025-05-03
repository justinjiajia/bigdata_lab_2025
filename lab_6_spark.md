# EMR settings

- EMR release: 7.8.0 

-  ![#f03c15](https://placehold.co/15x15/f03c15/f03c15.png) IMPORTANT: Application: Hadoop, Spark, and Hive
  
- Primary instance: type: `m4.large`, quantity: 1

- Core instance: type: `m4.large`, quantity: 3
  
- Software configurations
    ```json
    [
      {
        "classification":"core-site",
        "properties": {
          "hadoop.http.staticuser.user": "hadoop"
        }
      },
      {
        "classification": "hive-site",
        "properties": {
          "hive.execution.engine": "mr"
        }
      }
    ]
    ```
    
- Make sure the primary node's EC2 security group has a rule allowing for "ALL TCP" from "My IP" and a rule allowing for "SSH" from "Anywhere".

<br>

# 1 Data Preparation

```shell
nano script.sh
```

Copy and paste the code snippet below into the *script.sh* file. 

```shell
#!/bin/bash
wget -O transactions.txt  https://raw.githubusercontent.com/justinjiajia/datafiles/main/browsing.csv
hadoop fs -mkdir -p /<Your ITSC Account>/input
hadoop fs -put transactions.txt /<Your ITSC Account>/input
hadoop fs -df -h /<Your ITSC Account>
```

```shell
bash script.sh
```



# 2 Run PySpark on Yarn via shell

## Start the shell

```shell
pyspark --master yarn --deploy-mode client
```
> Note: Spark shell can only be started in client mode.

## PySpark code to run sequentially

```python
transactions = sc.textFile("hdfs:///justinjia/input/transactions.txt")  # absolute path of the input file on HDFS

from itertools import combinations

pairs = transactions.map(lambda x: x.strip().split(" ")).flatMap(lambda x: combinations(x, 2)).map(lambda x: (x[0], x[1]) if x[0] <= x[1] else (x[1], x[0]))

pairs_count = pairs.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)

pairs_count.cache()

combined_pairs = pairs_count.flatMap(lambda x: [(x[0][0], [(x[0][1], x[1])]), (x[0][1], [(x[0][0], x[1])])])

rec_pairs_ordered = combined_pairs.reduceByKey(lambda x, y: sorted(x+y, key=lambda val: val[1], reverse=True)[:5])

rec_pairs_ordered.saveAsTextFile("hdfs:///<Your ITSC Account>/output")  # absolute path of the output directory file on HDFS

quit()
```


## Print the output

```shell
hadoop fs -cat /<Your ITSC Account>/output/part-* | grep "ELE96863"
```

<br>

# 3 Launch PySpark applications with `spark-submit`

## Create a PySpark script file

```shell
nano recommendation.py
```

## Code to paste into the file

```python
from pyspark.sql import SparkSession
from itertools import combinations

spark = SparkSession.builder.appName("recommendation").getOrCreate()
sc = spark.sparkContext

transactions = sc.textFile("hdfs:///<Your ITSC Account>/input/transactions.txt")
transactions.cache()
pairs = transactions.map(lambda x: x.strip().split(" ")).flatMap(lambda x: combinations(x, 2)).map(lambda x: (x[0], x[1]) if x[0] <= x[1] else (x[1], x[0]))
pairs_count = pairs.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y)
combined_pairs = pairs_count.flatMap(lambda x: [(x[0][0], [(x[0][1], x[1])]), (x[0][1], [(x[0][0], x[1])])])
rec_pairs_ordered = combined_pairs.reduceByKey(lambda x, y: sorted(x+y, key=lambda val: val[1], reverse=True)[:5])
rec_pairs_ordered.saveAsTextFile("hdfs:///<Your ITSC Account>/output_1")
```

## Submit the job

```shell
spark-submit --master yarn recommendation.py
```

> Check out [this page](spark-submit_options.md) for more launch options 



## Print the output

```shell
hadoop fs -cat /<Your ITSC Account>/output_1/part-* | grep "ELE96863"
```

<br>

# Reference

https://spark.apache.org/docs/latest/submitting-applications

https://github.com/justinjiajia/bigdata_lab/blob/main/spark-submit_options.md
