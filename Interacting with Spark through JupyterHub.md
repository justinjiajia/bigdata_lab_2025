### Settings

- Applications:  Hadoop, Spark, Hive, and JupyterHub
  
- 1 primary instance; type: `m4.large`

- 4 core instances; type: `m4.large`
  
    <img width="300" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/1644cc8c-d79b-4c48-a194-f5c49478d126">

- EMR release: 7.8.0

- Software configurations
```json
[
    {
        "classification":"core-site",
        "properties": {
            "hadoop.http.staticuser.user": "hadoop"
        }
    }
]
```
- Run a .sh file at `s3://ust-bigdata-class/install_python_libraries.sh` as a bootstrap action
  - Public URL: https://ust-bigdata-class.s3.us-east-1.amazonaws.com/install_python_libraries.sh
  - In the Bootstrap actions section, choose Add. Then paste the S3 uri in the pop-up window as shown below:
    <img src="https://github.com/user-attachments/assets/a41b6f5a-b543-48d8-85cf-ffed34529040" width=500/>
  - This bootstrap action will run the shell script to download and install needed Python libraries such as NumPy, Matplotlib, etc.

    
- Add a security rule that allows visititing the master node via a browser.

### Accessing JupyterHub 

Once the cluster is ready, switch to the Applications tab and copy and paste the URL for JupyterHub into the location bar of your browser.
 
<img width="800" alt="image" src="https://github.com/user-attachments/assets/7fecd488-f655-4fdd-9b14-574f669660d3" />


You may see a warning message, saying "your connection is not private". Click Advanced, and click Proceed to ec2-xx-xx-xx-xx.compute-1.amazonaws.com (unsafe).
 


JupyterHub on Amazon EMR has a default user with administrator permissions:

- Username: jovyan
- Password: jupyter 

<img width="300" alt="image" src="https://github.com/user-attachments/assets/b00830dc-6e9b-40ef-8ac3-578799cea9d4" />


Click PySpark. It will open a Juypter Notebook with PySpark already set up.  

<img width="800" alt="image" src="https://github.com/user-attachments/assets/683c9121-dc36-427d-aea5-4cfc9df5a713" />


### Data Preparation

 


```shell
#!/bin/bash

wget https://raw.githubusercontent.com/justinjiajia/datafiles/main/soc-LiveJournal1Adj.txt
wget -q https://raw.githubusercontent.com/justinjiajia/datafiles/main/flight-2015-summary.csv
wget -q https://raw.githubusercontent.com/justinjiajia/datafiles/main/Aircraft_Glossary.json.gz
wget -q https://raw.githubusercontent.com/Azure-Samples/MachineLearningSamples-Iris/master/iris.csv

hadoop fs -mkdir /input
hadoop fs -put soc-LiveJournal1Adj.txt /input
hadoop fs -put flight-2015-summary.csv /input
hadoop fs -put Aircraft_Glossary.json.gz /input
hadoop fs -put iris.csv /input
hadoop fs -df -h /input
hadoop fs -ls /input

wget -O books.csv https://raw.githubusercontent.com/justinjiajia/guide-to-data-mining/master/BX-Dump/BX-Books.csv

mkdir books

# Write the first 90000 records to books/part-1
head -n 90000 books.csv > books/part-1

# Write the next 90000 records to books/part-2
tail -n+90001 books.csv | head -n 90000 > books/part-2

# Write the remaining ones to books/part-3
tail -n+180001 books.csv > books/part-3

# Load the data to the default warehouse directory in HDFS
hdfs dfs -put books /user/hive/warehouse/
hdfs dfs -ls /user/hive/warehouse/books
```

### Spark Programming

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("yarn-client") \
                            .appName("PySpark DataFrame Example") \
                            .config("spark.driver.memory", "16g").getOrCreate()
```

```python
sc = spark.sparkContext
sc.getConf().getAll()
```

```python
spark.conf.get("spark.sql.warehouse.dir")
spark.conf.get("spark.submit.deployMode")
```


```
'hdfs://ip-172-31-72-16.ec2.internal:8020/user/spark/warehouse'
'cluster'
```

```python
flight_df = spark.read.options(header=True, inferSchema=True).csv('hdfs:///input/flight-2015-summary.csv')
flight_df = flight_df.withColumnRenamed('DEST_COUNTRY_NAME', 'destination') \
                     .withColumnRenamed('ORIGIN_COUNTRY_NAME', 'origin') \
                     .withColumnRenamed('COUNT', 'count')

from pyspark.sql.functions import avg
flight_df.groupBy('origin').agg(avg('count').alias('outbound_avg')).show(50)
```

```python
flight_df.write.saveAsTable('flight_table')
spark.catalog.listDatabases()
spark.catalog.listTables()

```
![image](https://github.com/user-attachments/assets/a84f9a4e-f6e9-49e6-a583-fbf95ae4f857)

Save as a Hive table.


```python
# List databases to confirm Hive connectivity
spark.sql("SHOW DATABASES").show()
spark.sql("SHOW TABLES").show()

# Read the Hive table
df = spark.sql("SELECT * FROM default.flight_table")

# Filter and process data
from pyspark.sql.functions import col
df.filter(col("count") > 1000).show()
```


<img width="459" alt="image" src="https://github.com/user-attachments/assets/d644f8d8-9114-430e-849e-b532e69a9050" />


```python
spark.sql("""CREATE EXTERNAL TABLE IF NOT EXISTS books (ISBN string, title string, author string,
                                                        publicationyear string, publisher string, imgURLs string, imgURLm string, ImgURLl string)
             row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties ('separatorChar' = ';', 'quoteChar'= '\"')
             LOCATION '/user/hive/warehouse/books';""")
spark.sql("SHOW TABLES").show()
spark.sql("drop table if exists books;")
```
<img width="908" alt="image" src="https://github.com/user-attachments/assets/53f0b024-91c7-4714-8bc5-f9e9251d66e7" />


```shell
hive -hiveconf hive.execution.engine='tez'
```

```shell
hive> SELECT publicationyear, COUNT(title) FROM books GROUP BY publicationyear LIMIT 5;
Query ID = hadoop_20250511171138_d42299d0-ea92-4f63-b8dc-33ae1ccf3aa0
Total jobs = 1
Launching Job 1 out of 1
Status: Running (Executing on YARN cluster with App id application_1746975498257_0004)

----------------------------------------------------------------------------------------------
        VERTICES      MODE        STATUS  TOTAL  COMPLETED  RUNNING  PENDING  FAILED  KILLED  
----------------------------------------------------------------------------------------------
Map 1 .......... container     SUCCEEDED      6          6        0        0       0       0  
Reducer 2 ...... container     SUCCEEDED      4          4        0        0       0       0  
----------------------------------------------------------------------------------------------
VERTICES: 02/02  [==========================>>] 100%  ELAPSED TIME: 13.78 s    
----------------------------------------------------------------------------------------------
OK
1897	1
1900	3
1904	1
1910	1
1911	5
Time taken: 14.285 seconds, Fetched: 5 row(s)
hive> quit;
[hadoop@ip-172-31-72-10 ~]$ 
``` 


### Experiment 1

How to modify a spark application with custom configurations: https://repost.aws/knowledge-center/modify-spark-configuration-emr-notebook

In a Jupyter notebook cell, run the `%%configure` command with desired configurations:

```python
%%configure -f
{"conf": {
    "spark.dynamicAllocation.executorIdleTimeout": "5m"}  
}
```
For example, we may want to increase executors' idle timeout. Otherwise, executors will be automatically removed after 1 minute.

<img width="800" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/fde09276-dc92-45cf-9cad-ac957890cb52">

Running any code will start a new application on YARN with the custom configurations.


<img width="1011" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/e05f75f2-880c-4199-98e9-4952cb57ba02">


<img width="1011" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/4bbb92ac-67d7-4912-90f8-597c054786f3">

<img width="1011" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/d066c0d1-a265-4228-8129-acf97887b71d">


| Instance ID | Instance Type | Software Entities | No. of Containers |
| ------------- |-------------| ------------- | ------------- |
| ip-xxxx-63-62  | core | driver (0 core; 1G mem) | 1 (1 vCore; 2.38G mem) |
| ip-xxxx-54-228  | core | executor 1  (4 cores; 2G mem)| 1 (1 vCore; 4.97G mem)|
| ip-xxxx-48-235  | core |  executor 2 (4 cores; 2G mem) | 1 (1 vCore; 4.97G mem)|
| ip-xxxx-58-45  | core |  executor 3 (4 cores; 2G mem)| 1 (1 vCore; 4.97G mem)|


Note that the driver process now is started on a core instance. And there's no application master displayed on this page (no **Miscellaneous process** section).

> A Jupyter notebook uses the Sparkmagic kernel as a client for interactively working with Spark in a remote EMR cluster through an Apache Livy server.  https://repost.aws/knowledge-center/modify-spark-configuration-emr-notebook


Later, running the configuration cell every time will launch a new application with new configurations.

<br>

### Experiment 2


```python
%%configure -f
{"conf": {
    "spark.executor.cores": "2", 
    "spark.dynamicAllocation.executorIdleTimeout": "5m"} 
}
```


<img width="800" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/eb020ee4-7978-48b7-9d78-0d85ec297894">

<img width="1011" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/da36ada8-af96-4d27-bb69-d41530c33f20">
<img width="1011" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/1d384489-6d97-43a2-80c5-23838923f4c7">

| Instance ID | Instance Type | Software Entities | No. of Containers |
| ------------- |-------------| ------------- | ------------- |
| ip-xxxx-58-45  | core | driver (0 core; 1G mem) | 1 (1 vCore; 2.38G mem)|
| ip-xxxx-48-235  | core | executor 1  (2 cores; 2G mem)| 1 (1 vCore; 4.97G mem)|
| ip-xxxx-54-228  | core |  executor 2 (2 cores; 2G mem) | 1 (1 vCore; 4.97G mem)|
| ip-xxxx-63-62  | core |  executor 3 (2 cores; 2G mem)| 1 (1 vCore; 4.97G mem)|


<br>

### Experiment 3


```python
%%configure -f
{"conf": {
    "spark.executor.cores": "3", 
    "spark.executor.memory": "2g",
    "spark.dynamicAllocation.executorIdleTimeout": "5m"}
}
```

<img width="874" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/d8d6f227-1995-4538-9274-60571fcb8076">

<img width="1404" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/e59d0ecf-c538-4259-b534-f57c4eb578ac">

<img width="1405" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/68367571-16ef-409b-920c-f7c877342be7">

<img width="1429" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/9a2ebe4b-4c10-4c54-b0c8-03897ecf4a32">

| Instance ID | Instance Type | Software Entities | No. of Containers |
| ------------- |-------------| ------------- | ------------- |
| ip-xxxx-58-45  | core | driver (0 core; 1G mem) & executor 3 (3 cores, 912M mem) | 2 |
| ip-xxxx-48-235  | core | executors 4 & 5 (3 cores, 912M mem each)| 2  |
| ip-xxxx-54-228  | core |  executors 1 & 2 (3 cores, 912M mem each) | 2  |
| ip-xxxx-63-62  | core |  executors 6 & 7 (3 cores, 912M mem each)| 2  |

After all executors die out, it can be verified that the memory allocated to the container for driver is still 2.38G.


<br>

### Observations

To summarize:
```
%%configure -f
{"conf": {
    "spark.executor.instances": "6",     # does't take effect
    "spark.executor.cores": "3",         # take effect
    "spark.executor.memory": "2g",       # take effect; can affect the actual no. of executors
    "spark.dynamicAllocation.executorIdleTimeout": "5m"}  # take effect
}
```

It seems that a spark application created in this way can only run in cluster mode.

`'spark.submit.deployMode'` defaults to `'cluster'` (`sc.getConf().get('spark.submit.deployMode')`) and seems to be unmodifiable?




<img width="800" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/45703223-2ec8-4117-a9f5-d7d5ff38e135">

<img width="800" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/3cfb6080-5e9b-4b7e-a71b-2051e5db9f5a">

This also explains why the driver process runs on a worker machine. 

https://spark.apache.org/docs/latest/configuration.html




