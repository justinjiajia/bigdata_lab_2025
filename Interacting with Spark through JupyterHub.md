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

Once the cluster is ready, switch to the Applications tab. Click the URL to open JupyterHub in a new browser tab:
 
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

# Add the user `livy` to the group `hdfsadmingroup` in the Linux system
# without affecting `livy`'s existing group memberships (thanks to `-a`)
# this allows writing the result of a Spark program back to HDFS
sudo usermod -aG hdfsadmingroup livy
```


- When using Jupyter (a non-Spark client), interactions with the Spark cluster are mediated through Livy, which acts as a REST server. Livy submits Spark jobs on behalf of the user.

- Livy runs under its own Linux user (typically `livy`). For it to write to HDFS, it needs appropriate permissions in HDFS.


The following code checks whether `livy` has permission to write to HDFS:

```shell
# Check groups for livy
groups livy
```
outputs:
```
livy : livy hadoop hdfsadmingroup
```

This indicates the groups that the user `livy` belongs to in the Linux system:

- `livy`: The user’s primary group (created by default when the user livy was added to the system).

- `hadoop`: A secondary group, often used for Hadoop-related services/access.

- `hdfsadmingroup`: Another secondary group (created manually to manage HDFS permissions).

We can now write data analysis results to HDFS using the following code at the end of a Spark program:

```Python
output_rdd.saveAsTextFile("hdfs:///output")
```

```shell
hdfs dfs -ls /
```
```
Found 6 items
drwxr-xr-x   - hdfs   hdfsadmingroup          0 2025-05-25 15:19 /apps
drwxr-xr-x   - hadoop hdfsadmingroup          0 2025-05-25 16:03 /input
drwxr-xr-x   - livy   hdfsadmingroup          0 2025-05-25 16:13 /output
drwxrwxrwt   - hdfs   hdfsadmingroup          0 2025-05-25 15:21 /tmp
drwxr-xr-x   - hdfs   hdfsadmingroup          0 2025-05-25 15:19 /user
drwxr-xr-x   - hdfs   hdfsadmingroup          0 2025-05-25 15:19 /var
```

- `livy`: Owner of the directory; `hdfsadmingroup`: Group associated with the directory.
  
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

This saves the Spark DataFrame as a Hive table:

 

<img width="800" alt="image" src="https://github.com/user-attachments/assets/a84f9a4e-f6e9-49e6-a583-fbf95ae4f857" />


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

<img width="800" alt="image" src="https://github.com/user-attachments/assets/53f0b024-91c7-4714-8bc5-f9e9251d66e7" />


We can also interact with this table from within the Hive shell.

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



