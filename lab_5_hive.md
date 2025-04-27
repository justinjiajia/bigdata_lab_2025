# EMR settings

- EMR release: 7.8.0 

- ![#f03c15](https://placehold.co/15x15/f03c15/f03c15.png) IMPORTANT: Application: Hadoop, Hive, and Tez
  
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
            "classification": "hdfs-site",
            "properties": {
                "dfs.block.size": "16777216",
                "dfs.replication": "3"
            }
        },
        {
            "classification": "mapred-site",
            "properties": {
                "mapreduce.job.reduces": "3"
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



# Data preparation

```shell
nano script.sh
```

Copy and paste the code snippet below into the *script.sh* file. 


```shell
#!/bin/bash

# A .csv file that contains 271,379 book records
wget -O books.csv https://raw.githubusercontent.com/justinjiajia/guide-to-data-mining/master/BX-Dump/BX-Books.csv

mkdir books

# Create a tmp director for storing temporary files when hive chooses to runs MR jobs locally
mkdir tmp

# Write the first 90000 records to books/part-1
head -n 90000 books.csv > books/part-1

# Write the next 90000 records to books/part-2
tail -n+90001 books.csv | head -n 90000 > books/part-2

# Write the remaining ones to books/part-3
tail -n+180001 books.csv > books/part-3

# Load the data to the default warehouse directory in HDFS
hdfs dfs -put books /user/hive/warehouse/
hdfs dfs -cat /user/hive/warehouse/books/* | head
hdfs dfs -df -h
```


Save the change and get back to the shell. Then run:

```shell
bash script.sh
```
or 

```shell
sh script.sh
```

This allows you to do all the local and HDFS file system operations in one go.

Run the following code to confirm the data has been placed on HDFS:

```shell
hdfs dfs -ls /user/hive/warehouse/books
```



<br>

# Use SQL-like Queries with Hive

<br>

## Step 1: Open the Hive CLI  

To launch Hive with a custom configuration:

```shell
hive -hiveconf mapreduce.cluster.local.dir=/home/hadoop/tmp
```



Note: 

- `-hiveconf` lets us set configuration properties at startup.
- Here, we’re telling Hive where to store temporary files when it chooses to run MapReduce jobs locally.

<br>

## Step 2: Creating a Table (External vs Internal)

Define a schema over raw data stored in HDFS — just like creating a virtual table you can query with SQL:

```sql
create external table books
(ISBN string, BookTitle string, BookAuthor string, YearOfPublication string, Publisher string, ImageURLS string, ImageURLM string, ImageURLL string)
row format serde 'org.apache.hadoop.hive.serde2.OpenCSVSerde' with serdeproperties ("separatorChar" = ";", "quoteChar"= "\"");
```

Note:

- If you **don’t** use the `EXTERNAL` keyword, Hive creates the table in **internal mode**, meaning:

  - Hive manages the data entirely.
  - Dropping the table will delete the underlying data stored in HDFS, e.g., `/user/hive/warehouse/books`.

   

  

  <br>

## Step 3: Exploring Schema and Data



Show a brief schema:

```sql
DESCRIBE books;
```

Show a detailed schema (including storage info):

```sql
DESCRIBE FORMATTED books;
```

Display the first 5 records to have a preview:

```sql
SELECT * FROM books LIMIT 5;
```



## Step 4: Aggregation Queries

1 MapReduce job is sufficient for a basic group-by:

```sql
SELECT YearOfPublication, COUNT(BookTitle) FROM books GROUP BY YearOfPublication LIMIT 20;
```

To get the **top 20 most popular publication years**, sorted by book count (requires 2 MapReduce jobs):

```sql
SELECT YearOfPublication, COUNT(BookTitle) AS NoOfBooks FROM books
GROUP BY YearOfPublication ORDER BY NoOfBooks DESC LIMIT 20;
```

<br>



## Step 5: Writing Query Output to a Table

Create a new external table:

```sql
CREATE EXTERNAL TABLE YearBuckets (YearOfPublication STRING, count INT);
```

Insert grouped data into this table:

```sql
INSERT OVERWRITE TABLE YearBuckets
SELECT YearOfPublication, COUNT(BookTitle) FROM books GROUP BY YearOfPublication;
```

Hive will create a directory `/user/hive/warehouse/yearbuckets/` automatically to store the results.

To confirm this, you can run:

```shell
dfs -ls /user/hive/warehouse/yearbuckets;
```

Preview the output:

```sql
SELECT * from YearBuckets LIMIT 20;
```



------



<br>



## Step 6: Self-Join: Book Pairs from Same Publisher and Year

To find pairs of books published in the same year by the same publisher (and avoid duplicate pairs like A-B and B-A):

```sql
SELECT t1.BookTitle, t2.BookTitle, t2.Publisher, t2.YearOfPublication FROM books t1 JOIN books t2 ON t1.Publisher = t2.Publisher AND t1.YearOfPublication = t2.YearOfPublication WHERE t1.BookTitle < t2.BookTitle LIMIT 10;
```

------



<br>

## Step 7 Saving Output to HDFS or Local



### Option 1: Save to a Custom HDFS Directory

```sql
INSERT OVERWRITE DIRECTORY "/tmp/pairs" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT t1.BookTitle, t2.BookTitle, t2.Publisher, t2.YearOfPublication FROM books t1 JOIN books t2 ON t1.Publisher = t2.Publisher AND t1.YearOfPublication = t2.YearOfPublication WHERE t1.BookTitle < t2.BookTitle;
```

You can confirm the output is saved using:

```shell
dfs -ls /tmp/pairs;
```



------



### Option 2: Save Output to a Local Filesystem Directory

```sql
INSERT OVERWRITE LOCAL DIRECTORY "pairs" ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' SELECT t1.BookTitle, t2.BookTitle, t2.Publisher, t2.YearOfPublication FROM books t1 JOIN books t2 ON t1.Publisher = t2.Publisher AND t1.YearOfPublication = t2.YearOfPublication WHERE t1.BookTitle < t2.BookTitle;
```

You can confirm the output is saved using:

```shell
!ls pairs;
```





------

<br>



## Step 8: Exit the Hive CLI

```sql
exit;
```





<br>

# Where to find the Hive metastore (Optional)

Hive stores table schemas in a **local Derby database** when no external metastore is configured. On an EMR master node, the default Derby metastore location is:

```sql
ls /var/lib/hive/metastore/metastore_db/
```



<br>

# Run Hive Queries on Tez (Optional)

**Apache Tez** is a high-performance, DAG-based execution engine that is often used as a drop-in replacement for MapReduce in Hive.

Tez supports a **rich set of operators** — including filters, joins, unions, group-bys, and sorts — and can pass intermediate results **in-memory** between stages. As a result, it can run complex Hive queries **much faster** than MapReduce.

To use Tez as Hive’s execution engine:

```shell
hive -hiveconf hive.execution.engine=tez
```



 

