
# EMR settings

- EMR release: 7.8.0

- Application: Hadoop
  
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
                "dfs.block.size": "16M",
                "dfs.replication": "3"
            }
        },
        {
            "classification": "mapred-site",
            "properties": {
                "mapreduce.job.reduces": "3"
            }
        }
    ]
    ```

- Make sure the primary node's EC2 security group has a rule allowing for "ALL TCP" from "My IP" and a rule allowing for "SSH" from "Anywhere".


<br>

# Local file system operations for data preparation


```shell
$ mkdir data

$ cd data

$ wget https://archive.org/download/encyclopaediabri31156gut/pg31156.txt

$ wget https://archive.org/download/encyclopaediabri34751gut/pg34751.txt

$ wget https://archive.org/download/encyclopaediabri35236gut/pg35236.txt

$ wget -O nytimes.txt https://raw.githubusercontent.com/justinjiajia/datafiles/main/nytimes_news_articles.txt

$ cd ..

$ du -sh data
```

Instead of running them one after another, we can also put them into a `.script` file and run them in batch.

```shell
$ nano data_prep.sh
```


Copy and paste the code snippet below into the file:

```shell
#!/bin/bash

rm -r data
mkdir data
cd data
wget https://archive.org/download/encyclopaediabri31156gut/pg31156.txt
wget https://archive.org/download/encyclopaediabri34751gut/pg34751.txt
wget https://archive.org/download/encyclopaediabri35236gut/pg35236.txt
wget -O nytimes.txt https://raw.githubusercontent.com/justinjiajia/datafiles/main/nytimes_news_articles.txt
cd ..
du -sh data
```

Save the change and get back to the shell. Then run:

```shell
bash data_prep.sh
```
or 

```shell
sh data_prep.sh
```




<br>

# HDFS operations for data preparation

You can change all `<Your ITSC Account>` placeholders below to your ITSC account string first. 
Later, you can just copy and paste the commands to the terminal for execution

Note that `hadoop fs` and `hdfs dfs` can be interchangeably used below.

```shell
$ hadoop fs -df -h

$ hadoop fs -ls /

$ hadoop fs -mkdir -p /<Your ITSC Account>

$ hadoop fs -put data /<Your ITSC Account>

$ hadoop fs -ls /<Your ITSC Account>/data

$ hdfs dfs -df -h
```

You can use a HDFS filesystem checking utility to get a file's block report, e.g.,

```shell
$ hdfs fsck /<Your ITSC Account>/data/nytimes.txt -files -blocks -locations
```

<br>

# MapReduce job submission


Note that `hadoop jar` and `yarn jar` can be interchangeably used below.

```shell
$ hadoop jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar wordcount /<Your ITSC Account>/data /<Your ITSC Account>/wordcount_output
```

We can use the `-D` flag to define a value for a property in the format of `property=value`.
E.g., we can specify the number of reducers to use as follows:

```shell
$ yarn jar /usr/lib/hadoop-mapreduce/hadoop-mapreduce-examples.jar wordcount -D mapreduce.job.reduces=2  /<Your ITSC Account>/data /<Your ITSC Account>/wordcount_output_1
```

<br>

# Get the output

```shell
hadoop fs -cat /<Your ITSC Account>/wordcount_output/part-r-* > combinedresult.txt
head -n20 combinedresult.txt
tail -n20 combinedresult.txt
```

