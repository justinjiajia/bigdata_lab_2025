
# EMR settings

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
                "dfs.blocksize": "16M",
                "dfs.replication": "3"
            }
        }
    ]
    ```
    - EMR's default block size is 128M, and the default replication factor is 2. 
      
      

- Make sure the primary node's EC2 security group has a rule allowing for "ALL TCP" from "My IP" and a rule allowing for "SSH" from "Anywhere".

- You can also include a rule allowing for "SSH" from "Anywhere" into the EC2 security group for core nodes.

<br>



# File system operations for data preparation

```shell
mkdir data

cd data

wget https://archive.org/download/encyclopaediabri31156gut/pg31156.txt

wget https://archive.org/download/encyclopaediabri34751gut/pg34751.txt

wget https://archive.org/download/encyclopaediabri35236gut/pg35236.txt

wget -O nytimes.txt https://raw.githubusercontent.com/justinjiajia/datafiles/main/nytimes_news_articles.txt

wget -O flights.csv https://raw.githubusercontent.com/justinjiajia/datafiles/main/International_Report_Passengers.csv

cd ..

du -sh data
```

<br>

# HDFS operations

You can change all `<Your ITSC Account>` placeholders below to your ITSC account string first. Later, you can just copy and paste the commands to the terminal for execution.

Note that `hadoop fs` and `hdfs dfs` are interchangeable below.

```shell
$ hadoop fs -ls /

$ hadoop fs -mkdir -p /<Your ITSC Account>

$ hdfs dfs -ls /

$ hadoop fs -put data /<Your ITSC Account>

$ hadoop fs -ls /<Your ITSC Account>/data

$ hadoop fs -ls /<Your ITSC Account>/data

$ hadoop fs -cat /<Your ITSC Account>/data/pg35236.txt | tail -n50

$ hadoop fs -get /<Your ITSC Account>/data <A Directory in Local FS> 
```

<br>

# Customize block size and replication factor

We can customize the block size and replication factor on a file basis (not on a directory basis)

```shell
[hadoop@xxxx ~]$ hadoop fs -mkdir /input
[hadoop@xxxx ~]$ hadoop fs -D dfs.blocksize=32M -put data/flights.csv /input/a.csv
[hadoop@xxxx ~]$ hadoop fs -D dfs.replication=2 -D dfs.blocksize=64M -put data/flights.csv /input/b.csv
```

On the NameNode Web UI, we can see these two files successfully added.

<img width="800" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/a7f4c939-e271-444b-9789-e1ece0b392d2">

<br>

We can also reset the replication factor for an existing file or directory:

```shell
[hadoop@xxxx ~]$ hadoop fs -setrep -w 3 /input/b.csv
Replication 3 set: /input/b.csv
Waiting for /input/b.csv .... done
```
<img width="800" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/2d961332-baa1-43b8-9df2-01bc16eb463b">

```shell
[hadoop@xxxx ~]$ hadoop fs -setrep -w 2 /input
Replication 2 set: /input/a.csv
Replication 2 set: /input/b.csv
Waiting for /input/a.csv ...
WARNING: the waiting time may be long for DECREASING the number of replications.
. done
Waiting for /input/b.csv ... done
```

<br>

> The `-w` option in the `hadoop fs -setrep` command stands for "wait." When you use the `-w` option, the command will wait for the replication to complete before returning.


<img width="800" alt="image" src="https://github.com/justinjiajia/bigdata_lab/assets/8945640/41ff4ab0-0442-4195-8279-0571a925deca">
