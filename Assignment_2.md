


This assignment aims to give you hands-on practice with PySpark programming in EMR.

Launch an EMR cluster as instructed by lab 6’s manual. Choose to install both Hadoop and Spark on the cluster. 

Then SSH into the master node, and run the following script to prepare the data.

```shell
#!/bin/bash

wget https://raw.githubusercontent.com/justinjiajia/datafiles/main/soc-LiveJournal1Adj.txt
wget https://raw.githubusercontent.com/justinjiajia/datafiles/main/tiny_social_data.txt

hadoop fs -mkdir /input
hadoop fs -put soc-LiveJournal1Adj.txt /input
hadoop fs -put tiny_social_data.txt /input
hadoop fs -df -h /input
hadoop fs -ls /input
```

Betweem the 2 datasets, the [*soc-LiveJournal1Adj.txt*](https://snap.stanford.edu/data/soc-LiveJournal1.html) dataset represents the social network structure of LiveJournal users as an adjacency list. Unlike mutual friendships in Assignment 1, LiveJournal allows one-way friendship declarations - user A can declare user B as a friend without requiring B to reciprocate.

Despite this directional nature, our recommendation approach remains unaffected: If two users appear together in the declaration lists of many other users, they are likely to be friends and we should remcommend them to declare each other as friends.

**Implementation Note**:

For simplicity, you may still recommend these pairs even if one user has already declared the other as a friend (no need to filter out existing one-way connections).

[*tiny_social_data.txt*](https://github.com/justinjiajia/datafiles/blob/main/tiny_social_data.txt) stores a tiny portion of this dataset:

```
46133
46134    46069,46079,46105
46136
46148    46046,46096
46149    46046
49949    46058,49896,49936,49952,49955,49957,49960,49963,49979
46163    46062,46126
46160    46069
46157    46073,46093,46070
46156    46077
46152    46112
46140    2915
46141
46145
46150
46151    46068
46154
46161    46075
46162
46164
46168    46165,46167,46176,46178,46175
46169    46165,46170,46221
46173    46165,46170,46171,46172,46174,46176,46177,46179,46180,46194,46195,46207
46174    46165,46173,46177,46179,46180
46175    46165,46168,46176,46178,46221
```


So, the format of a record is as follows:

```
user_id <tab> friend1_id,friend2_id,friend3_id, ...
```

The record

```
46134    46069,46079,46105
```
means user 46134 has declared 3 friends. 


Note that there're users who haven't declare any friends. There are also users who have declared only one friend. They all should be dropped from subsequent processing.


# 2 Run PySpark on Yarn via shell

## Start the shell

```shell
pyspark --master yarn
```

