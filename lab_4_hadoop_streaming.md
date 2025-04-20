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



# Data preparation

```shell
nano data_prep.sh
```

Copy and paste the code snippet below into the *data_prep.sh* file, and change all occurrences of `<Your ITSC Account>` to your ITSC account string. 


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
hadoop fs -mkdir /<Your ITSC Account>
hadoop fs -put data /<Your ITSC Account>
hadoop fs -df -h /<Your ITSC Account>/data
```


Save the change and get back to the shell. Then run:

```shell
bash data_prep.sh
```
or 

```shell
sh data_prep.sh
```

This allows you to do all the local and HDFS file system operations in one go.

Then, you can use a HDFS filesystem checking utility to get a file's block report, e.g.,

```shell
$ hdfs fsck /<Your ITSC Account>/data/nytimes.txt -files -blocks -locations
```

<br>

# Write a custom word count program

<br>

## Step 1: Create the program files for Mapper and Reducer


### Mapper code

```shell
nano mapper.py
```

Copy and paste the code below into the *mapper.py* file:

```shell
#!/usr/bin/env python3

import sys
import re
for line in sys.stdin:
    line = line.strip().lower()
    line = re.sub('[^A-Za-z\s]', '', line)
    words = line.split()
    for word in words:
        print(f"{word}\t{1}")
```


### Reducer code

```shell
nano reducer.py
```

Copy and paste the code below into the *reducer.py* file:


```shell
#!/usr/bin/env python3

import sys
current_word, current_count = None, 0
for line in sys.stdin:
    line = line.strip()
    word, count = line.split('\t', 1)
    count = int(count)
    if current_word == word:
       current_count += count
    else:
       if current_word:
          print(f"{current_word}\t{current_count}")
       current_count = count
       current_word = word

if word == current_word:
   print(f"{current_word}\t{current_count}")
```

<br>

## Step 2: Testing the programs locally 

First, create a local plain text file and populate it with several words:

```shell
echo -e "LAB\nA\nDATA1\na?\nbig.\nTHE\nlab\nintelligence\nanalytics\nThe\nBIG\ndata2\nan\nBusiness\nL.A.B." > input.txt
```


Note: `-e` enables interpretation of backslash escapes.

Run a sequence of linux commands and Python scripts chained by the pipe operator `|`:

```shell
cat input.txt | python ~/mapper.py | sort -k 1,1 | python ~/reducer.py
```

Note: `sort` is a Linux command that sorts lines of text. Here, it emulates Hadoop's shuffle and sort phase locally.

- By default, `sort` splits each line into fields separated by whitespace (e.g., use `-t $'\t'` explictly for tabs).

- `-k <start>,<end>` sorts lines by fields from <start> to <end> (inclusive). E.g., `-k 1,1` sorts only by the first field (key), grouping identical keys together.

<br>

## Step 3: Submitting the job

![#f03c15](https://placehold.co/15x15/f03c15/f03c15.png) IMPORTANT: Copy and paste the following code line by line, including `\` at the end.

```shell
$ mapred streaming -D mapreduce.job.reduces=2 \
  -files mapper.py,reducer.py \
  -input /<Your ITSC Account>/data -output /<Your ITSC Account>/program_output_1 \
  -mapper mapper.py -reducer reducer.py
```

Note: 

- `-D`: Set one Hadoop configuration property at a time
- `-files`: Specify a comma-separated list of files (no spaces) to be copied to the cluster
- `-D` and `-files` are generic options. They must appear before streaming-specific options such as `-input`, `-output`, `-mapper`, and `-reducer`.
- `\`: Tell the shell this command continues on the next line. Without it, pressing Enter would execute the command immediately.


<br>

## Step 4: Viewing the final output

```shell
hadoop fs -cat /<Your ITSC Account>/program_output_1/part-* > combinedresult.txt
head -n20 combinedresult.txt
tail -n20 combinedresult.txt
```

<br>

# Using Combiners

![#f03c15](https://placehold.co/15x15/f03c15/f03c15.png) IMPORTANT: Copy and paste the following code line by line, including `\` at the end.


```shell
$ mapred streaming -D mapreduce.job.reduces=2 \
  -files mapper.py,reducer.py \
  -input /<Your ITSC Account>/data -output /<Your ITSC Account>/program_output_2 \
  -mapper mapper.py -reducer reducer.py -combiner reducer.py
```

<br>

# Using custom Partitioner (Optional)


Hadoop Streaming allows the use of custom partitioners, but it requires the partitioner to be implemented in Java.


Note: You can skip step 1 by downloading a pre-compiled JAR.

```shell
wget https://github.com/justinjiajia/bigdata_lab_2025/raw/refs/heads/main/resources/partitioner.jar
```

##  Step 1: Coding and compiling the custom Java Partitioner 

Open a file named *CustomPartitioner.java*:

```shell
nano CustomPartitioner.java
```

Copy and paste the Java code below into the file:

```java
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.JobConf;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class CustomPartitioner implements Partitioner<Text, Text> {

    // Reserved words to send to partition 0
    private static final Set<String> RESERVED_WORDS = new HashSet<>(
        Arrays.asList("the", "a", "an")
    );

    @Override
    public int getPartition(Text key, Text value, int numPartitions) {
        String word = key.toString().toLowerCase().trim(); 

        // Edge case: If only 1 reducer, everything goes to partition 0
        if (numPartitions <= 1) {
            return 0;
        }

        // Send reserved words to partition 0
        if (RESERVED_WORDS.contains(word)) {
            return 0;
        } else {
            // Distribute other words across partitions 1 to (numPartitions-1)
            int hash = key.toString().hashCode();
            return (hash & Integer.MAX_VALUE) % (numPartitions - 1) + 1;
        }
    }

    @Override
    public void configure(JobConf job) {}
}
```

Compile the Java class and package it into a JAR called *partitioner.jar*: 

```shell
$ javac -classpath $(hadoop classpath) CustomPartitioner.java
$ jar -cf partitioner.jar CustomPartitioner*.class
```

You can then view the content of *partitioner.jar* to make sure that the *.class* file is inside:

```shell
$ jar -tf partitioner.jar
```

<br>

## Step 2: Testing the custom Partitioner

Since the custom partitioner is implemented in Java, we cannot test it directly through CLI commands like we do with Python scripts. 

Instead, we’ll validate its behavior by running a local MapReduce job.

This is configured by setting `-D mapreduce.framework.name=local`. 

### Submitting a local MapReduce job

![#f03c15](https://placehold.co/15x15/f03c15/f03c15.png) IMPORTANT: Copy and paste the following code line by line, including `\` at the end.

```shell
$ mapred streaming -D mapreduce.framework.name=local \
  -libjars partitioner.jar  \
  -files mapper.py,reducer.py,partitioner.jar \
  -input file://$(pwd)/input.txt -output file://$(pwd)/output \
  -mapper "python mapper.py" -reducer "python reducer.py" \
  -partitioner CustomPartitioner
```

Note:

- Hadoop assumes paths are in HDFS. The `file://` prefix above explicitly tells Hadoop to use the local file system (not HDFS) for input/output paths.

- `-libjars` specifies JAR files to include in the job's classpath. It is another example of generic options and must be specified before all streaming options. 

### Viewing the local output


```shell
$ ls output
$ cat output/part-00000
$ cat output/part-00001
$ cat output/part-00002
```

<br>

## Step 3: Submitting the job

```shell
mapred streaming -libjars partitioner.jar  \
-files mapper.py,reducer.py,partitioner.jar \
-input /<Your ITSC Account>/data -output /<Your ITSC Account>/program_output_3 \
-mapper "python mapper.py" -reducer "python reducer.py" \
-combiner "python reducer.py" \
-partitioner CustomPartitioner
```

<br>

## Step 4: Viewing the final output

```shell
hadoop fs -cat /<Your ITSC Account>/program_output_3/part-00000 
```





 

