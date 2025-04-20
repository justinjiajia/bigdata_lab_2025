# EMR settings

- EMR release: 7.1.0 

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
                "dfs.blocksize": "16M",
                "dfs.replication": "3"
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

Copy and paste the code snippet below  into the *data_prep.sh* file, and change all occurrences of `<Your ITSC Account>` to your ITSC account string:

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

<br>

# Create program files for Mapper and Reducer

<br>

## Mapper code

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

<br>

## Reducer code

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

# Test the programs locally (Optional)


```shell
$ chmod +x mapper.py reducer.py
$ echo "foo FOO2 quux. lab foo Ba1r Quux" | ~/mapper.py | sort -k 1,1 | ~/reducer.py
```

<br>

# Submit the job


```shell
$ mapred streaming -D mapreduce.job.reduces=2 \
  -files mapper.py,reducer.py \
  -input /<Your ITSC Account>/data -output /<Your ITSC Account>/program_output_1 \
  -mapper mapper.py -reducer reducer.py
```

<br>

# View the output

```shell
hadoop fs -cat /<Your ITSC Account>/program_output_1/part-* > combinedresult.txt
head -n20 combinedresult.txt
tail -n20 combinedresult.txt
```

<br>

# Use of combiners


```shell
$ mapred streaming -D mapreduce.job.reduces=2 \
  -files mapper.py,reducer.py \
  -input /<Your ITSC Account>/data -output /<Your ITSC Account>/program_output_2 \
  -mapper mapper.py -reducer reducer.py -combiner reducer.py
```







 

