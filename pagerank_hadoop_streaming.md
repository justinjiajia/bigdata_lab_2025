
### Data preparation

```shell
nano script.sh
```

```shell
#!/bin/bash

echo -e "A\t0.3333\tA,B\nB\t0.3333\tA,C\nC\t0.3333\tB" > pagerank.txt

hadoop fs -mkdir -p /pagerank/input
hadoop fs -put pagerank.txt /pagerank/input
```

### *mapper.py*

```shell
nano mapper.py
```


```python                                      
#!/usr/bin/env python3
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    # Parse input: <node_id> <current_rank> <outlinks>
    parts = line.split()
    node = parts[0]
    current_rank = float(parts[1])
    outlinks = parts[2].split(',') if len(parts) > 2 else []

    # Emit contributions to all outlinks
    if len(outlinks) > 0:
        contribution = current_rank / len(outlinks)
        for outlink in outlinks:
            print(f"{outlink}\t{contribution:.10f}")

    # Preserve the graph structure (pass outlinks to reducer)
    print(f"{node}\t{'|OUTLINKS|' + ','.join(outlinks)}")
```


### *reducer.py*


```shell
nano reducer.py
```



```python                                        
#!/usr/bin/env python3
import sys

N = 3  # Total nodes (adjust based on your graph)
last_node, contributions = None, []

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue

    node, value = line.split('\t', 1)

    if last_node and last_node != node:
        new_rank = sum(contributions)
        print(f"{last_node}\t{new_rank:.10f}\t{outlinks_str}")
        contributions = []

    if value.startswith('|OUTLINKS|'):
        outlinks = value.replace('|OUTLINKS|', '').split(',')
        outlinks_str = ','.join(outlinks) if outlinks else ''
    else:
        contributions.append(float(value))
        
       
    last_node = node

if last_node:
    new_rank = sum(contributions)
    print(f"{last_node}\t{new_rank:.10f}\t{outlinks_str}")
```


### Driver script 

```shell
nano run_pagerank.sh 
```


```shell
#!/bin/bash

# Configuration
INPUT_PATH="/pagerank/input"
OUTPUT_PREFIX="/pagerank/output_iter_"
MAX_ITERATIONS=10  # Default stopping criterion

# Remove old outputs
hadoop fs -rm -r ${OUTPUT_PREFIX}*

# Run iterations
for ((i=0; i<MAX_ITERATIONS; i++))
do
    echo "Iteration $i"
    INPUT=$INPUT_PATH
    if [ $i -ne 0 ]; then
        INPUT="${OUTPUT_PREFIX}$((i-1))"
    fi

    # Run Hadoop Streaming job
    mapred streaming -D mapreduce.input.fileinputformat.split.minsize=134217728 \   # create splits of at least 128 MB; otherwise, hadoop streaming will create 10+ splits
        -files mapper.py,reducer.py \
        -mapper "python mapper.py" \
        -reducer "python reducer.py" \
        -input $INPUT \
        -output "${OUTPUT_PREFIX}${i}"

done

echo "PageRank completed after $MAX_ITERATIONS iterations."
```

### Run the script

```shell
sh run_pagerank.sh
```
