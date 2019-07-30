# Testing Strategy

## Things to test

- **Correctness**
  - compare our results with the results of OCDDISVOVER on small datasets
- **Single-Node Performance**
  - test for 3-5 datasets
  - run OCDDISCOVER first on a machine
  - use the same machine to run our algorithm
  - run each dataset for each algorithm 10 times and calculate average runtime
  - compare the runtime of both approaches
- **Scaling with Number of Nodes**
  - pick one dataset (medium size); we could use `flights_1k.csv` and remove some columns (and rows?) to reduce the complexity
  - test with 1 to 10? nodes
  - make test twice for each number of nodes
- **Reliability and Robustness**
  - use a mind. 4-node cluster, medium size dataset
  - run algorithm and randomly kill a node or the JVM running on it
  - when finished compare results to running the 4-node cluster without killing nodes (time?, same results?)
- **Dynamic Computation**
  - use a mind. 3-node cluster, medium size dataset
  - after some time start a new node and let it join the cluster
  - record number of processed items of all nodes (workers)

## Measurements

- **time:** use `System.currentTimeMillis()` to get current system time (this is not accurate and continuous, but we deal with minutes and hours, so we don't care)
  - each node prints out the following times
    - starting time
    - when reduced column were received
    - downing time
  - the seed node (we only have one by design!) also outputs the times for pruning (begin and end)
  - at the end, we collect all times and calculate the runtime and other measurements
    - e.g. time duration to run the alg.: `max(allEndTimes) - min(allStartTimes)`
- **CPU Util:** **find a way to record CPU utilization!!!**
- **Memory:** use `cat /proc/meminfo | head -n 3` to record memory utilization every 5 min

## Prioritization

1. Create medium size dataset
2. Run medium to large size dataset to completion (to ensure bugfreeness)
3. Single Node Performance (compare to OCDDISCOVER)
4. 4-node cluster reliability test
5. Multi Node Scaling
