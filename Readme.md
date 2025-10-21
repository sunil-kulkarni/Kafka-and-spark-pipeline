## Apache Kafka and Apache Spark Pipeline

### This project demonstrates the use of Apache kafka and Apache spark for building a datastreaming pipeline

### How to run the setup:

#### Part-1 Data streaming 

To run this project we require four machines
1) Producer - To stream the data
2) Broker - Uses Zookeeper and kafka to receive data from the producer and distribute it to the consumers
3) Consumer-1 - Receives data from the broker and stores the relevant features in two different csv files.
4) Consumer-2 - Just like Consumer-1, it is used for receiving a different set of features and then stores in two separate csv files.

#### Part-2 Data manipulation

Run the spark jobs and set the threshold values as mentioned in `thresholds.txt`



##### Condition for sparkjob-1
| Condition | Alert |
| --------- |------ |
|avg(cpu_pct) > threshold AND avg(mem_pct) > threshold |“High CPU + Memory stress” |
| avg(cpu_pct) >  threshold AND avg(mem_pct) ≤  threshold | “CPU spike suspected” |
| avg(mem_pct) >  threshold AND avg(cpu_pct) ≤  threshold | “Memory saturation suspected” 
 

##### Conditions for sparkjob-2

| Condition | Alert |
| --------- |------ |
| max(net_in) >  threshold AND max(disk_io) > threshold | “Network flood + Disk thrash suspected” |
| max(net_in) >  threshold AND max(disk_io) ≤  threshold | “Possible DDoS” |
| max(disk_io) >  threshold AND max(net_in) ≤  threshold | “Disk thrash suspected” |

## Installation

### For producer
```bash
pip install kafka-python
sudo apt-get install -y libsnappy-dev
pip install python-snappy
```




