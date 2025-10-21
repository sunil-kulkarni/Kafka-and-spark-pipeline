## Apache Kafka and Apache Spark Pipeline

 This project demonstrates the use of Apache kafka and Apache spark for building a datastreaming pipeline



### Part-1 Data streaming 

To run this project we require four machines
1) Producer - To stream the data
2) Broker - Uses Zookeeper and kafka to receive data from the producer and distribute it to the consumers
3) Consumer-1 - Receives data from the broker and stores the relevant features in two different csv files.
4) Consumer-2 - Just like Consumer-1, it is used for receiving a different set of features and then stores in two separate csv files.

### Part-2 Data manipulation

Run the spark jobs and set the threshold values as mentioned in `thresholds.txt`



### Condition for sparkjob-1
| Condition | Alert |
| --------- |------ |
|avg(cpu_pct) > threshold AND avg(mem_pct) > threshold |“High CPU + Memory stress” |
| avg(cpu_pct) >  threshold AND avg(mem_pct) ≤  threshold | “CPU spike suspected” |
| avg(mem_pct) >  threshold AND avg(cpu_pct) ≤  threshold | “Memory saturation suspected” 
 

### Conditions for sparkjob-2

| Condition | Alert |
| --------- |------ |
| max(net_in) >  threshold AND max(disk_io) > threshold | “Network flood + Disk thrash suspected” |
| max(net_in) >  threshold AND max(disk_io) ≤  threshold | “Possible DDoS” |
| max(disk_io) >  threshold AND max(net_in) ≤  threshold | “Disk thrash suspected” |

## Installation

### For Producer
```bash
pip install kafka-python
sudo apt-get install -y libsnappy-dev
pip install python-snappy
```
### For Broker
Install java jdk
```bash
sudo apt-get update
sudo apt-get install -y openjdk-11-jdk
```
Extract kafka

```bash
wget https://downloads.apache.org/kafka/3.9.0/kafka_2.13-3.9.0.tgz
tar -xzf kafka_2.13-3.9.0.tgz
cd kafka_2.13-3.9.0
```

### For consumer
```bash
sudo apt-get install libsnappy-dev
pip install python-snappy
```

### For running sparkjobs
```bash
pip install pyspark
```
