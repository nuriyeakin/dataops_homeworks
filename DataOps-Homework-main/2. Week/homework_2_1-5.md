### 1. QUESTIONS

##### 1.  
Create a topic named `atscale`, 2 partitions and replication factor 1.

##### 2. 
List all topics.

##### 3. 
Describe `atscale` topic.

##### 4. 
Use data-generator and send `https://raw.githubusercontent.com/erkansirin78/datasets/master/Churn_Modelling.csv` to  3 patitioned `churn` topic.

- Message key should be CustomerId.

- Consume under `churn_group` and this group must have 3 consumer. 
- Use different terminal for each consumer. 
- Use `kafka-console-consumer.sh` as a consumer.

- Observe 500 messages.

##### 5. 
Delete `atscale` and `churn` topics.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
### 2. Start Kafka local

1. Start zookeeper
```
[train@trainvm]$ sudo systemctl start zookeeper
```

2. Start kafka server
```
[train@trainvm]$ sudo systemctl start kafka
```

3. Status kafka server
```
[train@trainvm]$ sudo systemctl status kafka
```

### 2. List topics
` [train@trainvm]$ kafka-topics.sh --bootstrap-server localhost:9092 --list `

### 3. Create a topic

```
kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic atscale \
--replication-factor 1 \
--partitions 2
```

```
kafka-topics.sh --bootstrap-server localhost:9092 --list
```


### 4. Describe a topic
```
kafka-topics.sh --bootstrap-server localhost:9092 \
--describe --topic atscale


Topic: atscale   PartitionCount: 2       ReplicationFactor: 1    Configs: segment.bytes=1073741824
        Topic: atscale   Partition: 0    Leader: 0       Replicas: 0     Isr: 0
        Topic: atscale   Partition: 1    Leader: 0       Replicas: 0     Isr: 0
      
```

### 5. Create New Topic For Question 4

[train@trainvm]$ 
```
kafka-topics.sh --bootstrap-server localhost:9092 \
--create --topic churn \
--partitions 3 \
--replication-factor 1
```

[train@trainvm]$ 
```
kafka-topics.sh --bootstrap-server localhost:9092 --list
```

### 6. Producing with data-generator
- Use instructions in this repo to use data generator

```
[train@trainvm]$ git clone https://github.com/erkansirin78/data-generator.git
```

```
[train@trainvm]$ cd data-generator/
```

```
[train@trainvm data-generator]$ python3 -m virtualenv datagen
```

```
[train@trainvm data-generator]$ source datagen/bin/activate
```
 
```
(datagen) [train@trainvm data-generator]$ pip install -r requirements.txt
```

### 7. Create Consumer 

- You open 3 terminal
- Terminal 2,3,4
  
```  
[train@trainvm]$ kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic churn --group churn_group
```

### 8. Produce dataset 

- Terminal 1
```
(datagen) [train@trainvm data-generator]$  python dataframe_to_kafka.py -i ~/datasets/Churn_Modelling.csv -t churn -k 1
```


### 9. Delete a topic
```
(datagen) [train@trainvm data-generator]$ kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic churn
```
```
(datagen) [train@trainvm data-generator]$ kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic atscale
```

```
kafka-topics.sh --bootstrap-server localhost:9092 --list
```
