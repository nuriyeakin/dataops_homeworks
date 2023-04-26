### 0. Qestion

`https://github.com/erkansirin78/datasets/raw/master/iot_telemetry_data.csv.zip` 

- 1. Generate with data-generator to `~/data-generator/output` directory.

- 2. Based on the event time calculate for each (device) 10 mins window;
    - sensor (device) id
    - How many signal generated from this sensor
    - Average co and humidity    

Example console output:

```
+------------------------------------------+-----------------+--------+---------------------+------------------+
|window                                    |device           |count(1)|avg(co)              |avg(humidity)     |
+------------------------------------------+-----------------+--------+---------------------+------------------+
|[2020-07-12 02:55:00, 2020-07-12 03:05:00]|00:0f:00:70:91:0a|34      |0.0028614125091253836|76.03529357910156 |
|[2020-07-12 03:05:00, 2020-07-12 03:15:00]|b8:27:eb:bf:9d:51|163     |0.00498870632502656  |49.968711338160226|
|[2020-07-12 03:05:00, 2020-07-12 03:15:00]|00:0f:00:70:91:0a|98      |0.002834142176244332 |75.18061166880082 |
|[2020-07-12 03:00:00, 2020-07-12 03:10:00]|00:0f:00:70:91:0a|85      |0.0028452644179410794|75.77882268569049 |
|[2020-07-12 03:10:00, 2020-07-12 03:20:00]|1c:bf:ce:15:ec:4d|55      |0.004302371377972039 |78.06000005548651 |
|[2020-07-12 03:15:00, 2020-07-12 03:25:00]|00:0f:00:70:91:0a|10      |0.0028118212008848785|74.78999938964844 |
|[2020-07-12 03:15:00, 2020-07-12 03:25:00]|1c:bf:ce:15:ec:4d|10      |0.004261006554588676 |78.30999908447265 |
|[2020-07-12 02:55:00, 2020-07-12 03:05:00]|b8:27:eb:bf:9d:51|56      |0.004961154057777354 |50.92678655896868 |
|[2020-07-12 03:05:00, 2020-07-12 03:15:00]|1c:bf:ce:15:ec:4d|90      |0.004340634179405041 |78.10000025431314 |
|[2020-07-12 03:00:00, 2020-07-12 03:10:00]|b8:27:eb:bf:9d:51|137     |0.00497653272111703  |50.91313881421611 |
+------------------------------------------+-----------------+--------+---------------------+------------------+
```


### 1. Install Data
 
 ```
 ! wget -P /home/train/datasets/  https://github.com/erkansirin78/datasets/raw/master/IOT-temp.csv.zip
 ```
 
 ### 2. Activate venv
 ```
 [train@localhost ~]$ cd data-generator/
 
 [train@localhost data-generator]$ source datagen/bin/activate
 ```
 
### 3. Create New File For Schema
```
[train@localhost data-generator]$ mkdir /tmp/iot_telemetry-schema
```
 
```
python dataframe_to_log.py -i https://github.com/erkansirin78/datasets/raw/master/iot_telemetry_data.csv.zip -o /tmp/iot_telemetry-schema -oh True
```

### 4. Create New File For Input 

``` 
(datagen) [train@trainvm data-generator]$ mkdir /tmp/iot_telemetry-input 
``` 
``` 
python dataframe_to_log.py -i https://github.com/erkansirin78/datasets/raw/master/iot_telemetry_data.csv.zip -o /tmp/iot_telemetry-input -shf True
``` 
 
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

### 5. Cleaning Commands

``` 
(datagen) [train@trainvm data-generator]$ rm -rf /tmp/streaming/week5_2_check/*
``` 
 
``` 
(datagen) [train@trainvm data-generator]$ rm -rf /tmp/iot_telemetry-input/*
``` 
 
``` 
(datagen) [train@trainvm data-generator]$ rm -rf /tmp/iot_telemetry-output/*
```

### 6. Code

```
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import window, avg, count, col, to_timestamp

spark = SparkSession.builder.appName("homework5_2").getOrCreate()

# prevent to error
spark.sparkContext.setLogLevel('ERROR')


# We must give schema to read
df = spark.read.format("csv") \
    .option("header", True) \
    .option("sep", ",") \
    .option("inferSchema", True) \
    .load("file:///tmp/iot_telemetry-schema") \
    .withColumn("event_time", F.to_timestamp(F.col("event_time"), "y-M-d h:m:s. SSSSSS"))

stream_schema = df.schema
df.printSchema()



# Read from source
line = spark.readStream.format("csv") \
            .schema(stream_schema) \
            .load("file:///tmp/iot_telemetry-input")



line2 = line\
    .groupBy(window(col("event_time"), "10 minutes", "5 minutes"), "device")\
    .agg(count("*").alias("signal_count"), avg("co").alias("avg_co"), avg("humidity").alias("avg.humidity"))\
    .orderBy("device")






checkpointDir = "file:///tmp/streaming/week5_2_check"


streamingQuery = (line2.writeStream
                  .format("console")
                  .outputMode("complete")
                  .trigger(processingTime="1 second")
                  .option("numRows", 10)
                  .option("truncate", False)
                  .option("checkpointLocation", checkpointDir)
                  .start())


streamingQuery.awaitTermination()
```


```
-------------------------------------------
Batch: 6
-------------------------------------------
+------------------------------------------+-----------------+------------+---------------------+-----------------+
|window                                    |device           |signal_count|avg_co               |avg.humidity     |
+------------------------------------------+-----------------+------------+---------------------+-----------------+
|{2023-04-10 11:50:00, 2023-04-10 12:00:00}|00:0f:00:70:91:0a|44          |0.0035794761033698867|75.24090940302068|
|{2023-04-10 11:50:00, 2023-04-10 12:00:00}|1c:bf:ce:15:ec:4d|57          |0.0042269459313427305|61.24561390123869|
|{2023-04-10 11:50:00, 2023-04-10 12:00:00}|b8:27:eb:bf:9d:51|69          |0.005625910672311013 |50.55507246376812|
+------------------------------------------+-----------------+------------+---------------------+-----------------+
```






