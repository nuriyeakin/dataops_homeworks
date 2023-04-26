 ### 0. Qestion
 
 https://github.com/erkansirin78/datasets/raw/master/IOT-temp.csv.zip

Generate the above data set with data-generator to "/tmp/iot-temp-input" directory.

Read this directory with Spark streaming and write it to the "/tmp/iot-temp-output" file by adding new attributes.

For example, month, day of the week to the data set based on the event time.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------
 
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
[train@localhost data-generator]$ mkdir /tmp/iot-temp-input_schema
```
 
```
python dataframe_to_log.py -i https://github.com/erkansirin78/datasets/raw/master/IOT-temp.csv.zip -o /tmp/iot-temp-input_schema -oh True
```
 
 


### 4. Create New File For Input

``` 
(datagen) [train@trainvm data-generator]$ mkdir /tmp/iot-temp-input 
``` 
``` 
python dataframe_to_log.py -i https://github.com/erkansirin78/datasets/raw/master/IOT-temp.csv.zip -o /tmp/iot-temp-input -shf True
``` 
 
 

### 5. Cleaning Commands

``` 
(datagen) [train@trainvm data-generator]$ rm -rf /tmp/streaming/week5_1_check/*
``` 
 
``` 
(datagen) [train@trainvm data-generator]$ rm -rf /tmp/iot-temp-input/*
``` 
 
``` 
(datagen) [train@trainvm data-generator]$ rm -rf /tmp/iot-temp-output/*
```


### 6. Code

```
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("homework5_1").getOrCreate()

# prevent to error
spark.sparkContext.setLogLevel('ERROR')


# We must give schema to read
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("file:///tmp/iot-temp-input_schema")

stream_schema = df.schema


# Read from source
line = spark.readStream.format("csv") \
            .schema(stream_schema) \
            .load("file:///tmp/iot-temp-input")


line2 = (line.withColumn("event_time", F.to_timestamp(line.event_time, "y-M-d H:m:s.SSSSSS"))
             .withColumn("year", F.year(F.col("event_time")))
             .withColumn("month", F.month(F.col("event_time")))
             .withColumn("dayofweek", F.date_format(F.col("event_time"), "E")))


checkpointDir = "file:///tmp/streaming/week5_1_check"


streamingQuery = (line2.writeStream.format("console")
                  .outputMode("append")
                  .trigger(processingTime="4 second")
                  .option("numRows", 4)
                  .option("truncate", False)
                  .option("checkpointLocation", checkpointDir)
                  .start("file:///tmp/iot-temp-output"))


streamingQuery.awaitTermination()


```


```
-------------------------------------------
Batch: 14
-------------------------------------------
+-----------------------------------+----------+----------------+----+------+--------------------------+----+-----+---------+
|id                                 |room_id/id|noted_date      |temp|out/in|event_time                |year|month|dayofweek|
+-----------------------------------+----------+----------------+----+------+--------------------------+----+-----+---------+
|__export__.temp_log_117460_0ebf202a|Room Admin|10-09-2018 17:19|27  |Out   |2023-04-10 09:29:01.682762|2023|4    |Mon      |
|__export__.temp_log_150144_da593c7a|Room Admin|12-09-2018 02:07|27  |Out   |2023-04-10 09:29:02.184473|2023|4    |Mon      |
|__export__.temp_log_19013_cda5e9fa |Room Admin|18-09-2018 22:38|42  |Out   |2023-04-10 09:29:02.6851  |2023|4    |Mon      |
|__export__.temp_log_175992_29d43848|Room Admin|02-12-2018 23:24|36  |Out   |2023-04-10 09:29:03.192449|2023|4    |Mon      |
+-----------------------------------+----------+----------------+----+------+--------------------------+----+-----+---------+
```


### 6.2 Code 2

```
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("homework5_1").getOrCreate()

# prevent to error
spark.sparkContext.setLogLevel('ERROR')


# We must give schema to read
df = spark.read.format("csv") \
    .option("header", True) \
    .option("inferSchema", True) \
    .load("file:///tmp/iot-temp-input_schema")

stream_schema = df.schema


# Read from source
line = spark.readStream.format("csv") \
            .schema(stream_schema) \
            .load("file:///tmp/iot-temp-input")


line2 = (line.withColumn("event_time", F.to_timestamp(line.event_time, "y-M-d H:m:s.SSSSSS"))
             .withColumn("year", F.year(F.col("event_time")))
             .withColumn("month", F.month(F.col("event_time")))
             .withColumn("dayofweek", F.date_format(F.col("event_time"), "E")))



def write_to_multiple_sinks(df, batchId):
    df.cache()
    df.show()

    (df.write
       .format("csv")
       .mode("append")
       .option("numRows", 4)
       .option("truncate", False))


    (df.write
       .format("csv")
       .mode("append")
       .save("file:///tmp/iot-temp-output"))


    df.persist()


checkpointDir = "file:///tmp/streaming/week5_1_check"


streamingQuery = (line2.writeStream
                  .foreachBatch(write_to_multiple_sinks)
                  .option("checkpointLocation", checkpointDir)
                  .start())


streamingQuery.awaitTermination()
```







- Check the file
```
ls -ltr /tmp/iot-temp-output/
```

 
