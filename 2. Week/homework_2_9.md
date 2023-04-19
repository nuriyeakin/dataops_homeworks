## Postgres and kafka with DBeaver

### 1. Open pycharm create new project and Create requirements.txt

```
kafka-python
pandas
psycopg2
```

```
pip install -r requirements.txt
```

### 2. Create consumer.py

```
from kafka import KafkaConsumer
import psycopg2
consumer = KafkaConsumer('churn',
                         consumer_timeout_ms=10000,
                         bootstrap_servers=['localhost:9092', 'localhost:9292'])
#ConnectDB
conn = psycopg2.connect( database="traindb", user='train', password='Ankara06', host='127.0.0.1', port= '5432')
cursor = conn.cursor()
#CreateTable
cursor.execute("DROP TABLE IF EXISTS ChurnCustomers")
sql ='''CREATE TABLE ChurnCustomers(
    CustomerId INT NOT NULL,
    Gender char(20),
    Age INT,
    EstimatedSalary FLOAT,
    Exited INT
)'''
cursor.execute(sql)
conn.commit()
for msg in consumer:
    messagekey = msg.key.decode("utf-8")
    messagevalue = msg.value.decode("utf-8")
    print(messagekey)
    print(messagevalue)
    print("*"*20)
    x=messagevalue.split(",")
    cursor.execute(
        "INSERT INTO ChurnCustomers (CustomerId, Gender, Age, EstimatedSalary, Exited) VALUES (%s,%s,%s,%s,%s)",
        (messagekey, str(x[0]), x[1], x[2], x[3]))
    conn.commit()
    print('Data added to POSTGRESQL')
conn.close()
```

### 3. Create producer.py

```
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import pandas as pd
import time
# Create topic
admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092', 'localhost:9292'])
try:
    churn = NewTopic(name='churn',num_partitions=3, replication_factor=1)
    admin_client.create_topics(new_topics=[churn])
except:
    print("churn topic already created.")
producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9292'])
# Read data
df = pd.read_csv("/home/train/datasets/Churn_Modelling.csv")
print(df.head())
df.columns
for index, row in df.iterrows():
    my_msg = f"{row['Gender']},{row['Age']},{row['EstimatedSalary']},{row['Exited']}".encode("utf-8")
    my_key = f"{row['CustomerId']}".encode("utf-8")
    producer.send('churn', key=my_key,
                   value=my_msg)
    print(index)
    time.sleep(0.1)
producer.flush()
```
- Select and run half of the code
```
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import pandas as pd
import time
# Create topic
admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092', 'localhost:9292'])
try:
    churn = NewTopic(name='churn',num_partitions=3, replication_factor=1)
    admin_client.create_topics(new_topics=[churn])
except:
    print("churn topic already created.")
producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9292'])
# Read data
df = pd.read_csv("/home/train/datasets/Churn_Modelling.csv")
print(df.head())
```
