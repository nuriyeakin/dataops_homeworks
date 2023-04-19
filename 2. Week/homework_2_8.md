### 1. Question

Write a python function that fulfills the following requirements.
- Arguments: KafkaAdminClient object, topic name, number of partitions and replication factor
- Do not take any action if there is a topic with the same name
- If there is no topic with the same name, it will create a topic using arguments

def create_a_new_topic_if_not_exists(admin_client, topic_name="example-topic", num_partitions=1, replication_factor=1):
	<YOUR CODE HERE>
  
  
### 2. Start Kafka
  
  
### 3. Open Pycharm Editor 
  
Create these files;
  -.gitignore
  - requirements.txt
  - admin_client.py

  
### 4. Create function 
  
```
from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092', 'localhost:9292'],
                                client_id='dataops_client')

print(admin_client.list_topics())
#  ['test1', 'test', 'test2', '__consumer_offsets']

def create_a_new_topic(admin_client, topic_name="example-topic", num_partitions=1, replication_factor=1):

    if topic_name not in admin_client.list_topics():
        create_topic1 = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        try:
            admin_client.create_topics(new_topics=[create_topic1], validate_only=False)
        except:
            print('kafka.errors.TopicAlreadyExistsError')


create_a_new_topic(admin_client, topic_name="homework_topic_1", num_partitions=1, replication_factor=1)

print(admin_client.list_topics())

admin_client.close()
```
