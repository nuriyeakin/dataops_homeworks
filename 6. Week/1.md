### 0. Qestion

```
- New sales are available per hour in `http://trainvm.vbo.local:5000/data`

- You need to ingest this data into object storage as it is. 
  - Example:
    - For the time xx:yy you need to process hour_xx_supermarket_sales.csv file.
    - For the time 13:15 you need to process hour_13_supermarket_sales.csv file.

- For some hours there will be no data. In this case your pipeline must not fail. 

- After ingesting data from nginx server you need to save object storage:
  - Bucket: `dataops`
  - Key: `supermarket_sales/{today}/hour_{this_hour}_supermarket_sales.csv`

- For each day there must be different directory and in this directory there must be at most 24 files for each hour. 
  But if there is no data in the source, no file is acceptable.

- Example value for today: 20230312 and example value for hour: 17
```


### 1. Activate venv for airflow
 
```
[train@trainvm ~]$ source ~/venvairflow/bin/activate
```
 
```
(venvairflow) [train@trainvm ~]$ sudo systemctl start airflow
```
  
```
(venvairflow) [train@trainvm ~]$ sudo systemctl start airflow-scheduler
```

```
(venvairflow) [train@trainvm ~]$ cd dataops7/
```

```
(venvairflow) [train@trainvm ~]$ cd airflow_project/
```

```
(venvairflow) [train@trainvm airflow_project]$ sudo systemctl start docker
```

```
(venvairflow) [train@trainvm airflow_project]$ docker-compose up -d
```

```
(venvairflow) [train@trainvm airflow_project]$ docker-compose ps
```

### 2. Pre Requiests

```
# connect to nginx container
docker exec -it nginx bash


# change directory to html root
root@1ea647bbd0c4:/# cd /usr/share/nginx/html/
root@1ea647bbd0c4:/usr/share/nginx/html# mkdir data
root@1ea647bbd0c4:/usr/share/nginx/html# cd data


# Download data
root@1ea647bbd0c4:/usr/share/nginx/html/data# for i in {10..20}; do curl -o hour_${i}_supermarket_sales.csv  https://raw.githubusercontent.com/erkansirin78/datasets/master/supermarket_hourly/hour_${i}_supermarket_sales.csv; done


# check data
root@1ea647bbd0c4:/usr/share/nginx/html/data# ls -l
total 160
-rw-r--r--. 1 root root   497 Jul 19 14:05 50x.html
-rw-r--r--. 1 root root 13698 Oct 15 05:57 hour_10_supermarket_sales.csv
-rw-r--r--. 1 root root 12159 Oct 15 05:57 hour_11_supermarket_sales.csv
-rw-r--r--. 1 root root 12069 Oct 15 05:57 hour_12_supermarket_sales.csv
-rw-r--r--. 1 root root 13906 Oct 15 05:57 hour_13_supermarket_sales.csv
-rw-r--r--. 1 root root 11241 Oct 15 05:57 hour_14_supermarket_sales.csv
-rw-r--r--. 1 root root 13770 Oct 15 05:57 hour_15_supermarket_sales.csv
-rw-r--r--. 1 root root 10454 Oct 15 05:57 hour_16_supermarket_sales.csv
-rw-r--r--. 1 root root 10085 Oct 15 05:57 hour_17_supermarket_sales.csv
-rw-r--r--. 1 root root 12597 Oct 15 05:57 hour_18_supermarket_sales.csv
-rw-r--r--. 1 root root 15266 Oct 15 05:57 hour_19_supermarket_sales.csv
-rw-r--r--. 1 root root 10222 Oct 15 05:57 hour_20_supermarket_sales.csv
-rw-r--r--. 1 root root   615 Jul 19 14:05 index.html
```

- Test 
```
[train@trainvm ~]$ curl -o /tmp/hour_13_supermarket_sales.csv http://localhost:5000/data/hour_13_supermarket_sales.csv

[train@trainvm ~]$ ls -l /tmp | grep super
-rw-rw-r--. 1 train train  13906 Oct 15 09:02 hour_13_supermarket_sales.csv
```


### 3. Create "homework_6.py"
```
import io
import logging
from datetime import datetime, timedelta

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator

start_date = datetime(2023, 3, 11)
today = datetime.now().strftime('%Y%m%d')
this_hour = int(datetime.now().strftime('%H'))
default_args = {
    'owner': 'train',
    'start_date': start_date,
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}


# pandas df to s3
s3_client = boto3.client('s3',
                         aws_access_key_id='airflow',
                      aws_secret_access_key='Ankara06',
                         endpoint_url='http://localhost:9000')


def save_df_to_s3(df, bucket, key, s3_client, index=False):
    ''' Store df as a buffer, then save buffer to s3'''
    try:
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        s3_client.put_object(Body=csv_buffer.getvalue(), Bucket=bucket, Key=key)
        logging.info(f'{key} saved to s3 bucket {bucket}')
    except Exception as e:
        raise logging.exception(e)


def read_data(**kwargs):
    try:
        df = pd.read_csv(kwargs['data'])
    except:
        return 'error'

    if len(df) > 0:
        save_df_to_s3(df=df, bucket='dataops', key=f'supermarket_sales/{today}/hour_{this_hour}_supermarket_sales.csv', s3_client=kwargs['s3_client'])
        return 'task_success'
    else:
        return 'not_data'


with DAG('homework6_2', default_args=default_args, schedule_interval='@hourly', catchup=False,) as dag:
    t1 = BranchPythonOperator(task_id='read_data',
                                                  python_callable=read_data,
                                                  op_kwargs={'data': f'http://trainvm.vbo.local:5000/data/hour_{this_hour}_supermarket_sales.csv',
                                                             's3_client': s3_client},
                                                  do_xcom_push=False)

    t2 = DummyOperator(task_id='error')

    t3 = DummyOperator(task_id='task_success')

    t4 = DummyOperator(task_id='not_data')

    t5 = DummyOperator(task_id='following_tasks',trigger_rule='none_failed_or_skipped')

    t6 = DummyOperator(task_id='following_another_tasks')

    t1 >> [t2, t3, t4] >> t5 >> t6
```

```
(venvairflow) [train@trainvm 6_Week]$ cp homework6_1.py ~/venvairflow/dags
```

```
(venvairflow) [train@trainvm 6_Week]$ ls ~/venvairflow/dags/
```

### 4. Create AWS key_id and access_key

- Go to http://127.0.0.1:1502/home

- Go to Admin-Variables

- Create "s3_access_key_id" and  "s3_secret_access_key"
