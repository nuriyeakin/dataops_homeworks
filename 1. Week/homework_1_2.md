# Questions 

- Create a hive database `hive_odev` and load this data https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv into `wine` table.

- In `wine` table filter records that `Alcohol`greater than 13.00 then insert these records into `wine_alc_gt_13` table.

- Drop `hive_odev` database including underlying tables in a single command.

- Load this https://raw.githubusercontent.com/erkansirin78/datasets/master/hive/employee.txt into table `employee` in `company` database. 

- Write a query that returns the employees whose Python skill is greater than 70.

-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

# Answers


### 0. Start Hadoop services
```
[train@localhost ~]$ tart-all.sh
```

### 1. Beeline connection
```
[train@localhost ~]$ beeline -u jdbc:hive2://127.0.0.1:10000
```

You should see `0: jdbc:hive2://127.0.0.1:10000>` means beeline shell is ready to use.  

Close logs
```
0: jdbc:hive2://127.0.0.1:10000> et hive.server2.logging.operation.level=NONE;
```


### 2. Create database named "hive_odev"
```
0: jdbc:hive2://127.0.0.1:10000> show databases;
```

```
0: jdbc:hive2://127.0.0.1:10000> create database hive_odev;
```

```
0: jdbc:hive2://127.0.0.1:10000> show databases;
```

```
0: jdbc:hive2://127.0.0.1:10000> use hive_odev;
```

#### Create table

0: jdbc:hive2://127.0.0.1:10000> 
```
create table if not exists wine
(Alcohol float, Malic_Acid float, Ash float, Ash_Alcanity float, Magnesium float, Total_Phenols float, Flavanoids float, Nonflavanoid_Phenols float, Proanthocyanins float, Color_Intensity float, Hue float, OD280 float, Proline float, Customer_Segment int)
row format delimited
fields terminated by ','
lines terminated by '\n'
tblproperties('skip.header.line.count'='1');
```

#### Show table
```
0: jdbc:hive2://127.0.0.1:10000> show tables;
```

#### Describe table
```
0: jdbc:hive2://127.0.0.1:10000> describe wine;
```

### 3. Load data to hive table
```
[train@trainvm ~]$ hdfs dfs -put ~/datasets/Wine.csv /user/train/hdfs_odev
```

```
0: jdbc:hive2://127.0.0.1:10000> load data inpath '/user/train/hdfs_odev/Wine.csv' into table wine;
```

#### Show
```
0: jdbc:hive2://127.0.0.1:10000> select * from wine limit 10;
```

### 4. Create a table with a query
```
0: jdbc:hive2://127.0.0.1:10000> create table wine_alc_gt_13 as select * from wine where wine.alcohol > 13.00;
```

```
0: jdbc:hive2://127.0.0.1:10000> select count(1) from wine_alc_gt_13;
```

### 5. Drop table and dataset only one specail query


```
0: jdbc:hive2://127.0.0.1:10000> drop database hive_odev cascade;
```
```
0: jdbc:hive2://127.0.0.1:10000> show databases;
```

### 6. Download DataSets
```
[train@trainvm ~]$ wget https://raw.githubusercontent.com/erkansirin78/datasets/master/hive/employee.txt -O ~/datasets/employe.csv
```

#### Check DataSets
```
[train@trainvm ~]$ ls -l ~/datasets/
```

### 7. Create database
```
0: jdbc:hive2://127.0.0.1:10000> create database company;
```

```
0: jdbc:hive2://127.0.0.1:10000> show databases;
```

```
0: jdbc:hive2://127.0.0.1:10000> use company;
```

### 8. Create table

0: jdbc:hive2://127.0.0.1:10000> 
```
create table employee (name string,work_place array<string>,gender_age struct <gender:string,age:int>,skills_score map<string,int>)
row format delimited
fields terminated by '|'
collection items terminated by ','
map keys terminated by ':'
stored as textfile
tblproperties('skip.header.line.count'='1');
```

#### Show table
```
show tables;
```

### 9. Send with put
```
[train@trainvm ~]$ hdfs dfs -put ~/datasets/employee.txt /user/train/hdfs_odev
```

```
0: jdbc:hive2://127.0.0.1:10000> load data inpath '/user/train/hdfs_odev/employee.txt' into table employee;
```

### 10. Write a query that returns the employees whose Python skill is greater than 70.

```
0: jdbc:hive2://127.0.0.1:10000> select * from employee where skills_score["Python"] > 70; 
```

### 11. Delete Databases

```
0: jdbc:hive2://127.0.0.1:10000> DROP DATABASE IF EXISTS company CASCADE;
```




