## Questions

- Download and put `https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv` dataset in hdfs `/user/train/hdfs_odev` directory.

- Copy this hdfs file `/user/train/hdfs_odev/Wine.csv` to `/tmp/hdfs_odev` hdfs directory.

- Delete `/tmp/hdfs_odev` directory with skipping the trash. 

- Explore `/user/train/hdfs_odev/Wine.csv` file from web hdfs.  
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------

## Answers

### 0. Start Hadoop services
```
[train@localhost ~]$ start-all.sh
```

### 1. Check to files
```
[train@localhost ~]$ hdfs dfs -ls /
```

```
[train@localhost ~]$ hdfs dfs -ls /user
```

```
[train@localhost ~]$ hdfs dfs -ls /user/train
```

### 2. Create new file
```
[train@localhost ~]$ hdfs dfs -mkdir /user/train/hdfs_odev
```

#### Check the file
```
[train@localhost ~]$ hdfs dfs -ls /user/train/
```

### 3. Download DataSets in local
```
[train@localhost ~]$ wget https://raw.githubusercontent.com/erkansirin78/datasets/master/Wine.csv -O ~/datasets/Wine.csv
```

#### Check DataSets
```
[train@localhost ~]$ ls -l ~/datasets/
```

### 4. Put DataSets from local to hdfs
```
[train@localhost ~]$ hdfs dfs -put ~/datasets/Wine.csv /user/train/hdfs_odev
```

#### Check the dataset
```
[train@localhost ~]$ hdfs dfs -ls  /user/train/hdfs_odev/Wine.csv
```

### 5. Look at the datasets
```
[train@localhost ~]$ hdfs dfs -head  /user/train/hdfs_odev/Wine.csv
```


### 6. Create new file in temp
```
[train@localhost ~]$ hdfs dfs -mkdir /tmp/hdfs_odev
```


### 7. Copies /user/train/hdfs_odev/Wine.csv` to `/tmp/hdfs_odev
```
[train@localhost ~]$ hdfs dfs -cp /user/train/hdfs_odev/Wine.csv /tmp/hdfs_odev/Wine.csv
```


#### Check the datasets
```
[train@localhost ~]$ hdfs dfs -ls /tmp/hdfs_odev/
```


### 8. Delete the datasets
```
[train@trainvm ~]$ hdfs dfs -rm -r /tmp/hdfs_odev
```

```
[train@localhost ~]$ hdfs dfs -ls /tmp/
```

### 9. We have to change the chmod of the tmp to Explore `/user/train/hdfs_odev/Wine.csv` file from web hdfs.
```
[train@localhost ~]$ hdfs dfs -chmod 777 /tmp
```

In Linux:

http://127.0.0.1:9870/explorer.html#/ 






