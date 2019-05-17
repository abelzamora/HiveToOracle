# hiveToOracle

This is an application to move data from Hive table to another Oracle table.

This project can be tested in local. The project contains a `docker-compose.test.yml` to start an image of Hive and another of Oracle, with the necessary requirements to move the data from Hive to Oracle 

## Starting the project in local environment
To start the docker images, just run the next command `/usr/local/bin/docker-compose -f docker-compose.test.yml up -d`

Once both DDBB are up and running you can access to the tables using the below configuration:

### Oracle

```properties
hostname: localhost
port: 1521
sid: xe
service name: xe
username: system
password: oracle
```

Before running the tests it is necessary to create this table:

```oraclesqlplus
CREATE TABLE students
			(
				name         VARCHAR(20),
                age NUMBER,
                gpa NUMBER
			)	;		
```

### Hive

Connect to the Hive cluster
```bash
bash -c "clear && docker exec -it export_hive_1 sh"
```

Create hdfs directory
```bash
hdfs dfs -mkdir /tmp/students;
```


Start beeline client to access to Hive tables
```bash
beeline -u 'jdbc:hive2://localhost:10000/default'
```

Create Hive table
```sql
 CREATE EXTERNAL TABLE `students`(                           
   `name` varchar(64),                              
   `age` int,                                       
   `gpa` decimal(3,2))                              
 ROW FORMAT SERDE                                   
   'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'  
 STORED AS INPUTFORMAT                              
   'org.apache.hadoop.mapred.TextInputFormat'       
 OUTPUTFORMAT                                       
   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat' 
 LOCATION                                           
   '/tmp/students' 
 TBLPROPERTIES (                                    
   'COLUMN_STATS_ACCURATE'='true',                  
   'numFiles'='1',                                  
   'numRows'='2',                                   
   'rawDataSize'='44',                              
   'totalSize'='46',                                
   'transient_lastDdlTime'='1550144008');
```

Add data to the Hive table
```sql
INSERT INTO TABLE students VALUES ('students_1', 1, 1.1), ('students_2', 2, 1.2), ('students_3', 3, 1.3), ('students_4', 4, 1.4);
```

https://hub.docker.com/r/bde2020/hive/