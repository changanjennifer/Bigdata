



1. hive_sql_test1.t_user  

   •t_user观众表共6000+条数据

   •字段为：UserID, Sex, Age, Occupation, Zipcode

   ```
   select UserID, Sex, Age, Occupation, Zipcode from hive_sql_test1.t_user  limit 100;
   ```

   

2. hive_sql_test1.t_movie

   •t_movie电影表共3000+条数据

   •字段为：MovieID, MovieName, MovieType

   ```
   select MovieID, MovieName, MovieType from hive_sql_test1.t_movie limit 100;
   ```

   

3. t_rating影评表100万+条数据

   

```
hive_sql_test1.t_rating
字段为：UserID, MovieID, Rate, Times
select UserID, MovieID, Rate, Times from hive_sql_test1.t_rating limit 100;
```



#### 题目一

简单：   展示电影ID为2116这部电影各年龄段的平均影评分

```
select u.age as age,avg(r.rate) as avgrate 
from hive_sql_test1.t_rating r 
left join hive_sql_test1.t_user u on r.userid = u.userid 
where r.movieid = 2166 
group by u.age;
```



执行结果：

![firstSQL-hue-resut1.png](https://github.com/changanjennifer/Bigdata/blob/main/0801homework/firstSQL-hue-resut1.png)

#### 题目二

中等：找出男性评分最高且评分次数超过50次的10部电影，展示电影名，平均影评分和评分次数

```
select 'M' as sex, m.moviename as name, avg(r.rate) as agvrate, count(r.movieid) as total  from  hive_sql_test1.t_rating r 
left join hive_sql_test1.t_user u on r.userid = u.userid
left join hive_sql_test1.t_movie m on r.movieid = m.movieid
where u.sex='M' 
group by m.moviename
having count(*)>50
order by agvrate desc, total desc
limit 10;
```



执行结果：

![secondSQL-hue-result-2.png](https://github.com/changanjennifer/Bigdata/blob/main/0801homework/secondSQL-hue-result-2.png)

成功的log

```
NFO  : Compiling command(queryId=hive_20210807203318_12a82626-e4b2-41d6-8aad-e0a5b8daef8d): select 'M' as sex, m.moviename as name, avg(r.rate) as agvrate, count(r.movieid) as total  from  hive_sql_test1.t_rating r 
left join hive_sql_test1.t_user u on r.userid = u.userid
left join hive_sql_test1.t_movie m on r.movieid = m.movieid
where u.sex='M' 
group by m.moviename
having count(*)>50
order by agvrate desc, total desc
limit 10
INFO  : Semantic Analysis Completed
INFO  : Returning Hive schema: Schema(fieldSchemas:[FieldSchema(name:sex, type:string, comment:null), FieldSchema(name:name, type:string, comment:null), FieldSchema(name:agvrate, type:double, comment:null), FieldSchema(name:total, type:bigint, comment:null)], properties:null)
INFO  : Completed compiling command(queryId=hive_20210807203318_12a82626-e4b2-41d6-8aad-e0a5b8daef8d); Time taken: 0.132 seconds
INFO  : Executing command(queryId=hive_20210807203318_12a82626-e4b2-41d6-8aad-e0a5b8daef8d): select 'M' as sex, m.moviename as name, avg(r.rate) as agvrate, count(r.movieid) as total  from  hive_sql_test1.t_rating r 
left join hive_sql_test1.t_user u on r.userid = u.userid
left join hive_sql_test1.t_movie m on r.movieid = m.movieid
where u.sex='M' 
group by m.moviename
having count(*)>50
order by agvrate desc, total desc
limit 10
WARN  : 
INFO  : Query ID = hive_20210807203318_12a82626-e4b2-41d6-8aad-e0a5b8daef8d
INFO  : Total jobs = 2
INFO  : Starting task [Stage-9:MAPREDLOCAL] in serial mode
INFO  : Execution completed successfully
INFO  : MapredLocal task succeeded
INFO  : Launching Job 1 out of 2
INFO  : Starting task [Stage-3:MAPRED] in serial mode
INFO  : Number of reduce tasks not specified. Estimated from input data size: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:1
INFO  : Submitting tokens for job: job_1628329820738_0163
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://jikehadoop02:8088/proxy/application_1628329820738_0163/
INFO  : Starting Job = job_1628329820738_0163, Tracking URL = http://jikehadoop02:8088/proxy/application_1628329820738_0163/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hadoop/bin/hadoop job  -kill job_1628329820738_0163
INFO  : Hadoop job information for Stage-3: number of mappers: 1; number of reducers: 1
INFO  : 2021-08-07 20:33:35,369 Stage-3 map = 0%,  reduce = 0%
INFO  : 2021-08-07 20:33:51,882 Stage-3 map = 100%,  reduce = 0%, Cumulative CPU 11.92 sec
INFO  : 2021-08-07 20:34:10,489 Stage-3 map = 100%,  reduce = 100%, Cumulative CPU 14.84 sec
INFO  : MapReduce Total cumulative CPU time: 14 seconds 840 msec
INFO  : Ended Job = job_1628329820738_0163
INFO  : Launching Job 2 out of 2
INFO  : Starting task [Stage-4:MAPRED] in serial mode
INFO  : Number of reduce tasks determined at compile time: 1
INFO  : In order to change the average load for a reducer (in bytes):
INFO  :   set hive.exec.reducers.bytes.per.reducer=<number>
INFO  : In order to limit the maximum number of reducers:
INFO  :   set hive.exec.reducers.max=<number>
INFO  : In order to set a constant number of reducers:
INFO  :   set mapreduce.job.reduces=<number>
INFO  : number of splits:1
INFO  : Submitting tokens for job: job_1628329820738_0164
INFO  : Executing with tokens: []
INFO  : The url to track the job: http://jikehadoop02:8088/proxy/application_1628329820738_0164/
INFO  : Starting Job = job_1628329820738_0164, Tracking URL = http://jikehadoop02:8088/proxy/application_1628329820738_0164/
INFO  : Kill Command = /opt/cloudera/parcels/CDH-6.3.2-1.cdh6.3.2.p0.1605554/lib/hadoop/bin/hadoop job  -kill job_1628329820738_0164
INFO  : Hadoop job information for Stage-4: number of mappers: 1; number of reducers: 1
INFO  : 2021-08-07 20:34:24,269 Stage-4 map = 0%,  reduce = 0%
INFO  : 2021-08-07 20:34:29,471 Stage-4 map = 100%,  reduce = 0%, Cumulative CPU 1.57 sec
INFO  : 2021-08-07 20:34:37,735 Stage-4 map = 100%,  reduce = 100%, Cumulative CPU 3.74 sec
INFO  : MapReduce Total cumulative CPU time: 3 seconds 740 msec
INFO  : Ended Job = job_1628329820738_0164
INFO  : MapReduce Jobs Launched: 
INFO  : Stage-Stage-3: Map: 1  Reduce: 1   Cumulative CPU: 14.84 sec   HDFS Read: 24610339 HDFS Write: 117206 HDFS EC Read: 0 SUCCESS
INFO  : Stage-Stage-4: Map: 1  Reduce: 1   Cumulative CPU: 3.74 sec   HDFS Read: 123556 HDFS Write: 753 HDFS EC Read: 0 SUCCESS
INFO  : Total MapReduce CPU Time Spent: 18 seconds 580 msec
INFO  : Completed executing command(queryId=hive_20210807203318_12a82626-e4b2-41d6-8aad-e0a5b8daef8d); Time taken: 80.334 seconds
INFO  : OK
查询历史记录  
保存的查询 
结果 (10)  
 
 	sex	name
```



#### 题目三（选做）

困难：找出影评次数最多的女士所给出最高分的10部电影的平均影评分，展示电影名和平均影评分（可使用多行SQL）





