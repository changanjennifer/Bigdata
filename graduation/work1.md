###  题目一: 分析一条 TPCDS SQL（请基于 Spark 3.1.1 版本解答）

要求使用 tpcds 执行spark SQL 

1. 运行该SQL, 并截图该SQL的SQL 执行图。
2. 该SQL 用到了那些优化规则，(optimizer rules)
3. 请各用不少于200字描述其中的两条优化规则。



选择使用q73.sql,  

### 执行的shell命令：

```shell
nohup
./spark-3.1.1-bin-hadoop2.7/bin/spark-submit \
--conf spark.sql.planChangeLog.level=WARN \
--class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark \
--jars spark-core_2.12-3.1.1-tests.jar,spark-catalyst_2.12-3.1.1-tests.jar \
spark-sql_2.12-3.1.1-tests.jar \
--data-location tpcds-data-1g --query-filter "q73"\
> running.log 2>&1 &
```

q73.sql的sql如下：

```sql
SELECT
  c_last_name,
  c_first_name,
  c_salutation,
  c_preferred_cust_flag,
  ss_ticket_number,
  cnt
FROM
  (SELECT
    ss_ticket_number,
    ss_customer_sk,
    count(*) cnt
  FROM store_sales, date_dim, store, household_demographics
  WHERE store_sales.ss_sold_date_sk = date_dim.d_date_sk
    AND store_sales.ss_store_sk = store.s_store_sk
    AND store_sales.ss_hdemo_sk = household_demographics.hd_demo_sk
    AND date_dim.d_dom BETWEEN 1 AND 2
    AND (household_demographics.hd_buy_potential = '>10000' OR
    household_demographics.hd_buy_potential = 'unknown')
    AND household_demographics.hd_vehicle_count > 0
    AND CASE WHEN household_demographics.hd_vehicle_count > 0
    THEN
      household_demographics.hd_dep_count / household_demographics.hd_vehicle_count
        ELSE NULL END > 1
    AND date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
    AND store.s_county IN ('Williamson County', 'Franklin Parish', 'Bronx County', 'Orange County')
  GROUP BY ss_ticket_number, ss_customer_sk) dj, customer
WHERE ss_customer_sk = c_customer_sk
  AND cnt BETWEEN 1 AND 5
ORDER BY cnt DESC
```

###  SQL 执行图如下:

![sql-exec1](https://github.com/changanjennifer/Bigdata/blob/main/graduation/sql-exec1.png)



![sql-exec2](https://github.com/changanjennifer/Bigdata/blob/main/graduation/sql-exec2.png)

![sql-exec3](https://github.com/changanjennifer/Bigdata/blob/main/graduation/sql-exec3.png)

执行汇总信息

![sql-exec4](https://github.com/changanjennifer/Bigdata/blob/main/graduation/sql-exec4.png)



### 该SQL 执行中用到了如下的优化规则

1. 常量折叠 Constant Folding

2. 约束条件提取 InferFiltersFromConstraints

3. 谓词下推 PushDownPredicates

4. 将特点子查询转换为 left-semi/anti-join    RewritePredicateSubquery

5. 列裁剪 ColumnPruning

6. Null提取 NullPropagation

7. Join顺序优化 ReorderJoin

   

### 常量折叠Constant Folding：

 常量折叠是在编译阶段就识别和计算常量表达式的过程，而不是将常量表达式放到运行阶段再去做计算。这是许多SQL优化的规则，其优点是显而易见的，直接给出结果而不是每一行都重复去计算表达式，能减少对计算资源的浪费，提高执行速度。

```sql
date_dim.d_year IN (1999, 1999 + 1, 1999 + 2)
```

这里通过常量折叠，可以预先将其转化为 1999，2000，2001

产生的过程为：

```
=== Applying Rule org.apache.spark.sql.catalyst.optimizer.ConstantFolding ===
 OverwriteByExpression RelationV2[] noop-table, true, true  
 ......
 +- Filter (((d_dom#338 >= 1) AND (d_dom#338 <= 2)) AND d_year#335 IN (1999,(1999 + 1),(1999 + 2))) 
```

优化后

```
 +- Filter (((d_dom#338 >= 1) AND (d_dom#338 <= 2)) AND d_year#335 IN (1999,2000,2001))  

```



### 谓词下推 PushDownPredicates

如果能够将SQL语句中的谓词逻辑(where条件等）都尽量提前执行，下游处理已经过滤完毕的数据，能够减少工作量。可先对两个表进行Filter再进行join ， 当Filter可以过滤掉大部分数据时，参与join的数据量会大大减少，从而使得Join操作速度大大提高。

LogicPlan的优化，从逻辑上保证了将Filter下推后由于参与Join的数据量变少而提高了性能。另一方面，在物理层面，Filter下推后，对于支持Filter下推后的村粗，并不需要将表的全量数据扫描出来再过滤，而只是直接扫描符合Filter条件的数据，从而在物理层面极大减少了扫描表的开销。提高了了执行速度。