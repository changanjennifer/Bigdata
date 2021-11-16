### 1017 Flink作业



完成功能代码 • IDE中找到SpendReport.java • 实现方法public static Table report(Table transactions) • 例如直接什么都不做，把source直接写入sink： public static Table report(Table transactions) { return transactions; }

源码介绍

```java
tEnv.executeSql("CREATE TABLE transactions (\n" +
" account_id BIGINT,\n" +
" amount BIGINT,\n" +
" transaction_time TIMESTAMP(3),\n" +
" WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
") WITH (\n" +
" 'connector' = 'kafka',\n" +
" 'topic' = 'transactions',\n" +
" 'properties.bootstrap.servers' = 'kafka:9092',\n" +
" 'format' = 'csv'\n" +
")");

```

输入表transaction，用于读取信用卡交易数据，其中包含账户ID(account_id)，美元金额和时间戳

```java
tEnv.executeSql("CREATE TABLE spend_report (\n" +
" account_id BIGINT,\n" +
" log_ts TIMESTAMP(3),\n" +
" amount BIGINT\n," +
" PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
") WITH (\n" +
" 'connector' = 'jdbc',\n" +
" 'url' = 'jdbc:mysql://mysql:3306/sql-demo',\n" +
" 'table-name' = 'spend_report',\n" +
" 'driver' = 'com.mysql.jdbc.Driver',\n" +
" 'username' = 'sql-demo',\n" +
" 'password' = 'demo-sql'\n" +
")");
```

输出表spend_report存储聚合结果，是mysql表



### 程序实现：

1. 实现程序

   flink-playgrounds\table-walkthrough\src\main\java\org\apache\flink\playgrounds\spendreportSpendReport.java

2. 测试程序：flink-playgrounds\table-walkthrough\src\test\java\org\apache\flink\playgrounds\spendreportSpendReportTest.java

