### SparkCode 编程作业



### 作业一： 使用RDD API实现带词频的倒排索倒排索引

倒排索引（Inverted index），也被称为反向索引。它是文档检索系统中最常用的数据结构。被广泛 地应用于全文搜索引擎。 例子如下，被索引的文件为（0，1，2代表文件名。

生成类似于如下形式的输出：

“单词”: {(文件名1, 本文件词频)， （文件名2, 本文件词频)......(文件名n, 本文件词频)}



### 作业二 Distcp的spark实现



使用Spark实现Hadoop 分布式数据传输工具 DistCp (distributed copy)，只要求实现最基础的copy功 能，对于-update、-diff、-p不做要求 

对于HadoopDistCp的功能与实现，可以参考 

https://hadoop.apache.org/docs/current/hadoop-distcp/DistCp.html 

https://github.com/apache/hadoop/tree/release-2.7.1/hadoop-tools/hadoop-distcp 

Hadoop使用MapReduce框架来实现分布式copy，在Spark中应使用RDD来实现分布式copy 应实现的功能为： 

sparkDistCp hdfs://xxx/source hdfs://xxx/target 

得到的结果为，启动多个task/executor，将hdfs://xxx/source目录复制到hdfs://xxx/target，得到 hdfs://xxx/target/source 

需要支持source下存在多级子目录 需支持-i Ignore failures 参数 

需支持-m max concurrence参数，控制同时copy的最大并发task数



### 编程中的注意事项

1.  老师提醒， 读取文件时，不建议用sc.wholeTextFiles(）, 这个方法一次性将文件加载到内存，当遇到大文件时，很容易造成内存溢出。

2.  第二个练习中，现在对target目录的做法是用String.replace实现的，输入的Input/output必须要是全路径，不能是相对路径，不然就会发现这个语句不起作用的情况。

3.  用Apache commons-cli做输入参数的解析。

   
