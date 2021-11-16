### 作业：编程实践，使用Java API操作HBase 

实践主要是建表，插入数据，删除数据，查询等功能。建立一个如下所示的表： 

表名：$your_name:student 

空白处自行填写, 姓名学号一律填写真实姓名和学号

#### pom.xml 配置

服务器版本为2.1.0（hbase版本和服务器上的版本可以不一致，但尽量保证一致）

```
 
<dependency>
<groupId>org.apache.hbase</groupId>
<artifactId>hbase-client</artifactId>
<version>2.1.0</version>
</dependency>
```

#### 代码实现

请参见HbaseTest.java程序





