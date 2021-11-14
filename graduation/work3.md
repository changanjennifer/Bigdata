### 题目三：简述HDFS的读写流程

### HDFS写入文件的过程

1).使用HDFS提供的客户端开发库Client，向远程的Namenode发起RPC请求；

2).Namenode会检查要创建的文件是否已经存在，创建者是否有权限进行操作，成功则会为文件创建一个记录，否则会让客户端抛出异常；

3).当客户端开始写入文件的时候，开发库会将文件切分成多个packets，并在内部以数据队列"data queue"的形式管理这些packets，并向Namenode申请新的blocks，获取用来存储replicas的合适的datanodes列表，列表的大小根据在Namenode中对replication的设置而定。

4).开始以pipeline（管道）的形式将packet写入所有的replicas中。开发库把packet以流的方式写入第一个datanode，该datanode把该packet存储之后，再将其传递给在此pipeline中的下一个datanode，直到最后一个datanode，这种写数据的方式呈流水线的形式。

5).最后一个datanode成功存储之后会返回一个ack packet，在pipeline里传递至客户端，在客户端的开发库内部维护着"ack queue"，成功收到datanode返回的ack packet后会从"ack queue"移除相应的packet。

6).如果传输过程中，有某个datanode出现了故障，那么当前的pipeline会被关闭，出现故障的datanode会从当前的pipeline中移除，剩余的block会继续剩下的datanode中继续以pipeline的形式传输，同时Namenode会分配一个新的datanode，保持replicas设定的数量。

 

 

### HDFS文件读取的过程

1).使用HDFS提供的客户端开发库Client，向远程的Namenode发起RPC请求；

2).Namenode会视情况返回文件的部分或者全部block列表，对于每个block，Namenode都会返回有该block拷贝的DataNode地址；

3).客户端开发库Client会选取离客户端最接近的DataNode来读取block；如果客户端本身就是DataNode,那么将从本地直接获取数据.

4).读取完当前block的数据后，关闭与当前的DataNode连接，并为读取下一个block寻找最佳的DataNode；

5).当读完列表的block后，且文件读取还没有结束，客户端开发库会继续向Namenode获取下一批的block列表。

6).读取完一个block都会进行checksum验证，如果读取datanode时出现错误，客户端会通知Namenode，然后再从下一个拥有该block拷贝的datanode继续读。