### 题目二：架构设计题 

你是某互联网公司的大数据平台架构师，请设计一套基于 Lambda 架构的数据平台架构，要 求尽可能多的把课程中涉及的组件添加到该架构图中。并描述 Lambda 架构的优缺点，要求 不少于 300 字。



### 应用于互联网公司的大数据架构

参见下图：

![bigdata-lambda](https://github.com/changanjennifer/Bigdata/blob/main/graduation/bigdata-lambda.png)



### Lambda架构及其优缺点

​       Lambda架构是由Storm的作者Nathan Marz提出的一个实时大数据处理框架。Lambda架构的目标是设计出一个能满足实时大数据系统关键特性的架构，包括有：高容错、低延时和可扩展等。Lambda架构整合离线计算和实时计算，融合不可变性（Immunability），读写分离和复杂性隔离等一系列架构原则，可集成Hadoop，Kafka，Storm，Spark，Hbase等各类大数据组件。

#### Lambda架构

Lambda架构分为三层：

1. Batch Layer（批处理层）： 可以很好的处理离线数据，但有很多场景数据不断实时生成，并且需要实时查询处理。特点是可处理全体数据集合，耗时较长。预查询的过程是一个批处理的过程，因此，这一层可以选择诸如Hadoop这样的组件。
2. Speed Layer（流处理层）：用来处理增量的实时数据，即最近的增量数据流，延迟小，处理速度快。
3. Serving Layer: 用于响应用户的查询请求，合并Batch View和Realtime View中的结果数据集到最终的数据集。

下图给出了Lambda架构的一个完整视图和流程。

![all-view](https://github.com/changanjennifer/Bigdata/blob/main/graduation/all-view.png)



数据流进入系统后，同时发往Batch Layer和Speed Layer处理。Batch Layer以不可变模型离线存储所有数据集，通过在全体数据集上不断重新计算构建查询所对应的Batch Views。Speed Layer处理增量的实时数据流，不断更新查询所对应的Realtime Views。Serving Layer响应用户的查询请求，合并Batch View和Realtime View中的结果数据集到最终的数据集。



#### Lambda架构的优点：

- 容错性。Speed Layer中处理的数据也不断写入Batch Layer，当Batch Layer中重新计算的数据集包含Speed Layer处理的数据集后，当前的实时View就可以丢弃，这也就意味着Speed Layer处理中引入的错误，在Batch Layer重新计算时都可以得到修正。这点也可以看成是CAP理论中的最终一致性（Eventual Consistency）的体现。
- 复杂性隔离。Batch Layer处理的是离线数据，可以很好的掌控。Speed Layer采用增量算法处理实时数据，复杂性比Batch Layer要高很多。通过分开Batch Layer和Speed Layer，把复杂性隔离到Speed Layer，可以很好的提高整个系统的鲁棒性和可靠性。
- 可扩展性。Lambda体系架构的设计层是作为分布式系统被构建的。因此，通过简单地添加更多的主机，最终用户可以轻松地对系统进行水平扩展。
- 通用性。由于Lambda体系架构是一般范式，因此用户并不会被锁定在计算批处理视图的某个特定方式中。而且批处理视图和速度层的计算，可以被设计为满足某个数据系统的特定需求。
- 延展性。随着新的数据类型被导入，数据系统也会产生新的视图。数据系统不会被锁定在某类、或一定数量的批处理视图中。新的视图会在完成编码之后，被添加到系统中，其对应的资源也会得到轻松地延展。
- 按需查询。如有必要，批处理层可以在缺少批处理视图时，支持临时查询。如果用户可以接受临时查询的高延迟，那么批处理层的用途就不仅限于生成的批处理视图了。
- 低延迟的读取和更新。在Lambda体系架构中，速度层为大数据系统提供了对于最新数据集的实时查询。

#### Lambda架构缺点

- 数据维护成本高：由于所有数据都是被追加进来，并且批处理层中的任何数据都不会被丢弃，因此系统的扩展成本必然会随着时间的推移而增长。

- 复杂性：维护批流两套系统：由于批处理层和speed layer往往采用不同的大数据组件，显然，这两层虽然运行同一组数据，但是它们是在完全不同的系统上构建的。因此，用户需要维护两套相互独立的系统代码。这样不但复杂，而且极具一定的挑战性。

  

