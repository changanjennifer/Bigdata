#### Week03-作业要求

用Hadoop mapReduce 类库实现手机流量统计，按总流量逆序排列

##### 1.需求：

统计每一个手机号耗费的总上行流量、下行流量、总流量，

##### 2.数据准备：

(1)输入数据格式：

时间戳、电话号码、基站的物理地址、访问网址的ip、网站域名、数据包、接包数、上行/传流量、下行/载流量、响应码

(2)最终输出的数据格式：

手机号码    上行流量    下行流量     总流量

按总流量倒序



##### 3. 实现

##### 流量统计对象FlowBean

定义流量统计的对象FlowBean, 因为Hadoop默认的排序规则为字典规则，所以实现WritableComparable接口，重写compareTo(),以总流量逆序排列

```
	// 流量总和
	private long sumFlow;
	
	public int compareTo(FlowBean o) {
		return Long.compare(o.sumFlow, this.sumFlow);
	}
```



###### 统计Job :Map阶段

-   读入一行数据，切分字段，
-   抽取手机号、上行流量、下行流量。
-   以手机号为key,  流量对象(总流量=上行+下行) 为value输出

###### 统计Job: Reduce阶段

- 累加上、下行流量，得到总流量。
- 以FlowBean来封装流量信息，并将FlowBean作为map输出的key来传输。目的是利用FlowBean的排序属性

