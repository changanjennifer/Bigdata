package io.yhh.hadoop;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

// 1 实现WritableComparable接口
/**
 * @author JenniferYin
 *
 */
public class FlowBean  implements WritableComparable<FlowBean>  {

	// 上传流量
	private long upFlow;
	// 下载流量
	private long downFlow;
	// 流量总和
	private long sumFlow;

	// 必须要有，反序列化要调用空参构造器
	public FlowBean() {
	}

	public FlowBean(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	public void set(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}

	/**
	 * 序列化
	 *
	 * @param out
	 * @throws IOException
	 */
	//@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(upFlow);
		out.writeLong(downFlow);
		out.writeLong(sumFlow);
	}

	/**
	 * 反序列化 注：字段属性顺序必须一致
	 *
	 * @param in
	 * @throws IOException
	 */
	//@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}

	@Override
	public String toString() {
		return upFlow + "\t" + downFlow + "\t" + sumFlow;
	}

	public long getUpFlow() {
		return upFlow;
	}

	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}

	public long getDownFlow() {
		return downFlow;
	}

	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}

	public long getSumFlow() {
		return sumFlow;
	}
	public int compareTo(FlowBean o) {
		return Long.compare(o.sumFlow, this.sumFlow);
	}

	
	public static void main(String[] args) {
		String test1 ="1363157985066 	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com		24	27	2481	24681	200";
		String[] words = test1.split("\\s+");
		for (int i=0; i<words.length; i++) {
			System.out.println(i + " :" + words[i]);
		}
		//1,7,8
	}


}
