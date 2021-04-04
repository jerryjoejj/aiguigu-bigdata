package com.lma.flow;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlowBean implements Writable {

	// 上行流量
	private long upFlow;
	// 下行流量
	private long downFlow;
	// 总流量
	private long sumFlow;

	/**
	 *  空参构造
	 */
	public FlowBean() {
	}

	public void set(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = this.upFlow + this.downFlow;
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

	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}

	@Override
	public String toString() {
		return this.upFlow + "\t" + this.downFlow + "\t" + this.sumFlow;
	}

	/**
	 * 重写序列化方法
	 * @param out 序列化输入
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(this.upFlow);
		out.writeLong(this.downFlow);
		out.writeLong(this.sumFlow);
	}

	/**
	 * 重写反序列化方法
	 * @param in 反序列化输入
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.upFlow = in.readLong();
		this.downFlow = in.readLong();
		this.sumFlow = in.readLong();
	}
}
