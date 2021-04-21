package com.lma.reducejoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable {

	public static String ORDER_FLAG = "order";
	public static String PD_FLAG = "pd";


	// 订单id
	private String orderId;
	// 商品id
	private String pId;
	// 商品名称
	private String pName;
	// 商品数量
	private int amount;
	// 标记
	private String flag;

	public String getOrderId() {
		return orderId;
	}

	public void setOrderId(String orderId) {
		this.orderId = orderId;
	}

	public String getpId() {
		return pId;
	}

	public void setpId(String pId) {
		this.pId = pId;
	}

	public String getpName() {
		return pName;
	}

	public void setpName(String pName) {
		this.pName = pName;
	}

	public int getAmount() {
		return amount;
	}

	public void setAmount(int amount) {
		this.amount = amount;
	}

	public String getFlag() {
		return flag;
	}

	public void setFlag(String flag) {
		this.flag = flag;
	}


	@Override
	public String toString() {
		return "TableBean{" +
				"orderId='" + orderId + '\'' +
				", pId='" + pId + '\'' +
				", pName='" + pName + '\'' +
				", amount=" + amount +
				", flag='" + flag + '\'' +
				'}';
	}

	/**
	 * 序列化写
	 * @param out
	 * @throws IOException
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(this.orderId);
		out.writeUTF(this.pId);
		out.writeUTF(this.pName);
		out.writeInt(this.amount);
		out.writeUTF(this.flag);
	}

	/**
	 * 序列化读
	 * @param in
	 * @throws IOException
	 */
	@Override
	public void readFields(DataInput in) throws IOException {
		this.orderId = in.readUTF();
		this.pId = in.readUTF();
		this.pName = in.readUTF();
		this.amount = in.readInt();
		this.flag = in.readUTF();
	}


}
