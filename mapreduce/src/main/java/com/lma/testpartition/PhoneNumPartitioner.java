package com.lma.testpartition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class PhoneNumPartitioner extends Partitioner<FlowBean, Text> {
	@Override
	public int getPartition(FlowBean flowBean, Text text, int i) {
		String phoneNum = text.toString().substring(0, 3);
		switch (phoneNum) {
			case "136":
				return 0;
			case "137":
				return 1;
			case "138":
				return 2;
			case "139":
				return 3;
			default:
				return 4;
		}
	}
}
