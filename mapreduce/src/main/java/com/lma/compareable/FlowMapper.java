package com.lma.compareable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
	private FlowBean flowBean = new FlowBean();
	private Text phoneNum = new Text();


	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		String[] line = value.toString().split("\t");
		long upFlow = Long.parseLong(line[line.length - 3]);
		long downFlow = Long.parseLong(line[line.length - 2]);
		flowBean.set(upFlow, downFlow);
		phoneNum.set(line[1]);
		context.write(flowBean, phoneNum);
	}
}
