package com.lma.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

	private String fileName;
	private Text outK = new Text();
	private TableBean outV = new TableBean();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		FileSplit split = (FileSplit) context.getInputSplit();
		fileName = split.getPath().getName();
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		if (fileName.contains(TableBean.ORDER_FLAG)) {
			String[] line = value.toString().split("\t");
			outK.set(line[1]);
			outV.setOrderId(line[0]);
			outV.setpId(line[1]);
			outV.setAmount(Integer.parseInt(line[2]));
			outV.setpName("");
			outV.setFlag(TableBean.ORDER_FLAG);
		} else {
			String[] line = value.toString().split("\t");
			outK.set(line[0]);
			outV.setOrderId("");
			outV.setpId(line[0]);
			outV.setAmount(0);
			outV.setpName(line[1]);
			outV.setFlag(TableBean.PD_FLAG);
		}

		context.write(outK, outV);
	}
}
