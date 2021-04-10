package com.lma.combinetextformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WcDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 1、获取一个job实例
		Job job = Job.getInstance(new Configuration());
		// 2、设置类路径
		job.setJarByClass(WcDriver.class);

		// 3、设置mapper和reducer类
		job.setMapperClass(WcMap.class);
		job.setReducerClass(WcReduce.class);

		// 4.设置Mapper和Reducer key,value输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setInputFormatClass(CombineTextInputFormat.class);
		CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);

		// 5、设置输入输出
		FileInputFormat.setInputPaths(job, new Path("D:\\project\\my_project\\input_data\\11_input\\inputcombinetextinputformat"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\project\\my_project\\output_data\\combine4"));

		job.waitForCompletion(true);
	}
}
