package com.lma.outputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class LogDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

		// 1、获取一个job实例
		Job job = Job.getInstance(new Configuration());
		// 2、设置类路径
		job.setJarByClass(LogDriver.class);

		// 3、设置mapper和reducer类
		job.setMapperClass(LogMapper.class);
		job.setReducerClass(LogReducer.class);

		// 4.设置Mapper和Reducer key,value输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 5、设置输入输出
		job.setOutputFormatClass(LogOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("D:\\project\\my_project\\input_data\\11_input\\inputoutputformat"));
		// 存储_success文件
		FileOutputFormat.setOutputPath(job, new Path("D:\\1w1"));

		job.waitForCompletion(true);
	}
}
