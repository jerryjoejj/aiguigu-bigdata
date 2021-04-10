package com.lma.partition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// 1、获取Job实例
		Job job = Job.getInstance(new Configuration());
		// 2、设置主类
		job.setJarByClass(FlowDriver.class);
		// 3、设置Mapper和Reducer类
		job.setMapperClass(FlowMapper.class);
		job.setReducerClass(FlowReducer.class);
		// 4、设置Mapper和Reducer输入输出类
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);

		job.setPartitionerClass(MyPartitioner.class);
		job.setNumReduceTasks(5);
		// 5、设置输入输出目录
		FileInputFormat.setInputPaths(job, new Path("D:\\project\\my_project\\input_data\\11_input\\inputflow"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\project\\my_project\\output_data\\flow11"));

		// 6、执行
		boolean b = job.waitForCompletion(true);
		System.exit(b ? 0 : 1);
	}
}
