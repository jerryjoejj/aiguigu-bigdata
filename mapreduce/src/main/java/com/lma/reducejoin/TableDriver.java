package com.lma.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TableDriver {

	public static void main(String[] args) throws Exception {
		// 1、获取一个job实例
		Job job = Job.getInstance(new Configuration());
		// 2、设置类路径
		job.setJarByClass(TableDriver.class);

		// 3、设置mapper和reducer类
		job.setMapperClass(TableMapper.class);
		job.setReducerClass(TableReducer.class);

		// 4.设置Mapper和Reducer key,value输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(TableBean.class);
		job.setOutputKeyClass(TableBean.class);
		job.setMapOutputValueClass(NullWritable.class);
		// 5、设置输入输出
		FileInputFormat.setInputPaths(job, new Path(""));
		// 存储_success文件
		FileOutputFormat.setOutputPath(job, new Path(""));

		job.waitForCompletion(true);
	}
}
