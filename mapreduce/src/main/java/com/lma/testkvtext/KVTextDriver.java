package com.lma.testkvtext;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class KVTextDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		String[] inputArgs = {"E:\\AppCache\\hadoop\\1.txt", "E:\\AppCache\\hadoop\\output"};
		Configuration configuration = new Configuration();
		// 设置分隔符
		configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, " ");
		Job job = Job.getInstance(configuration);
		job.setJarByClass(KVTextDriver.class);
		job.setMapperClass(KVTextMapper.class);
		job.setReducerClass(KVTextReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.setInputPaths(job, new Path(inputArgs[0]));
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(inputArgs[1]));
		job.waitForCompletion(true);
	}
}
