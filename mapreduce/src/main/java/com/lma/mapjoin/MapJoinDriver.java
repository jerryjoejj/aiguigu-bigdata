package com.lma.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class MapJoinDriver {
	public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance();

		job.setJarByClass(MapJoinDriver.class);
		job.setMapperClass(MapJoinMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		// 将文件加载到缓存
		job.addCacheFile(new URI("file:///D:/project/my_project/input_data/11_input/tablecache/pd.txt"));
		// 设置不需要Reduce阶段
		job.setNumReduceTasks(0);
		FileInputFormat.setInputPaths(job, new Path("D:\\project\\my_project\\input_data\\11_input\\inputtable2"));
		FileOutputFormat.setOutputPath(job, new Path("D:\\11"));
		job.waitForCompletion(true);
	}
}
