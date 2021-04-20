package com.lma.outputformat;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

public class LogRecordWriter extends RecordWriter<Text, NullWritable> {
	private FSDataOutputStream atguiguOut;
	private FSDataOutputStream otherOut;

	public LogRecordWriter(TaskAttemptContext context) throws IOException {
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		// 创建流
		atguiguOut = fileSystem.create(new Path("D:\\project\\my_project\\output_data\\atguigu.log"));
		otherOut = fileSystem.create(new Path("D:\\project\\my_project\\output_data\\other.log"));


	}

	@Override
	public void write(Text key, NullWritable value) throws IOException, InterruptedException {
		String log = key.toString();
		if(log.contains("atguigu")) {
			atguiguOut.writeBytes(log + "\n");
		} else {
			otherOut.writeBytes(log + "\n");
		}
	}

	@Override
	public void close(TaskAttemptContext context) throws IOException, InterruptedException {
		IOUtils.closeStream(atguiguOut);
		IOUtils.closeStream(otherOut);
	}
}
