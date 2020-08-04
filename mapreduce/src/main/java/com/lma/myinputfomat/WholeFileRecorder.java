package com.lma.myinputfomat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileRecorder extends RecordReader<Text, BytesWritable> {

	private boolean isRead = false;
	private Text key = new Text();
	private BytesWritable value =  new BytesWritable();
	private FSDataInputStream dataInputStream;
	private FSDataInputStream inputStream;
	private FileSplit fileSplit;

	/**
	 * 初始化方法
	 * @param inputSplit
	 * @param taskAttemptContext
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
			throws IOException, InterruptedException {
		fileSplit = (FileSplit) inputSplit;
		Path path = fileSplit.getPath();
		Configuration conf = taskAttemptContext.getConfiguration();
		FileSystem fileSystem = path.getFileSystem(conf);
		inputStream = fileSystem.open(path);
	}

	/**
	 * 读取下一个KV值
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!isRead) {
			// 开流读取
			isRead = true;
			byte[] buffer = new byte[(int)fileSplit.getLength()];
			IOUtils.readFully(inputStream, buffer, 0, buffer.length);
			String name = fileSplit.getPath().toString();
			key.set(name);
			return true;
		} else {
			return false;
		}
	}

	@Override
	public Text getCurrentKey() throws IOException, InterruptedException {
		return key;
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException, InterruptedException {
		return value;
	}

	/**
	 * 当前读取的状态
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public float getProgress() throws IOException, InterruptedException {
		// 读完返回1，没读返回0
		return isRead ? 1 : 0;
	}

	@Override
	public void close() throws IOException {
		IOUtils.closeStream(inputStream);
	}
}
