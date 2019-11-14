package com.lma.testmapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WcMap extends Mapper<LongWritable, Text, Text, IntWritable> {

	private Text word = new Text();
	private IntWritable one = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] words = value.toString().split(" ");
		for(String word : words) {
			this.word.set(word);
			context.write(this.word, this.one);
		}
	}
}
