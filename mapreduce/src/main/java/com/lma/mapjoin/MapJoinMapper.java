package com.lma.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

public class MapJoinMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

	private HashMap<String, String> pdMap = new HashMap<>();
	private Text text  = new Text();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {

		URI[] cacheFiles = context.getCacheFiles();
		Path path = new Path(cacheFiles[0]);
		FileSystem fileSystem = FileSystem.get(context.getConfiguration());
		FSDataInputStream fsDataInputStream = fileSystem.open(path);
		BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, "UTF-8"));
		String line;
		while (StringUtils.isNotEmpty(line = bufferedReader.readLine())) {
			String[] split = line.split("\t");
			pdMap.put(split[0], split[1]);
		}

		IOUtils.closeStream(bufferedReader);
	}

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] fields = value.toString().split("\t");
		String pName = pdMap.get(fields[1]);
		text.set(fields[0] + "\t" + pName + "\t" + fields[1]);
		context.write(text, NullWritable.get());
	}
}
