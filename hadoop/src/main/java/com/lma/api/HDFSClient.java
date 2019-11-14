package com.lma.api;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;

public class HDFSClient {

	FileSystem fs;

	// 获取FileSystem对象
	@Before
	public void initFS() throws IOException, InterruptedException {
		fs = FileSystem.get(URI.create("hdfs://hadoop002:9000"), new Configuration(), "hadoop");
	}

	/**
	 * 向HDFS上传文件
	 * @throws IOException
	 */
	@Test
	public void put() throws IOException {
		fs.copyFromLocalFile(new Path("D:\\1.txt"), new Path("/"));
	}

	/**
	 * 从HDFS下载文件
	 */
	@Test
	public void get() throws IOException {
		fs.copyToLocalFile(new Path("/input"), new Path("D:\\2.txt"));
	}

	/**
	 * 删除文件件
	 * @throws IOException
	 */
	@Test
	public void delFolder() throws IOException {
		//递归删除
		fs.delete(new Path("/test001"), true);
	}

	/**
	 * 重命名文件
	 */
	@Test
	public void rename() throws IOException {
		fs.rename(new Path("/1.txt"), new Path("/100.txt"));
	}

	@Test
	public void testListFiles() throws IOException {
		RemoteIterator<LocatedFileStatus> files = fs.listFiles(new Path("/"), true);
		while (files.hasNext()) {
			System.out.println("=================================");
			LocatedFileStatus fileStatus = files.next();
			System.out.println("filName: " + fileStatus.getPath().getName());
			System.out.println("fileLen: " + fileStatus.getLen());

			// 文件块信息
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for (BlockLocation blockLocation : blockLocations) {
				String[] hosts = blockLocation.getHosts();
				for (String host : hosts) {
					System.out.print(host + " ");
				}
			}
		}
	}

	@Test
	public void testListStatus() throws IOException {
		FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
		for (FileStatus fileStatus: fileStatuses) {
			if(fileStatus.isFile()) {
				System.out.println("文件名为：" + fileStatus.getPath().getName());
			} else {
				System.out.println("文件夹名为：" + fileStatus.getPath().getName());
			}
		}
	}

	@After
	public void destoryFS() throws IOException {
		fs.close();
	}

}
