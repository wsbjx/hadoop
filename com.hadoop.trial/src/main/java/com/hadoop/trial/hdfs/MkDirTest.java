package com.hadoop.trial.hdfs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * 在HDFS文件系统中创建路径
 * 
 * @author hadoop
 * 
 */
public class MkDirTest
{
	public static void main(String[] args) throws Exception
	{
		String uri = "hdfs://10.10.141.14:9000/wangsheng/test";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		fs.mkdirs(new Path(uri));
	}
}
