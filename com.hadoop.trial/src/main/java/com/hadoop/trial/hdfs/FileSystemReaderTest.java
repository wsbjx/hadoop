package com.hadoop.trial.hdfs;

import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * 访问HDFS文件系统中的文件
 * 
 * @author hadoop
 * 
 */
public class FileSystemReaderTest
{
	public static void main(String[] args) throws Exception
	{
		String uri = "hdfs://10.10.141.14:9000/wangsheng/Test.java";
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		InputStream is = null;
		try
		{
			is = fs.open(new Path(uri));
			IOUtils.copyBytes(is, System.out, 4096, false);
		}
		finally
		{
			IOUtils.closeStream(is);
		}
	}
}
