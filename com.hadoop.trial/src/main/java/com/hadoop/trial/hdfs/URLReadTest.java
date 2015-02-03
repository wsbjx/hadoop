package com.hadoop.trial.hdfs;

import java.io.InputStream;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.io.IOUtils;

/**
 * HDFS URL试验
 * 
 * @author hadoop
 * 
 */
public class URLReadTest
{

	public static void main(String[] args)
	{
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		InputStream is = null;
		try
		{
			is = new URL("hdfs://10.10.141.14:9000/wangsheng/Test.java").openStream();
			IOUtils.copyBytes(is, System.out, 4096, false);
		}
		catch (Exception e)
		{
			e.printStackTrace();
			IOUtils.closeStream(is);
		}
	}
}
