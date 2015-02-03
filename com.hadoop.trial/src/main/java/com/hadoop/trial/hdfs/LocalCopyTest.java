package com.hadoop.trial.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Stack;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

/**
 * 将客户端文件拷贝到HDFS文件系统中
 * 
 * @author hadoop
 * 
 */
public class LocalCopyTest
{
	public static void main(String[] args) throws Exception
	{
		String hdfsPath = "hdfs://10.10.141.14:9000/wangsheng/";
		String localPath = "f:/test";
		Configuration conf = new Configuration();
		File localDir = new File(localPath);
		Stack<File> stack = new Stack<File>();
		stack.push(localDir);
		while (!stack.isEmpty())
		{
			File dir = stack.pop();
			File[] files = dir.listFiles();
			for (final File file : files)
			{
				if (file.isDirectory())
				{
					stack.push(file);
				}
				else if (file.isFile())
				{
					InputStream is = new FileInputStream(file);
					String uri = hdfsPath + file.getName();
					FileSystem fs = FileSystem.get(URI.create(uri), conf);
					OutputStream os = fs.create(new Path(uri), new Progressable()
					{
						@Override
						public void progress()
						{
							System.out.println("File: " + file.getAbsolutePath() + " uploaded successful!");
						}
					});
					IOUtils.copyBytes(is, os, 4096, true);
				}
			}
		}
	}
}
