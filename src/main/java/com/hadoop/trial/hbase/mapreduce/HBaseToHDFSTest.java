package com.hadoop.trial.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 从HBase中读取记录并存入HDFS中
 * 
 * @author hadoop
 * 
 */
public class HBaseToHDFSTest
{
	/**
	 * HBase的Map分析
	 * 
	 * @author hadoop
	 * 
	 */
	public static class HBaseMapper extends TableMapper<Text, IntWritable>
	{
		@Override
		protected void map(ImmutableBytesWritable key, Result result, Context context) throws IOException,
				InterruptedException
		{
			byte[] name = result.getValue(Bytes.toBytes("inform"), Bytes.toBytes("name"));
			byte[] math = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("math"));
			byte[] english = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("english"));
			context.write(new Text(name), new IntWritable(Bytes.toInt(math)));
			context.write(new Text(name), new IntWritable(Bytes.toInt(english)));
		}
	}

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception
	{
		Configuration configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum", "10.10.141.1");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("fs.default.name", "hdfs://10.10.141.14:9000");
		configuration.set("mapreduce.framework.name", "yarn");
		configuration.set("yarn.resourcemanager.address", "10.10.141.14:8132");
		configuration = HBaseConfiguration.create(configuration);
		Job job = new Job(configuration, "import hbase to hdfs");
		job.setJarByClass(HBaseToHDFSTest.class);
		TableMapReduceUtil.initTableMapperJob("T_STUDENT", new Scan(), HBaseMapper.class, IntWritable.class,
				Text.class, job);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.10.141.14:9000/wangsheng/bbase-output5"));
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}