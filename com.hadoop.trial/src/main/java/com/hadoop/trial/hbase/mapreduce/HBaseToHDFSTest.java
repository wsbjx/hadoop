package com.hadoop.trial.hbase.mapreduce;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;
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

	public static void main(String[] args) throws Exception
	{
		String classpath = "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/share/hadoop/common/*,$HADOOP_COMMON_HOME/share/hadoop/common/lib/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*,$HADOOP_YARN_HOME/share/hadoop/yarn/*,$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*";
		Configuration configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum", "10.10.141.1");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration.set("mapreduce.framework.name", "yarn");
		configuration.set("yarn.resourcemanager.address", "10.10.141.14:8132");
		configuration.set("fs.default.name", "hdfs://10.10.141.14:9000");
		configuration.set("yarn.resourcemanager.scheduler.address", "10.10.141.14:8130");
		configuration.set("yarn.application.classpath", classpath);
		configuration = HBaseConfiguration.create(configuration);
		Job job = Job.getInstance(configuration, HBaseToHDFSTest.class.getName());
		job.setJarByClass(HBaseToHDFSTest.class);
		TableMapReduceUtil.initTableMapperJob("T_STUDENT", new Scan(), HBaseMapper.class, IntWritable.class,
				Text.class, job);
		String date = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
		FileOutputFormat.setOutputPath(job, new Path("/wangsheng/output/HBaseToHDFSTest" + date));
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}

}