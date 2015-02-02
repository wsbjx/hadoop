package com.hadoop.trial.mapreduce;

import java.io.IOException;
import java.util.Date;

import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapReduceTest extends Configured implements Tool
{
	static Logger logger = LoggerFactory.getLogger(MapReduceTest.class);

	enum Counter
	{
		LINESKIP,
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private static final IntWritable one = new IntWritable(1);

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			String line = value.toString();
			try
			{
				String[] lineSplit = line.split(",");
				if (lineSplit.length < 5)
				{
					return;
				}
				String requestUrl = lineSplit[4];
				int lastIndex = requestUrl.lastIndexOf(' ');
				if (lastIndex < 0)
				{
					return;
				}
				logger.info("gagaga: " + line);
				requestUrl = requestUrl.substring(requestUrl.indexOf(' ') + 1, lastIndex);
				Text out = new Text(requestUrl + "-gagaga");
				context.write(out, one);
			}
			catch (Exception e)
			{
				context.getCounter(Counter.LINESKIP).increment(1);
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		@SuppressWarnings("unused")
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException
		{
			int count = 0;
			for (IntWritable v : values)
			{
				count = count + 1;
			}
			try
			{
				context.write(key, new IntWritable(count));
			}
			catch (InterruptedException e)
			{
				e.printStackTrace();
			}
		}
	}

	@Override
	public int run(String[] args) throws Exception
	{
		String classpath = "$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/share/hadoop/common/*,$HADOOP_COMMON_HOME/share/hadoop/common/lib/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/*,$HADOOP_HDFS_HOME/share/hadoop/hdfs/lib/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/*,$HADOOP_MAPRED_HOME/share/hadoop/mapreduce/lib/*,$HADOOP_YARN_HOME/share/hadoop/yarn/*,$HADOOP_YARN_HOME/share/hadoop/yarn/lib/*";
		Job job = Job.getInstance();
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.address", "10.10.141.14:8132");
		conf.set("fs.default.name", "hdfs://10.10.141.14:9000");
		conf.set("yarn.resourcemanager.scheduler.address", "10.10.141.14:8130");
		conf.set("yarn.application.classpath", classpath);
		job.setJobName(MapReduceTest.class.getName());
		job.setJarByClass(MapReduceTest.class);
		FileInputFormat.addInputPath(job, new Path("/wangsheng/tomcat.log"));
		String date = DateFormatUtils.format(new Date(), "yyyyMMddHHmmss");
		FileOutputFormat.setOutputPath(job, new Path("/wangsheng/output/" + date));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception
	{
		int res = ToolRunner.run(new Configuration(), new MapReduceTest(), args);
		System.exit(res);
	}
}