package com.hadoop.trial.mapreduce;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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

public class MapReduceTest extends Configured implements Tool
{
	static Log log = LogFactory.getLog(MapReduceTest.class);

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
				log.info("value: " + line);
				requestUrl = requestUrl.substring(requestUrl.indexOf(' ') + 1, lastIndex);
				Text out = new Text(requestUrl);
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
		Configuration conf = getConf();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resourcemanager.address", "10.10.141.14:8132");
		conf.set("fs.default.name", "hdfs://10.10.141.14:9000");
		Job job = Job.getInstance(conf, MapReduceTest.class.getName());
		job.setJarByClass(MapReduceTest.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://10.10.141.14:9000/wangsheng/tomcat.log"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://10.10.141.14:9000/wangsheng/output/test5"));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		// keep the same format with the output of Map and Reduce
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