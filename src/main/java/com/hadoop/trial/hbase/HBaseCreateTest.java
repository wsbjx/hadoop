package com.hadoop.trial.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseCreateTest
{
	public static void main(String[] args) throws IOException
	{
		Configuration configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum", "10.10.141.1");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration = HBaseConfiguration.create(configuration);
		Connection connection = ConnectionFactory.createConnection(configuration);
		Admin admin = connection.getAdmin();
		TableName tableName = TableName.valueOf("T_STUDENT");
		String[] familys = { "inform", "score" };
		if (admin.tableExists(tableName))
		{
			System.out.println("table:" + tableName + " exist!");
		}
		else
		{
			HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
			for (int i = 0; i < familys.length; i++)
			{
				tableDescriptor.addFamily(new HColumnDescriptor(familys[i]));
			}
			// 创建表
			admin.createTable(tableDescriptor);
			System.out.println("create table " + tableName + " successful!");
			// 加入数据
			Table table = connection.getTable(tableName);
			for (int i = 0; i < 100; i++)
			{
				Put put = new Put(Bytes.toBytes("id-" + i));
				put.add(Bytes.toBytes("inform"), Bytes.toBytes("name"), Bytes.toBytes("学生" + i));
				put.add(Bytes.toBytes("inform"), Bytes.toBytes("age"), Bytes.toBytes(20 + i));
				put.add(Bytes.toBytes("inform"), Bytes.toBytes("remark"), Bytes.toBytes("我们的队伍向太阳,gagaga!"));
				put.add(Bytes.toBytes("score"), Bytes.toBytes("math"), Bytes.toBytes(100 + i));
				put.add(Bytes.toBytes("score"), Bytes.toBytes("english"), Bytes.toBytes(90 + i));
				table.put(put);
			}
		}
	}
}
