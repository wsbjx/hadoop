package com.hadoop.trial.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseGetTest
{
	public static void main(String[] args) throws IOException
	{
		Configuration configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum", "10.10.141.1");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration = HBaseConfiguration.create(configuration);
		Connection connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf("T_STUDENT"));
		String rowId = "id-15";
		Get get = new Get(Bytes.toBytes(rowId)).addColumn(Bytes.toBytes("inform"), Bytes.toBytes("name"))
				.addColumn(Bytes.toBytes("inform"), Bytes.toBytes("age"))
				.addColumn(Bytes.toBytes("inform"), Bytes.toBytes("remark"))
				.addColumn(Bytes.toBytes("score"), Bytes.toBytes("math"))
				.addColumn(Bytes.toBytes("score"), Bytes.toBytes("english"));
		Result result = table.get(get);
		byte[] name = result.getValue(Bytes.toBytes("inform"), Bytes.toBytes("name"));
		byte[] age = result.getValue(Bytes.toBytes("inform"), Bytes.toBytes("age"));
		byte[] remark = result.getValue(Bytes.toBytes("inform"), Bytes.toBytes("remark"));
		byte[] math = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("math"));
		byte[] english = result.getValue(Bytes.toBytes("score"), Bytes.toBytes("english"));
		System.out.println("rowid: " + rowId);
		System.out.println("name=" + Bytes.toString(name));
		System.out.println("age=" + Bytes.toInt(age));
		System.out.println("remark=" + Bytes.toString(remark));
		System.out.println("math=" + Bytes.toInt(math));
		System.out.println("english=" + Bytes.toInt(english));
	}
}
