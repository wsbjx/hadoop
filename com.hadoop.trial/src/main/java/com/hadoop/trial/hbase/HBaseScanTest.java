package com.hadoop.trial.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseScanTest
{
	public static void main(String[] args) throws IOException
	{
		Configuration configuration = new Configuration();
		configuration.set("hbase.zookeeper.quorum", "10.10.141.3");
		configuration.set("hbase.zookeeper.property.clientPort", "2181");
		configuration = HBaseConfiguration.create(configuration);
		Connection connection = ConnectionFactory.createConnection(configuration);
		Table table = connection.getTable(TableName.valueOf("T_STUDENT"));
		Scan scan = new Scan();
		ResultScanner resultScanner = null;
		try
		{
			resultScanner = table.getScanner(scan);
			for (Result r : resultScanner)
			{
				for (Cell cell : r.listCells())
				{
					System.out.println("row:" + Bytes.toString(CellUtil.cloneRow(cell)));
					System.out.println("family:" + Bytes.toString(CellUtil.cloneFamily(cell)));
					System.out.println("qualifier:" + Bytes.toString(CellUtil.cloneQualifier(cell)));
					System.out.println("value:" + Bytes.toString(CellUtil.cloneValue(cell)));
					System.out.println("timestamp:" + cell.getTimestamp());
					System.out.println("-------------------------------------------");
				}
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
		finally
		{
			resultScanner.close();
		}
	}
}
