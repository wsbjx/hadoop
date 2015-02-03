package com.hadoop.trial.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class HbaseCommons
{

	static Configuration conf = HBaseConfiguration.create();
	static String tableName = "";

	public static void main(String[] args) throws Exception
	{

		// String tableName="test";
		// createTable(tableName, null);

	}

	/**
	 * 批量添加数据
	 * 
	 * @param tableName
	 *            标名字
	 * @param rows
	 *            rowkey行健的集合 本方法仅作示例，其他的内容需要看自己义务改变
	 * 
	 * **/

	public static void insertList(String tableName, String rows[]) throws Exception
	{
		HTable table = new HTable(conf, tableName);
		List<Put> list = new ArrayList<Put>();
		for (String r : rows)
		{
			Put p = new Put(Bytes.toBytes(r));
			// 此处示例添加其他信息
			// p.add(Bytes.toBytes("family"),Bytes.toBytes("column"), 1000,
			// Bytes.toBytes("value"));
			list.add(p);
		}
		table.put(list);// 批量添加
		table.close();// 释放资源
	}

	/**
	 * 创建一个表
	 * 
	 * @param tableName
	 *            表名字
	 * @param columnFamilys
	 *            列簇
	 * 
	 * **/
	public static void createTable(String tableName, String[] columnFamilys) throws Exception
	{
		// admin 对象
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName))
		{
			System.out.println("此表，已存在！");
		}
		else
		{
			// 旧的写法
			// HTableDescriptor tableDesc=new HTableDescriptor(tableName);
			// 新的api
			HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));

			for (String columnFamily : columnFamilys)
			{
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}

			admin.createTable(tableDesc);
			System.out.println("建表成功!");

		}
		admin.close();// 关闭释放资源

	}

	/**
	 * 删除一个表
	 * 
	 * @param tableName
	 *            删除的表名
	 * */
	public static void deleteTable(String tableName) throws Exception
	{
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName))
		{
			admin.disableTable(tableName);// 禁用表
			admin.deleteTable(tableName);// 删除表
			System.out.println("删除表成功!");
		}
		else
		{
			System.out.println("删除的表不存在！");
		}
		admin.close();
	}

	/**
	 * 插入一条数据
	 * 
	 * @param tableName
	 *            表明
	 * @param columnFamily
	 *            列簇
	 * @param column
	 *            列
	 * @param value
	 *            值
	 * 
	 * ***/
	public static void insertOneRow(String tableName, String rowkey, String columnFamily, String column, String value)
			throws Exception
	{

		HTable table = new HTable(conf, tableName);
		Put put = new Put(Bytes.toBytes(rowkey));
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column), Bytes.toBytes(value));
		table.put(put);// 放入表
		table.close();// 释放资源

	}

	/**
	 * 删除一条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param row
	 *            rowkey行键
	 * 
	 * */
	public static void deleteOneRow(String tableName, String row) throws Exception
	{

		HTable table = new HTable(conf, tableName);
		Delete delete = new Delete(Bytes.toBytes(row));
		table.delete(delete);
		table.close();
	}

	/**
	 * 删除多条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rows
	 *            行健集合
	 * 
	 * **/
	public static void deleteList(String tableName, String rows[]) throws Exception
	{
		HTable table = new HTable(conf, tableName);
		List<Delete> list = new ArrayList<Delete>();
		for (String row : rows)
		{
			Delete del = new Delete(Bytes.toBytes(row));
			list.add(del);
		}
		table.delete(list);
		table.close();// 释放资源

	}

	/**
	 * 获取一条数据，根据rowkey
	 * 
	 * @param tableName
	 *            表名
	 * @param row
	 *            行健
	 * 
	 * **/
	public static void getOneRow(String tableName, String row) throws Exception
	{
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		printRecoder(result);// 打印记录
		table.close();// 释放资源
	}

	/**
	 * 查看某个表下的所有数据
	 * 
	 * @param tableName
	 *            表名
	 * */
	public static void showAll(String tableName) throws Exception
	{
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs)
		{
			printRecoder(r);// 打印记录
		}
		table.close();// 释放资源
	}

	/**
	 * 查看某个表下的所有数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            行健
	 * */
	public static void ScanPrefixByRowKey(String tableName, String rowKey) throws Exception
	{
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		scan.setFilter(new PrefixFilter(Bytes.toBytes(rowKey)));
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs)
		{
			printRecoder(r);// 打印记录
		}
		table.close();// 释放资源
	}

	/**
	 * 查看某个表下的所有数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            行健扫描
	 * @param limit
	 *            限制返回数据量
	 * */
	public static void ScanPrefixByRowKeyAndLimit(String tableName, String rowKey, long limit) throws Exception
	{
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		scan.setFilter(new PrefixFilter(Bytes.toBytes(rowKey)));
		scan.setFilter(new PageFilter(limit));
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs)
		{
			printRecoder(r);// 打印记录
		}
		table.close();// 释放资源
	}

	/**
	 * 根据rowkey扫描一段范围
	 * 
	 * @param tableName
	 *            表名
	 * @param startRow
	 *            开始的行健
	 * @param stopRow
	 *            结束的行健
	 * **/
	public void scanByStartAndStopRow(String tableName, String startRow, String stopRow) throws Exception
	{
		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRow));
		scan.setStopRow(Bytes.toBytes(stopRow));
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs)
		{
			printRecoder(r);
		}
		table.close();// 释放资源

	}

	/**
	 * 扫描整个表里面具体的某个字段的值
	 * 
	 * @param tableName
	 *            表名
	 * @param columnFalimy
	 *            列簇
	 * @param column
	 *            列
	 * **/
	public static void getValueDetail(String tableName, String columnFalimy, String column) throws Exception
	{

		HTable table = new HTable(conf, tableName);
		Scan scan = new Scan();
		ResultScanner rs = table.getScanner(scan);
		for (Result r : rs)
		{
			System.out.println("值: " + new String(r.getValue(Bytes.toBytes(columnFalimy), Bytes.toBytes(column))));
		}
		table.close();// 释放资源

	}

	/**
	 * 打印一条记录的详情
	 * 
	 * */
	public static void printRecoder(Result result) throws Exception
	{
		for (Cell cell : result.rawCells())
		{
			System.out.print("行健: " + new String(CellUtil.cloneRow(cell)));
			System.out.print("列簇: " + new String(CellUtil.cloneFamily(cell)));
			System.out.print(" 列: " + new String(CellUtil.cloneQualifier(cell)));
			System.out.print(" 值: " + new String(CellUtil.cloneValue(cell)));
			System.out.println("时间戳: " + cell.getTimestamp());
		}
	}

}