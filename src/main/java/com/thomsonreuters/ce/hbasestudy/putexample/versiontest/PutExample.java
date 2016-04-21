package com.thomsonreuters.ce.hbasestudy.putexample.versiontest;

//cc PutExample Example application inserting data into HBase
//vv PutExample
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Date;
import java.io.IOException;
import java.io.FileInputStream;

public class PutExample {

public static void main(String[] args) throws IOException {
	String tableName = "hbasetest";
	String familyName = "info";
	Configuration conf = HBaseHelper.getHBaseConfig(1);
	
    Connection connection = ConnectionFactory.createConnection(conf);
	//创建表		
	Admin admin = connection.getAdmin();
	HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
	tableDesc.addFamily(new HColumnDescriptor(familyName));
	
	if (admin.tableExists(TableName.valueOf(tableName)))
	{
		admin.disableTable(TableName.valueOf(tableName));
		admin.deleteTable(TableName.valueOf(tableName));
	}
	
	admin.createTable(tableDesc);
	
	//插入表数据

    Table table = connection.getTable(TableName.valueOf(tableName));
	
	//多条插入
	List<Put> list = new ArrayList<Put>();
	Put p = null;
	p = new Put("rowkey1".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), "wangwu".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), "male".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), "beijing".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), "25".getBytes());
	list.add(p);
	
	table.put(list); 
	p = new Put("rowkey2".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), new Date().getTime(),   "wangwu".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), new Date().getTime(), "male".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), new Date().getTime(), "beijing".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), new Date().getTime(),  "25".getBytes());
	list.add(p);
	
	table.put(list); 
	
	
	
	table.close(); // co PutExample-6-DoPut Close table and connection instances to free resources.
    connection.close();
}
}
//^^ PutExample
