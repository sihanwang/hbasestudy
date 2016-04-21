package com.thomsonreuters.ce.hbasestudy.putexample.v1;

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

import java.io.IOException;
import java.io.FileInputStream;

public class PutListExample {

public static void main(String[] args) throws IOException {
	String tableName = "hbasetest";
	String familyName = "info";
	Configuration conf = HBaseHelper.getHBaseConfig(1);
	
    Connection connection = ConnectionFactory.createConnection(conf);
	//创建表		
	Admin admin = connection.getAdmin();
	HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
	tableDesc.addFamily(new HColumnDescriptor(familyName));
	admin.createTable(tableDesc);
	
	//插入表数据

    Table table = connection.getTable(TableName.valueOf(tableName));
	//单条插入
	//row1为rowkey
	Put putRow1 = new Put("row1".getBytes());
	putRow1.addColumn(familyName.getBytes(), "name".getBytes(), "zhangsan".getBytes());
	putRow1.addColumn(familyName.getBytes(), "age".getBytes(), "24".getBytes());
	putRow1.addColumn(familyName.getBytes(), "city".getBytes(), "chengde".getBytes());
	putRow1.addColumn(familyName.getBytes(), "sex".getBytes(), "male".getBytes());
	table.put(putRow1);
	
	//多条插入
	List<Put> list = new ArrayList<Put>();
	Put p = null;
	p = new Put("rowkey1".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), "wangwu".getBytes());
	p.addColumn(familyName.getBytes(), "sex".getBytes(), "male".getBytes());
	p.addColumn(familyName.getBytes(), "city".getBytes(), "beijing".getBytes());
	p.addColumn(familyName.getBytes(), "age".getBytes(), "25".getBytes());
	list.add(p);
	p = new Put("rowkey2".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), "zhangliu".getBytes());
	p.addColumn(familyName.getBytes(), "sex".getBytes(), "male".getBytes());
	p.addColumn(familyName.getBytes(), "city".getBytes(), "handan".getBytes());
	p.addColumn(familyName.getBytes(), "age".getBytes(), "28".getBytes());
	list.add(p);
	p = new Put("rowkey3".getBytes());
	p.addColumn(familyName.getBytes(), "name".getBytes(), "liqing".getBytes());
	p.addColumn(familyName.getBytes(), "sex".getBytes(), "female".getBytes());
	p.addColumn(familyName.getBytes(), "city".getBytes(), "guangzhou".getBytes());
	p.addColumn(familyName.getBytes(), "age".getBytes(), "18".getBytes());
	list.add(p);
	table.put(list); 
}
}
//^^ PutExample
