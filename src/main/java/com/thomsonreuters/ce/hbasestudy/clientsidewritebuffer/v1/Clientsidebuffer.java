package com.thomsonreuters.ce.hbasestudy.clientsidewritebuffer.v1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.thomsonreuters.ce.hbasestudy.putexample.v2.HBaseHelper;

public class Clientsidebuffer {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
	    Configuration conf = HBaseConfiguration.create(); // co PutExample-1-CreateConf Create the required configuration.

	    // ^^ PutExample
	    HBaseHelper helper = HBaseHelper.getHelper(conf);
	    
	    helper.dropTable("testtable");
	    helper.createTable("testtable", "colfam1");
	    
	    Connection connection = ConnectionFactory.createConnection(conf);
	    HTable table = (HTable)connection.getTable(TableName.valueOf("testtable")); // co PutExample-2-NewTable Instantiate a new client.
    
	    System.out.println("Auto flush: " + table.isAutoFlush());
	    table.setAutoFlush(false,true);
	    
	    Put put1 = new Put(Bytes.toBytes("row1"));
	    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
	    table.put(put1);
	    Put put2 = new Put(Bytes.toBytes("row2"));
	    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val2"));
	    table.put(put2);
	    Put put3 = new Put(Bytes.toBytes("row3"));
	    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), Bytes.toBytes("val3"));
	    table.put(put3);
	    Get get = new Get(Bytes.toBytes("row1"));
	    Result res1 = table.get(get);
	    System.out.println("Result: " + res1);
	    table.flushCommits();
	    Result res2 = table.get(get);
	    System.out.println("Result: " + res2);
	    
	    
	    
	    
	}

}
