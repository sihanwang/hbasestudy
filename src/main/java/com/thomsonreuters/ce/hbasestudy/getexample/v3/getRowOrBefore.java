package com.thomsonreuters.ce.hbasestudy.getexample.v3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.io.IOException;

import com.thomsonreuters.ce.hbasestudy.putexample.v2.HBaseHelper;

public class getRowOrBefore {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		  Configuration conf = HBaseConfiguration.create(); // co PutExample-1-CreateConf Create the required configuration.

		    // ^^ PutExample
		    HBaseHelper helper = HBaseHelper.getHelper(conf);
		    
		    helper.dropTable("testtable");
		    helper.createTable("testtable", "colfam1");
		    
		    //alter 'testtable' , { NAME=>'colfam1','VERSIONS'=>5}
		    
		    Connection connection = ConnectionFactory.createConnection(conf);
		    HTable table = (HTable)connection.getTable(TableName.valueOf("testtable")); // co PutExample-2-NewTable Instantiate a new client.
	    		    
		    System.out.println("Auto flush: " + table.isAutoFlush());
		    table.setAutoFlush(false,true);
		    
		    Put put1 = new Put(Bytes.toBytes("row1"));
		    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), new Date().getTime()-10, Bytes.toBytes("val1_version1"));
		    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), new Date().getTime()+10, Bytes.toBytes("val1_version2"));
		    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), new Date().getTime()+10, Bytes.toBytes("val1_version1"));
		    table.put(put1);
		    
		    Put put2 = new Put(Bytes.toBytes("row2"));
		    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), new Date().getTime()-10, Bytes.toBytes("val1_version1"));
		    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), new Date().getTime()+10, Bytes.toBytes("val1_version2"));
		    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"), new Date().getTime()+10, Bytes.toBytes("val1_version1"));
		    table.put(put2);		    

		    table.flushCommits();
		    
		Result result1 = table.getRowOrBefore(Bytes.toBytes("row1"),
				Bytes.toBytes("colfam1"));
		System.out.println("Found: " + Bytes.toString(result1.getRow()));
		Result result2 = table.getRowOrBefore(Bytes.toBytes("row99"),
				Bytes.toBytes("colfam1"));
		System.out.println("Found: " + Bytes.toString(result2.getRow()));
		for (KeyValue kv : result2.raw()) {
			System.out.println(" Col: " + Bytes.toString(kv.getFamily()) + "/"
					+ Bytes.toString(kv.getQualifier()) + ", Value: "
					+ Bytes.toString(kv.getValue()));
		}
		Result result3 = table.getRowOrBefore(Bytes.toBytes("abc"),
				Bytes.toBytes("colfam1"));
		System.out.println("Found: " + result3);
		    		    

	}

}
