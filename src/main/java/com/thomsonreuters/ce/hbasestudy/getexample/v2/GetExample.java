package com.thomsonreuters.ce.hbasestudy.getexample.v2;

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

import java.util.Date;
import java.io.IOException;

import com.thomsonreuters.ce.hbasestudy.putexample.v2.HBaseHelper;

public class GetExample {

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
		    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), new Date().getTime()-10, Bytes.toBytes("val1"));
		    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), new Date().getTime()+10, Bytes.toBytes("val2"));
		    table.put(put1);

		    table.flushCommits();
		    
		    Get get = new Get(Bytes.toBytes("row1"));
		    get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));

		    get.setMaxVersions();
		    
		    /*
		    Result result = table.get(get);
		    byte[] val = result.getValue(Bytes.toBytes("colfam1"),
		    Bytes.toBytes("qual1"));
		    */
		    Result result = table.get(get);
		    KeyValue[] val = result.raw();
		    
		    for (KeyValue kv : val)
		    {
		    	System.out.println(kv.toString()); //multi version will be printed out.
		    }
		    		    

	}

}
