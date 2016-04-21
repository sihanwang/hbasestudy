package com.thomsonreuters.ce.hbasestudy.getexample.v1;

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

import java.util.List;
import java.util.ArrayList;
import java.util.Date;
import java.io.IOException;

import com.thomsonreuters.ce.hbasestudy.putexample.v2.HBaseHelper;

public class ListOfGetExample {

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
		    
		    byte[] cf1 = Bytes.toBytes("colfam1");
		    byte[] qf1 = Bytes.toBytes("qual1");
		    byte[] qf2 = Bytes.toBytes("qual2");
		    byte[] row1 = Bytes.toBytes("row1");
		    byte[] row2 = Bytes.toBytes("row2");
		    List<Get> gets = new ArrayList<Get>();
		    Get get1 = new Get(row1);
		    get1.addColumn(cf1, qf1);
		    gets.add(get1);
		    Get get2 = new Get(row2);
		    get2.addColumn(cf1, qf1);
		    gets.add(get2);
		    Get get3 = new Get(row2);
		    get3.addColumn(cf1, qf2);
		    gets.add(get3);
		    Result[] results = table.get(gets);
		    System.out.println("First iteration...");
		    for (Result result : results) {
		    String row = Bytes.toString(result.getRow());
		    System.out.print("Row: " + row + " ");
		    byte[] val = null;
		    if (result.containsColumn(cf1, qf1)) {
		    val = result.getValue(cf1, qf1);
		    System.out.println("Value: " + Bytes.toString(val));
		    }
		    if (result.containsColumn(cf1, qf2)) {
		    val = result.getValue(cf1, qf2);
		    System.out.println("Value: " + Bytes.toString(val));
		    }
		    }
		    System.out.println("Second iteration...");
		    for (Result result : results) {
		    for (KeyValue kv : result.raw()) {
		    System.out.println("Row: " + Bytes.toString(kv.getRow()) +
		    " Value: " + Bytes.toString(kv.getValue()));
		    }
		    }
		    		    

	}

}
