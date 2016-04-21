package com.thomsonreuters.ce.hbasestudy.putexample.atomiccompare_set;

// cc PutExample Example application inserting data into HBase
// vv PutExample
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.util.Bytes;
// ^^ PutExample
// vv PutExample

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

public class PutExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create(); // co PutExample-1-CreateConf Create the required configuration.

    // ^^ PutExample
    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1");
    // vv PutExample
    Connection connection = ConnectionFactory.createConnection(conf);
    HTable table = (HTable)connection.getTable(TableName.valueOf("testtable")); // co PutExample-2-NewTable Instantiate a new client.

    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
    Bytes.toBytes("val1"));
    
    boolean res1 = table.checkAndPut(Bytes.toBytes("row1"),
    Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), null, put1);
    System.out.println("Put applied: " + res1);
    
    
    boolean res2 = table.checkAndPut(Bytes.toBytes("row1"),
    Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), null, put1);
    System.out.println("Put applied: " + res2);
    
    
    Put put2 = new Put(Bytes.toBytes("row1"));
    put2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
    Bytes.toBytes("val2"));
    
    boolean res3 = table.checkAndPut(Bytes.toBytes("row1"),
    Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
    Bytes.toBytes("val1"), put2);
    System.out.println("Put applied: " + res3);
    
    Put put3 = new Put(Bytes.toBytes("row2"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
    Bytes.toBytes("val3"));
    
    //This will raise exception because put row and check row are different.
    
    boolean res4 = table.checkAndPut(Bytes.toBytes("row1"),
    Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
    Bytes.toBytes("val1"), put3);
    System.out.println("Put applied: " + res4);
  }
}
// ^^ PutExample
