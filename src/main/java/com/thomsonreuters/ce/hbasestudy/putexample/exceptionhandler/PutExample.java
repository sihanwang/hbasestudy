package com.thomsonreuters.ce.hbasestudy.putexample.exceptionhandler;

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

    List<Put> puts = new ArrayList<Put>();
    
    Put put1 = new Put(Bytes.toBytes("row1"));
    put1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
    Bytes.toBytes("val1"));
    puts.add(put1);
    Put put2 = new Put(Bytes.toBytes("row2"));
    put2.addColumn(Bytes.toBytes("BOGUS"), Bytes.toBytes("qual1"),
    Bytes.toBytes("val2"));
    puts.add(put2);
    Put put3 = new Put(Bytes.toBytes("row2"));
    put3.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
    Bytes.toBytes("val3"));
    puts.add(put3);
    Put put4 = new Put(Bytes.toBytes("row2"));
    puts.add(put4);
    
    try {
    table.put(puts); //put的时候做客户端校验,抛出异常，并排除错误记录
    } catch (Exception e) {
    System.err.println("Error: " + e);
    table.flushCommits();//把正确的put数据插入到数据库，对错误的记录报错并返回exception
    }
  }
}
// ^^ PutExample
