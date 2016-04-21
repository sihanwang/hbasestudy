package com.thomsonreuters.ce.hbasestudy.filter.custom;

// cc CustomFilterExample Example using a custom filter
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class CustomFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("zone_event"));

    
    long now= new Date().getTime();
    
    Filter filter1 = new NewCustomFilter(99996, new Date(now-2592000000L), new Date(now));

    Scan scan = new Scan();
    scan.setFilter(filter1);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ CustomFilterExample
    System.out.println("Results of scan:");
    // vv CustomFilterExample
    for (Result result : scanner) {
      for (KeyValue kv : result.raw()) {
        System.out.println("KV: " + kv + ", Value: " +
          Bytes.toString(kv.getValue()));
      }
    }
    scanner.close();
    // ^^ CustomFilterExample
  }
}
