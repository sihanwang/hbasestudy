package com.thomsonreuters.ce.hbasestudy.filter;

// cc PrefixFilterExample Example using the prefix based filter
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;



import java.io.IOException;

public class PrefixFilterExample {

  public static void main(String[] args) throws IOException {
    Configuration conf = HBaseConfiguration.create();

    HBaseHelper helper = HBaseHelper.getHelper(conf);
    helper.dropTable("testtable");
    helper.createTable("testtable", "colfam1", "colfam2");
    System.out.println("Adding rows to table...");
    helper.fillTable("testtable", 1, 10, 10, "colfam1", "colfam2");

    Connection connection = ConnectionFactory.createConnection(conf);
    Table table = connection.getTable(TableName.valueOf("testtable"));

    // vv PrefixFilterExample
    Filter filter = new PrefixFilter(Bytes.toBytes("row-1"));

    Scan scan = new Scan();
    scan.setFilter(filter);
    ResultScanner scanner = table.getScanner(scan);
    // ^^ PrefixFilterExample
    System.out.println("Results of scan:");
    // vv PrefixFilterExample
    for (Result result : scanner) {
      for (KeyValue kv : result.raw()) {
        System.out.println("KV: " + kv + ", Value: " +
          Bytes.toString(kv.getValue()));
      }
    }
    scanner.close();

    Get get = new Get(Bytes.toBytes("row-5"));
    get.setFilter(filter);
    Result result = table.get(get);
    // ^^ PrefixFilterExample
    System.out.println("Result of get: ");
    // vv PrefixFilterExample
    for (KeyValue kv : result.raw()) {
      System.out.println("KV: " + kv + ", Value: " +
        Bytes.toString(kv.getValue()));
    }
    // ^^ PrefixFilterExample
  }
}
