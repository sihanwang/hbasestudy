package com.thomsonreuters.ce.hbasestudy.coprocesor.regionobserver;

import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class LoadObserver {

	public static void main(String[] args)  throws Exception{
		// TODO Auto-generated method stub
	    Configuration conf = HBaseConfiguration.create();
	    
	    Connection connection = ConnectionFactory.createConnection(conf);
	    
		HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("testtable"));
		htd.addFamily(new HColumnDescriptor("colfam1"));
		htd.setValue("COPROCESSOR$1", "hdfs://jing-server-3:8020/hbase/lib/coprocessor.jar" +
		"|" + RegionObserverExample.class.getCanonicalName() +
		"|" + Coprocessor.PRIORITY_USER);
		HBaseAdmin admin = (HBaseAdmin)connection.getAdmin();
		admin.createTable(htd);
		
		System.out.println(admin.getTableDescriptor(Bytes.toBytes("testtable")));		
		
	}

}
