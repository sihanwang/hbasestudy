package com.thomsonreuters.ce.hbasestudy.clientsidewritebuffer.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Used by the book examples to generate tables and fill them with test data.
 */
public class HBaseHelper {
	/**
	 * 获取HBaseConfiguration
	 * @param flag	
	 * 			集群标识：0，单机；1集群
	 * @return
	 */
	public static Configuration getHBaseConfig(int flag){
		Configuration conf = new Configuration();
		if(flag > 0){
			//集群
			conf.set("fs.defaultFS", "hdfs://10.35.62.195:9000/");
			//conf.set("mapreduce.framework.name", "local");
			//conf.set("mapred.job.tracker", "master129:9001");
			conf.set("hbase.zookeeper.quorum", "10.35.62.195");
		}else{
			//单机
			conf.set("fs.defaultFS", "hdfs://ubuntu:9000/");
			conf.set("mapreduce.framework.name", "local");
			conf.set("mapred.job.tracker", "ubuntu:9001");
			conf.set("hbase.zookeeper.quorum", "ubuntu");
		}
		
		return conf;
	}
	

}
