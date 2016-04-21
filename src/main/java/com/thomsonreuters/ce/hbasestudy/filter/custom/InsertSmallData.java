package com.thomsonreuters.ce.hbasestudy.filter.custom;

// cc PutExample Example application inserting data into HBase
// vv PutExample
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
// ^^ PutExample
// vv PutExample



import java.io.IOException;
import java.util.Random;

public class InsertSmallData {
	private final static String DATE_FORMAT_SSS = "yyyyMMddHHmmss";
	private static DateFormat formatter = new SimpleDateFormat(DATE_FORMAT_SSS);
	
	public static void main(String[] args) throws IOException {


		Configuration conf = HBaseConfiguration.create(); // co PutExample-1-CreateConf Create the required configuration.

		// ^^ PutExample
		HBaseHelper helper = HBaseHelper.getHelper(conf);
		helper.dropTable("vessel_location");
		helper.createTable("vessel_location", "details");
		
		helper.dropTable("zone_event");
		helper.createTable("zone_event", "details");
		
		
		// vv PutExample
		Connection connection = ConnectionFactory.createConnection(conf);
		HTable location_table = (HTable)connection.getTable(TableName.valueOf("vessel_location")); // co PutExample-2-NewTable Instantiate a new client.
		location_table.setAutoFlush(false,true);
		
		HTable event_table = (HTable)connection.getTable(TableName.valueOf("zone_event")); // co PutExample-2-NewTable Instantiate a new client.
		event_table.setAutoFlush(false,true);
		
		Random random = new Random();

		int IMO_MAX=9999999;
		int IMO_Min=1000000;

		int COORDINATES_MAX=180;
		int COORDINATES_MIN=-180;

		int SPEED_MIN=0;
		int SPEED_MAX=200;
		
		int EVENT_DURATION_MIN=7200000;
		int EVENT_DURATION_MAX=604800000;
		
		int POLYGON_ID_MAX=99999;
		int POLYGON_ID_MIN=100;
		int POLYGON_NUM_MIN=1;
		int POLYGON_NUM_MAX=4;
		

		for (int i=0 ; i <1;i++)
		{

			int imo=random.nextInt(IMO_MAX)%(IMO_MAX-IMO_Min+1) + IMO_Min;
			
			long Now=new Date().getTime();
			

			for (int j=0; j <10000; j++)
			{
				
				long timestamp=Now - (random.nextInt(Integer.MAX_VALUE)%(Integer.MAX_VALUE +1));

				String Latitude=String.valueOf(random.nextInt(COORDINATES_MAX)%(COORDINATES_MAX-COORDINATES_MIN+1) + COORDINATES_MIN);
				String Longitude=String.valueOf(random.nextInt(COORDINATES_MAX)%(COORDINATES_MAX-COORDINATES_MIN+1) + COORDINATES_MIN);
				String Speed=String.valueOf(random.nextInt(SPEED_MAX)%(SPEED_MAX-SPEED_MIN+1) + SPEED_MIN);
				String Destination="Port_"+String.valueOf(j);

				Put put= new Put(Bytes.toBytes(padNum(imo,7) + padNum(Long.MAX_VALUE-timestamp,19)));
				put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("speed"), Bytes.toBytes(Speed));
				put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("destination"), Bytes.toBytes(Destination));
				put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("timestamp"), Bytes.toBytes(formatter.format(new Date(timestamp))));
				put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("lat"), Bytes.toBytes(Latitude));
				put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("long"), Bytes.toBytes(Longitude));
				
				int polygon_num=random.nextInt(POLYGON_NUM_MAX)%(POLYGON_NUM_MAX-POLYGON_NUM_MIN+1) + POLYGON_NUM_MIN;
				
				StringBuffer polygons=new StringBuffer("");
				
				for (int x=0 ; x<polygon_num ; x++)
				{
					int polygon_id=random.nextInt(POLYGON_ID_MAX)%(POLYGON_ID_MAX-POLYGON_ID_MIN+1) + POLYGON_ID_MIN;
					
					if (x==0)
					{
						polygons=polygons.append(polygon_id);
					}
					else
					{
						polygons=polygons.append("|").append(polygon_id);
					}					
					
					long eventduration=random.nextInt(EVENT_DURATION_MAX)%(EVENT_DURATION_MAX-EVENT_DURATION_MIN+1) + EVENT_DURATION_MIN;;
					long endtimestamp=timestamp+eventduration;

					Put putEvent= new Put(Bytes.toBytes(padNum(polygon_id,5) + padNum(Long.MAX_VALUE-timestamp,19)+padNum(imo,7)));
					
					putEvent.addColumn(Bytes.toBytes("details"), Bytes.toBytes("destination"), Bytes.toBytes(Destination));
					putEvent.addColumn(Bytes.toBytes("details"), Bytes.toBytes("entrytimestamp"), Bytes.toBytes(formatter.format(new Date(timestamp))));
					putEvent.addColumn(Bytes.toBytes("details"), Bytes.toBytes("outtimestamp"), Bytes.toBytes(formatter.format(new Date(endtimestamp))));
					event_table.put(putEvent);;
					
				}
				
				put.addColumn(Bytes.toBytes("details"), Bytes.toBytes("events"), Bytes.toBytes(polygons.toString()));
				location_table.put(put);

				
			}
			
			location_table.flushCommits();
			System.out.println("Done for IMO:"+imo +" locations");
			
			event_table.flushCommits();
			System.out.println("Done for IMO:"+imo +" events");
			
		}
		
		connection.close();
		helper.close();
		

	}


  
  
  public static String padNum(long num, int pad) {
	    String res = Long.toString(num);
	    if (pad > 0) {
	      while (res.length() < pad) {
	        res = "0" + res;
	      }
	    }
	    return res;
	  }

 

}
// ^^ PutExample
