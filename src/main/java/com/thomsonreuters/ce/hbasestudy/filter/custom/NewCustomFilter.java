package com.thomsonreuters.ce.hbasestudy.filter.custom;

import java.io.IOException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;


import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;

public class NewCustomFilter extends FilterBase {

	@Override
	public boolean filterAllRemaining() throws IOException {
		// TODO Auto-generated method stub
		return filterremaining;
	}


	@Override
	public void reset() throws IOException {
		// TODO Auto-generated method stub
		filterrow=true;
	}


	@Override
	public boolean filterRow() throws IOException {
		// TODO Auto-generated method stub
		return filterrow;
	}


	private int Zone_ID;
	private Date Start_Time;
	private Date End_Time;
	private boolean filterrow=true;
	private boolean filterremaining=false;
	
	@Override
	public boolean filterRowKey(byte[] buffer, int offset, int length)
			throws IOException {
		// TODO Auto-generated method stub
		String row_key=Bytes.toString(buffer, offset, length);
		
		//0010092233706038214266901097070
		String zone_id=row_key.substring(0, 5);
		
		if (zone_id.compareTo(padNum(Zone_ID,5))==0)
		{
			Date strart_time= new Date(Long.MAX_VALUE-Long.parseLong(row_key.substring(5, 24)));
			
			if (!strart_time.after(Start_Time))
			{
				filterremaining=true;
				return filterrow;
			}
			else
			{
				filterrow=false;
				return filterrow;
			}
		}
		else if (zone_id.compareTo(padNum(Zone_ID,5))>0)
		{
			filterremaining=true;
			return filterrow;
		}
			
		return filterrow;
	}

	
	public NewCustomFilter() {
		super();
	}

	
	public NewCustomFilter(int zone_id, Date starttime, Date endtime) {
		this.Zone_ID=zone_id;
		this.Start_Time=starttime;
		this.End_Time=endtime;
		
	}
	

	@Override
	public ReturnCode filterKeyValue(Cell cell) throws IOException {

		if (filterrow)
		{
			return ReturnCode.NEXT_ROW;
		}
		else
		{

			byte[] byte_family=CellUtil.cloneFamily(cell);	
		

			if (Bytes.compareTo(Bytes.toBytes("details"), byte_family) !=0)
			{
				return ReturnCode.INCLUDE_AND_NEXT_COL;
			}		

		
			byte[] byte_qualifier=CellUtil.cloneQualifier(cell);
		

			if (Bytes.compareTo(Bytes.toBytes("outtimestamp"), byte_qualifier) !=0)
			{
				return ReturnCode.INCLUDE_AND_NEXT_COL;
			}
			else
			{
		
				byte[] byte_value=CellUtil.cloneValue(cell);
		
				SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMddHHmmss");
				ParsePosition pos = new ParsePosition(0);

				Date end_time=formatter.parse(Bytes.toString(byte_value), pos);
		
				if (end_time.before(End_Time))
				{
					filterrow=false;		
					return ReturnCode.INCLUDE_AND_NEXT_COL;
				}
				else
				{
					filterrow=true;
					return ReturnCode.NEXT_ROW;
				}
			}
		}

	}

	public byte[] toByteArray() throws IOException
	{
		
		byte[] total=new byte[20];
		byte[] zoneid= Bytes.toBytes(this.Zone_ID);		
		byte[] starttime= Bytes.toBytes(this.Start_Time.getTime());
		byte[] endtime= Bytes.toBytes(this.End_Time.getTime());
		
		System.arraycopy(zoneid, 0, total, 0, 4);
		System.arraycopy(starttime, 0, total, 4, 8);
		System.arraycopy(endtime, 0, total, 12, 8);
		
		return total;
	}
	
	
	public static NewCustomFilter parseFrom(final byte [] pbBytes) throws DeserializationException 
	{
		    
		////////////////////////////////////
		int zoneid= Bytes.toInt(pbBytes, 0, 4);
		long starttime= Bytes.toLong(pbBytes, 4, 8);
		long endtime= Bytes.toLong(pbBytes, 12, 8);
		
		return new NewCustomFilter(zoneid, new Date(starttime), new Date(endtime));
	}
	

	public static void main(String[] args)
	{
		byte[] total=new byte[20];
		byte[] zoneid= Bytes.toBytes(4);		
		byte[] starttime= Bytes.toBytes(999999L);
		byte[] endtime= Bytes.toBytes(99998L);
		
		System.arraycopy(zoneid, 0, total, 0, 4);
		System.arraycopy(starttime, 0, total, 4, 8);
		System.arraycopy(endtime, 0, total, 12, 8);
		
		////////////////////////////////////
		int newzoneid= Bytes.toInt(total, 0, 4);
		long newstarttime= Bytes.toLong(total, 4, 8);
		long newendtime= Bytes.toLong(total, 12, 8);
		
		System.out.println();
		
		
		
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
