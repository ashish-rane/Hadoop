package com.xpert.nosql.hbase.bulkload;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to read csv and create Key Value pairs where Key is Hbase table name 
 * and value is a put object which is one row.
 * @author Ashish.Rane
 *
 */
public class HBaseBulkLoadMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{

	private String hbaseTable;
	private String dataSeperator;
	private String columnFamily1;
	private String columnFamily2;
	
	private ImmutableBytesWritable hbaseTableName;

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		
		Configuration conf = context.getConfiguration();
		hbaseTable =conf.get("hbase.table.name");
		dataSeperator = conf.get("data.seperator");
		columnFamily1 = conf.get("COLUMN_FAMILY_1");
		columnFamily2 = conf.get("COLUMN_FAMILY_2");
		
		hbaseTableName = new ImmutableBytesWritable(Bytes.toBytes(hbaseTable));
		
	}

	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		
		try{
			String[] values = value.toString().split(dataSeperator);
			String rowKey = values[0];
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("first_name"), Bytes.toBytes(values[1]));
			put.addColumn(Bytes.toBytes(columnFamily1), Bytes.toBytes("last_name"), Bytes.toBytes(values[2]));
			put.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("email"), Bytes.toBytes(values[3]));
			put.addColumn(Bytes.toBytes(columnFamily2), Bytes.toBytes("city"), Bytes.toBytes(values[4]));
			
			context.write(hbaseTableName, put);
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	
	

	
	
	
}
