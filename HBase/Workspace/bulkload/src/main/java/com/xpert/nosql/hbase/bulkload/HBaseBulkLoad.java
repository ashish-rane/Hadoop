package com.xpert.nosql.hbase.bulkload;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;

/**
 * This 
 * @author Ashish.Rane
 *
 */
public class HBaseBulkLoad {

	/**
	 * 
	 * @param filepath - Output from mapper
	 * @param tableName - target hbase table.
	 */
	public static void doBulkLoad(String filepath, String tableName){
		
		try{
			Configuration conf = new Configuration();
			conf.set("mapreduce.child.java.opts", "-Xmx1g");
			
			HBaseConfiguration.addHbaseResources(conf);
			LoadIncrementalHFiles loadFiles = new LoadIncrementalHFiles(conf);
			
			Connection connection = ConnectionFactory.createConnection(conf);
	
			Table table = connection.getTable(TableName.valueOf(tableName));
			
			loadFiles.doBulkLoad(filepath, table);
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
}
