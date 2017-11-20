package com.xpert.nosql.hbase1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 *
 */
public class App 
{
	static Configuration conf = HBaseConfiguration.create();
	
    public static void main( String[] args ) throws Exception
    {
    	
        conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        
        Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ns1:demo"));
        
        Scan scan1 = new Scan();
        ResultScanner resultScan1 = table.getScanner(scan1);
        try{
	        for (Result rs: resultScan1){
	        	
	        	// System.out.println(rs);
	 
	        	// Row key
	        	System.out.println(Bytes.toString(rs.getRow()));
	        	
	        	// Columns
	        	System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("column1"))));
	        	System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("column2"))));
	        	System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("column3"))));
	        	System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("cf2"), Bytes.toBytes("column4"))));
	        }
        }
        finally{
        	resultScan1.close();
        }
        
    }
}
