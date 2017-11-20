package com.xpert.nosql.hbase1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class App2 {

	static Configuration conf = HBaseConfiguration.create();
	
	public static void main(String[] args) throws Exception {
		
		 conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com");
	        conf.set("hbase.zookeeper.property.clientPort", "2181");
	        
	        ScanDemo();
	        
	        InsertDemo();
	        
	        GetDemo();
	}
	
	private static void ScanDemo() throws Exception{
		Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ns1:demo"));
        
        Scan scan1 = new Scan();
        ResultScanner resultScan1 = table.getScanner(scan1);
        
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
	
	
	private static void InsertDemo() throws Exception{
		
		Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ns1:demo"));
        
        // This same mechanism is used for update as well as insert.
        
        Put put = new Put(Bytes.toBytes("3"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("column1"), Bytes.toBytes("value1"));
        put.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("column2"), Bytes.toBytes("value2"));
        
        table.put(put);
        
	}
	
	private static void GetDemo() throws Exception{
		
		Connection conn = ConnectionFactory.createConnection(conf);
        Table table = conn.getTable(TableName.valueOf("ns1:demo"));
        
        Get get = new Get(Bytes.toBytes("3"));
        
        Result rs = table.get(get);
        System.out.println(Bytes.toString(rs.getRow()));
        System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("column1"))));
    	System.out.println(Bytes.toString(rs.getValue(Bytes.toBytes("cf1"), Bytes.toBytes("column2"))));
	}

}
