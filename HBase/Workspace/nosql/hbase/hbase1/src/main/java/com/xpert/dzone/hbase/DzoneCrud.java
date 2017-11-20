package com.xpert.dzone.hbase;


/**
 * To Run this jar first 
 * #>export HADOOP_CLASSPATH=`hbase classpath`
 * #>hadoop jar <jarfile> <mainClass>
 * 
 * 
 */

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

import com.sun.tools.javac.util.List;

public class DzoneCrud {

	public static void main(String[] args) throws Exception, ZooKeeperConnectionException, IOException{
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "sandbox.hortonworks.com" );
	    conf.set("hbase.zookeeper.property.clientPort","2181");
		Connection conn = ConnectionFactory.createConnection(conf);
		// Create the table
		//HBaseAdmin admin = new HBaseAdmin(conf);
		Admin admin =  conn.getAdmin();
		HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf("people"));
		tableDesc.addFamily(new HColumnDescriptor("personal"));
		tableDesc.addFamily(new HColumnDescriptor("contactinfo").setMaxVersions(3));
		tableDesc.addFamily(new HColumnDescriptor("creditcard"));
		
		//admin.createNamespace(NamespaceDescriptor.create("dzone").build());
		admin.createTable(tableDesc);
		
		
		
		
		Table table = conn.getTable(TableName.valueOf("people"));
		
		// Insert values
		
		java.util.List<Put> puts = new ArrayList<Put>();
		
		Put put = new Put(Bytes.toBytes("doe-john-m-54321")); // Row key
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("givenName"), Bytes.toBytes("John"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("mi"), Bytes.toBytes("M"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("surName"), Bytes.toBytes("Doe"));
		put.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes("john.m.doe@gmail.com"));
		
		puts.add(put);
		
		put = new Put(Bytes.toBytes("ashish-rane-g-56789"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("givenName"), Bytes.toBytes("Ashish"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("mi"), Bytes.toBytes("G"));
		put.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("surName"), Bytes.toBytes("Rane"));
		put.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"), Bytes.toBytes("ashishrane@outlook.com"));
		
		puts.add(put);
		
		System.out.println("Inserting rows in batch " + puts.size());
		
		table.put(puts);
		
		//table.close();
		
		// Get Values
		Get get = new Get(Bytes.toBytes("doe-john-m-54321"));
		get.addFamily(Bytes.toBytes("personal"));
		get.setMaxVersions(3);
		
		Result rs = table.get(get);
		System.out.println(rs);
		
		// Scan values where name starts with smith with 25 results
		Scan scan = new Scan(Bytes.toBytes("rane-"));
		scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("givenName"));
		scan.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
		
		scan.setFilter(new PageFilter(25));
		ResultScanner scanner1 = table.getScanner(scan);
		
		for(Result rs1: scanner1){
			// Row key
        	System.out.println(Bytes.toString(rs.getRow()));
        	
        	// Columns
        	System.out.println(Bytes.toString(rs1.getValue(Bytes.toBytes("personal"), Bytes.toBytes("givenName"))));
        	System.out.println(Bytes.toString(rs1.getValue(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"))));
			
		}
		
		scanner1.close();
		
		// Delete rows, column family or columns
		//Delete delete = new Delete(Bytes.toBytes("doe-john-m-12345"));
		//delete.addColumn(Bytes.toBytes("contactinfo"), Bytes.toBytes("email"));
		
		// table.delete(delete);
		
		table.close();
	}
}
