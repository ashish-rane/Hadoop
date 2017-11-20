package com.xpert.nosql.hbase.misc;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.*;
/**
 *  A HBase Java Client filter Example class written for COMP5338@S4.2014 Tutorial 10
 *  
 *  http://web.it.usyd.edu.au/~comp5338/code/HBaseFilterExample.java
 *  
 * @author Ying Zhou
 *
 */
public class HBaseFilterExample {

	public static final String ZOOKEEPER  = "ec2-54-89-120-93.compute-1.amazonaws.com";
	public static final String WIKITABLE= "share:wikipagecount";
	public static final String PHOTOTABLE ="share:photos";
	
	/**
	 * Use the  wiki page count table to test value filter
	 * we want to find all record start with amazon with a hit count greater than 1
	 * 
	 */
	public void testValueFilter(){
		
		Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZOOKEEPER );
        config.set("hbase.zookeeper.property.clientPort","2181");
		

        try{
        	HTable wikiTable = new HTable(config, WIKITABLE);
		
        	Filter vFilter = new ValueFilter(CompareFilter.CompareOp.GREATER,
			new BinaryComparator(Bytes.toBytes("1")));
		
        	Scan scan = new Scan();
        	scan.setFilter(vFilter);
        	scan.setStartRow(Bytes.toBytes("en/amazon"));
        	scan.setStopRow(Bytes.toBytes("en/amazoo"));
        	ResultScanner scanner = wikiTable.getScanner(scan);
        	for (Result result: scanner){
        		for (KeyValue kv: result.raw()){
        			System.out.println("kv: " + kv + ", Value: " + Bytes.toString(kv.getValue()));
        		}
        			
    		}
        	scanner.close();
        	wikiTable.close();
        }catch (IOException e){
        	e.printStackTrace();
        }
	}
	

	
	/**
	 * Use the wikipagecount table to find out page titles start with amazon and 
	 * has been visited during 20120910-140000
	 * 
	 *
	 */
	
	public void testSingleColumnValueFilterWiki(){
		byte[] groups = Bytes.toBytes("pagecount");
		byte[] group = Bytes.toBytes("20140814-040000"); 
		
		
		Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZOOKEEPER );
        config.set("hbase.zookeeper.property.clientPort","2181");
		
    	
    	try{
        	HTable wikiTable = new HTable(config, WIKITABLE);
		
        	SingleColumnValueFilter vFilter = new SingleColumnValueFilter(groups, group,
        			
    				CompareFilter.CompareOp.GREATER_OR_EQUAL,
    				new BinaryComparator(Bytes.toBytes("1")));
        	//Drop rows that do not have the column at all from the result set
        	vFilter.setFilterIfMissing(true);
        	Scan scan = new Scan();
        	scan.setFilter(vFilter);
        	scan.setStartRow(Bytes.toBytes("en/amazon"));
        	scan.setStopRow(Bytes.toBytes("en/amazoo"));
        	ResultScanner scanner = wikiTable.getScanner(scan);
        	for (Result result: scanner){
        		for (KeyValue kv: result.raw()){
        			System.out.println("kv: " + kv + ", Value: " + Bytes.toString(kv.getValue()));
        		}
        			
    		}
        	scanner.close();
        	wikiTable.close();
        }catch (IOException e){
        	e.printStackTrace();
        }
		
	}
	
	/**
	 * Use the photoComKeys table to find out all photos belonging to user 42307458@N04 
	 * that contains word 'canon' in the tag list
	 * 
	 *
	 */
	
	public void testSingleColumnValueFilterPhoto(){
		byte[] content = Bytes.toBytes("c");
		byte[] tags = Bytes.toBytes("t");
		byte[] startRow = 	Bytes.toBytes("42307458@N04");
		byte[] stopRow = 	Bytes.toBytes("42307458@N05");
		Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZOOKEEPER );
        config.set("hbase.zookeeper.property.clientPort","2181");
        
        try{
        	HTable photoTable = new HTable(config, PHOTOTABLE);
		
        	SingleColumnValueFilter vFilter = new SingleColumnValueFilter(content,tags,
        			
    				CompareFilter.CompareOp.EQUAL,
    				new SubstringComparator("canon"));
        	
        	//Drop rows that do not have the column at all from the result set
        	vFilter.setFilterIfMissing(true);
        	Scan scan = new Scan();
        	scan.setFilter(vFilter);
        	scan.setStartRow(startRow);
        	scan.setStopRow(stopRow);
        	scan.addColumn(content, tags);// only return the tags to verify the results
        	ResultScanner scanner = photoTable.getScanner(scan);
        	for (Result result: scanner){
        		for (KeyValue kv: result.raw()){
        			System.out.println("kv: " + kv + ", Value: " + Bytes.toString(kv.getValue()));
        		}
        			
    		}
        	scanner.close();
        	photoTable.close();
        }catch (IOException e){
        	e.printStackTrace();
        }
	}
	
	/**
	 * Test qualifier filter using photoComKeys table to find all photos belonging 
	 * to user "42307458@N04" that are 
	 * included in group '35034359018@N01'
	 * 
	 */
	public void testQualifierFilterPhoto(){
		byte[] groupId = Bytes.toBytes("35034359018@N01");
		byte[] startRow = 	Bytes.toBytes("42307458@N04");
		byte[] stopRow = 	Bytes.toBytes("42307458@N05");
		
		Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZOOKEEPER );
        config.set("hbase.zookeeper.property.clientPort","2181");
        
        try{
        	HTable photoTable = new HTable(config, PHOTOTABLE);
		
        	Filter qFilter = new QualifierFilter(
    				CompareFilter.CompareOp.EQUAL,
    				new BinaryComparator(groupId));

        	Scan scan = new Scan();
        	scan.setFilter(qFilter);
        	scan.setStartRow(startRow);
        	scan.setStopRow(stopRow);
        	ResultScanner scanner = photoTable.getScanner(scan);
        	for (Result result: scanner){
        	   	System.out.println("Row Key: " + Bytes.toString(result.getRow()));
			System.out.println("The group is added at timestamp " +
				Bytes.toInt(result.getValue(Bytes.toBytes("g"),groupId)));
    		}	
        	scanner.close();
        	photoTable.close();
        }catch (IOException e){
        	e.printStackTrace();
        }
	}
	
	public static void main(String argv[]){

		Logger.getRootLogger().setLevel(Level.OFF);
		HBaseFilterExample hbfe = new HBaseFilterExample();
//		System.setProperty("http.proxyHost", "web-cache.usyd.edu.au");
//		System.setProperty("http.proxyPort","8080");
		System.out.println("Find all record start with amazon with a hit count greater than 1 from wikipagecounttable");
		System.out.println("The results are: " );
		
		hbfe.testValueFilter();
		
		System.out.println("--------------------------");
		
		System.out.println("find out page titles start with amazon and\n" +
				"has been visited during 20140814-140000 from wikipagecount: ");
		System.out.println("The results are: " );
		
		
		hbfe.testSingleColumnValueFilterWiki();
		
		System.out.println("--------------------------");
		
		System.out.println("find all photos belonging to user 42307458@N04 that contain \n" +
		"word \"cannon\" in the tag list from photoComKeys table" );
	System.out.println("The results are: " );
		hbfe.testSingleColumnValueFilterPhoto();
		
		System.out.println("--------------------------");
		
		System.out.println("find all photos belonging to user 42307458@N04 that are \n" +
			"included in group 35034359018@N01 from photoComKeys table" );
		System.out.println("The results are: " );
		
		hbfe.testQualifierFilterPhoto();
		
		
	}
}
