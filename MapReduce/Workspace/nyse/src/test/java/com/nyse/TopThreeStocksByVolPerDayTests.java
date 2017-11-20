package com.nyse;

import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.nyse.keyvalues.LongPair;
import com.nyse.keyvalues.TextPair;
import com.nyse.topthreestocksbyvol.TopThreeStocksByVolPerDayMapper;
import com.nyse.topthreestocksbyvol.TopThreeStocksByVolPerDayReducer;

public class TopThreeStocksByVolPerDayTests {

	private MapReduceDriver<LongWritable, Text, LongPair, Text, LongPair, Text> mrDriver;
	
	private HashMap<Long, String> records = new HashMap<Long, String>();
	
	
	@Before
	public void Setup(){
	
		Mapper<LongWritable, Text, LongPair, Text> mapper = new TopThreeStocksByVolPerDayMapper();
		Reducer<LongPair, Text, LongPair, Text> reducer = new TopThreeStocksByVolPerDayReducer();
		
		mrDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
		
		records.put(1L, "BPL,02-Jan-2014,70.96,70.99,70.04,70.59,235500");
		
	}
	
	@Test
	public void Test1() throws IOException{
		
		for(Long key: records.keySet())
		{
			String value = records.get(key);
			
			mrDriver.addInput(new LongWritable(key), new Text(value));
		}
		
		
		mrDriver.run();
		
	}
}
