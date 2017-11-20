package com.nyse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.nyse.avgstockvolpermonth.AvgStockVolPerMonthCombiner;
import com.nyse.avgstockvolpermonth.AvgStockVolPerMonthMapper;
import com.nyse.avgstockvolpermonth.AvgStockVolPerMonthReducer;
import com.nyse.keyvalues.LongPair;
import com.nyse.keyvalues.TextPair;

public class AvgStockVolPerMonthTests {
	
	private MapReduceDriver<LongWritable, Text, TextPair, LongPair, TextPair, LongPair> mrDriver;
	
	private MapDriver<LongWritable, Text, Text, LongWritable> mapdriver;
	
	private HashMap<Long, String> records = new HashMap<Long, String>();
	
	@Before
	public void Setup()
	{
		Mapper<LongWritable, Text, TextPair, LongPair> mapper = new AvgStockVolPerMonthMapper();
		Reducer<TextPair, LongPair, TextPair, LongPair> combiner = new AvgStockVolPerMonthCombiner();
		Reducer<TextPair, LongPair, TextPair, LongPair> reducer = new AvgStockVolPerMonthReducer();
		
		mrDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer, combiner);
		
		
		String inputFile = "C:/tmp/samp.csv";
		BufferedReader reader = null;
		Long count = 1L;
		try {
			java.io.File file = new File(inputFile);
			if(file.exists())
			{
			
				reader = new BufferedReader(new FileReader(inputFile));
				
				String line = null;
				while( (line = reader.readLine()) != null)	
				{
					records.put(count, line);
					count++;
				}
			}
			
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}
		finally{
			if(reader != null){
				try {
					reader.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
				reader = null;
			}
		}
		
		
		
	}
	
	/*
	@Test
	public void TestMapReduce()
	{
		mrDriver.withInput(new LongWritable(1), new Text("BPL,02-Jan-2014,70.96,70.99,70.04,70.59,235500"))
		.withInput(new LongWritable(2), new Text("BPL,03-Jan-2014,70.55,70.88,69.77,70.75,192000"))
		.withOutput(new TextPair("2014-01","BPL"), new LongPair(213750L, 2L));
		
		try {
			
			mrDriver.runTest();
		} catch (IOException e) {
			
			e.printStackTrace();
		}

	}*/

	
	@Test
	public void TestMapReduceWithFileInput2() throws Exception
	{
		for(Long key: records.keySet())
		{
			String value = records.get(key);
			
			mrDriver.addInput(new LongWritable(key), new Text(value));
		}
		
		Configuration configuration = new Configuration();
		configuration.set("fs.default.name", "file:///");
		configuration.set("mapreduce.framework.name", "local");
		configuration.set("mapreduce.jobtracker.address", "local");
		
		configuration.set(FileOutputFormat.OUTDIR, "output");
		mrDriver.setConfiguration(configuration);
		
		mrDriver.run();
		
	}
	
	/*
	@Test
	public void TestMapReduceWithFileInput3() throws Exception
	{
		
		try {
			Configuration configuration = new Configuration();
			configuration.set("fs.default.name", "file:///");
			configuration.set("mapreduce.framework.name", "local");
			configuration.set("mapreduce.jobtracker.address","local");
			configuration.set("mapreduce.map.log.level", "INFO");
			configuration.set("mapreduce.reduce.log.level", "INFO");
			configuration.set("fs.file.impl", "com.windows.fs.WindowsLocalFileSystem");
			configuration.set("io.serializations","org.apache.hadoop.io.serializer.JavaSerialization,"
					 + "org.apache.hadoop.io.serializer.WritableSerialization");
			
			
			 File input = new File("input"); 
 		     File output = new File("output");

			
			FileUtils.deleteQuietly(output);
			
			FileSystem fs = FileSystem.getLocal(configuration); 
		    

		    AvgStockVolPerMonthDriver driver = new AvgStockVolPerMonthDriver();
			
			driver.setConf(configuration);
			
			int exitCode = driver.run(new String[]{"input", "output"});
			
			
		} catch (IOException e) {
		
			e.printStackTrace();
		}
	}*/

	/*
	@Test
	public void TestMapReduceWithFileInput()
	{
		Configuration conf = new Configuration(); 
	    conf.set("fs.defaultFS", "file:///"); 
	    conf.set("mapred.framework.name", "local");
	    Path input = new Path("C:\\tmp\\NYSE_2014.csv"); 
	    Path output = new Path("output");
	    
	    try {
	    	
		    FileSystem fs = FileSystem.getLocal(conf); 
		    fs.delete(output, true); // delete old output
		
		    mrDriver.setConfiguration(conf);
			mrDriver.runTest();
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
	*/

	
}
