package com.nyse.stockcompanyjoin.distcache;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;

import com.nyse.keyvalues.TextPair;
import com.nyse.parsers.CompanyParser;
import com.nyse.parsers.NYSEParser;
import com.sun.tools.doclets.internal.toolkit.Configuration;

public class StockCompanyJoinDistCacheMapper  extends Mapper<LongWritable, Text, TextPair,Text >{

	private Map<String, CompanyParser> companyList = new HashMap<String, CompanyParser>();
	
	private void initialize(Mapper<LongWritable, Text, TextPair, Text>.Context context) throws IOException{
		
		BufferedReader reader = null;
		
		URI[] paths = context.getCacheFiles();
		
		for(URI p : paths){
			
			
			if(p.getPath().endsWith("companylist_noheader.csv")){
				
				try
				{
					
					// No need to give path of the file because we will be supplying this as part of distributed
					// cache using -files command line option
					File file = new File(p.getPath());
					reader = new BufferedReader(new FileReader(file.getName()));
					String line = null;
					while((line = reader.readLine()) != null )
					{
						CompanyParser parser = new CompanyParser(line);
						companyList.put(parser.getStockTicker(), parser);
					}
				}
				finally{
					if(reader != null){
						reader.close();
					}
				}
			}
		}
		
		// OR
		/*
		try
		{
			// No need to give path of the file because we will be supplying this as part of distributed
			// cache using -files command line option
			reader = new BufferedReader(new FileReader("companylist_noheader.csv"));
			String line = null;
			while((line = reader.readLine()) != null )
			{
				CompanyParser parser = new CompanyParser(line);
				companyList.put(parser.getStockTicker(), parser);
			}
		}
		finally{
			if(reader != null){
				reader.close();
			}
		}
		*/
	}

	@Override
	protected void setup(
			Mapper<LongWritable, Text, TextPair, Text>.Context context)
			throws IOException, InterruptedException {
		
		initialize(context);
	}

	@Override
	protected void map(LongWritable key, Text record,
			Mapper<LongWritable, Text, TextPair, Text>.Context context)
			throws IOException, InterruptedException {

			NYSEParser parser = new NYSEParser(record.toString());
			
			CompanyParser cp =  companyList.get(parser.getStockTicker());
			
			if(cp != null){
				TextPair outKey = new TextPair(parser.getStockTicker(), parser.getTradeDate());
				
				
				String companyName = cp.getName().replace(",", "");
				String sector = cp.getSector();
				String industry = cp.getIndustry();
				
				Text outValue = new Text(record.toString() + "," + companyName + "," + sector + "," + industry);
				
				context.write(outKey, outValue);
			}
		
	}
}
