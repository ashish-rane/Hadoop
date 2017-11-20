package com.nyse.topthreestocksbyvol;

import java.io.IOException;
import java.text.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import com.nyse.keyvalues.LongPair;
import com.nyse.parsers.NYSEParser;

public class TopThreeStocksByVolPerDayMapper extends Mapper<LongWritable, Text, LongPair, Text>{

	private Logger log = Logger.getLogger(getClass());
	
	private int startyear = Integer.MAX_VALUE;
	private int endyear =  Integer.MIN_VALUE;

	
	@Override
	protected void setup(
			Mapper<LongWritable, Text, LongPair, Text>.Context context)
			throws IOException, InterruptedException {
		
		log.info("Map Started");
	}



	@Override
	protected void map(LongWritable key, Text record,
			Mapper<LongWritable, Text, LongPair, Text>.Context context)
			throws IOException, InterruptedException {
		
		NYSEParser parser = new NYSEParser(record.toString());
		
		/*try {
			int year = parser.getTradeYear();
			
			if(year < startyear){
				startyear = year;
			}
			
			if(year > endyear){
				endyear = year;
			}

		} catch (ParseException e) {
			
			e.printStackTrace();
		}*/
		
		LongPair outputKey = new LongPair(parser.getTradeDateNumeric(), parser.getVolume());
		
		//log.info("Output Key: " + outputKey);
		
		context.write(outputKey, record);
	}
	
	@Override
	protected void cleanup(
			Mapper<LongWritable, Text, LongPair, Text>.Context context)
			throws IOException, InterruptedException {
		
		/*Configuration conf = context.getConfiguration();
		conf.set("start_year", Integer.toString(startyear));
		conf.set("end_year", Integer.toString(endyear));*/
		
		
		log.info("Map Finished");
	}


}
