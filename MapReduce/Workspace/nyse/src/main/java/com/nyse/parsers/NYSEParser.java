package com.nyse.parsers;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class NYSEParser {

	private String stockTicker;
	private String tradeDate;
	private Float openPrice;
	private Float highPrice;
	private Float lowPrice;
	private Float closePrice;
	private Long volume;
	
	public String getStockTicker() {
		return stockTicker;
	}
	public void setStockTicker(String stockTicker) {
		this.stockTicker = stockTicker;
	}
	public String getTradeDate() {
		return tradeDate;
	}
	public void setTradeDate(String tradeDate) {
		this.tradeDate = tradeDate;
	}
	public Float getOpenPrice() {
		return openPrice;
	}
	public void setOpenPrice(Float openPrice) {
		this.openPrice = openPrice;
	}
	public Float getHighPrice() {
		return highPrice;
	}
	public void setHighPrice(Float highPrice) {
		this.highPrice = highPrice;
	}
	public Float getLowPrice() {
		return lowPrice;
	}
	public void setLowPrice(Float lowPrice) {
		this.lowPrice = lowPrice;
	}
	public Float getClosePrice() {
		return closePrice;
	}
	public void setClosePrice(Float closePrice) {
		this.closePrice = closePrice;
	}
	
	public Long getVolume() {
		return volume;
	}
	public void setVolume(Long volume) {
		this.volume = volume;
	}
	
	
	public NYSEParser(String record) {
		
		String[] tokens = record.split(",");
		
		this.stockTicker = tokens[0];
		this.tradeDate = tokens[1];
		this.openPrice = Float.parseFloat(tokens[2]);
		this.highPrice = Float.parseFloat(tokens[3]);
		this.lowPrice = Float.parseFloat(tokens[4]);
		this.closePrice = Float.parseFloat(tokens[5]);
		this.volume = Long.parseLong(tokens[6]);
		
		
	}
	
	public int getTradeYear() throws ParseException
	{
		SimpleDateFormat orignalTradeDateFormat = new SimpleDateFormat("dd-MMM-yyyy");
		Date origDate = null;
		
		origDate = orignalTradeDateFormat.parse(this.tradeDate);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(origDate);
		return calendar.get(Calendar.YEAR);
	}
	
	public String getTradeMonth()
	{
		SimpleDateFormat orignalTradeDateFormat = new SimpleDateFormat("dd-MMM-yyyy");
		SimpleDateFormat targetTradeDateFormat = new SimpleDateFormat("yyyy-MM");
		Date origDate = null;
		try {
			origDate = orignalTradeDateFormat.parse(tradeDate);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return targetTradeDateFormat.format(origDate);
	}
	
	public Long getTradeDateNumeric(){
		
		SimpleDateFormat orignalTradeDateFormat = new SimpleDateFormat("dd-MMM-yyyy");
		SimpleDateFormat targetTradeDateFormat = new SimpleDateFormat("yyyyMMdd");
		Date origDate = null;
		
		try {
			origDate = orignalTradeDateFormat.parse(tradeDate);
			
		} catch (ParseException e) {

			e.printStackTrace();
		}
		
		Long targetDate = Long.parseLong(targetTradeDateFormat.format(origDate));
		
		return targetDate;
	}

}
