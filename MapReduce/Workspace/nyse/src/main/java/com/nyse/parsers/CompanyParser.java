package com.nyse.parsers;

public class CompanyParser {

	// Members
	private String stockTicker;
	private String name;
	private String lastSale;
	private String marketCap;
	private String adrsto;
	private String ipoYear;
	private String sector;
	private String industry;
	private String summaryQuote;
	
	/* 
	 * Properties
	 */
	
	public String getStockTicker() {
		return stockTicker;
	}
	public void setStockTicker(String stockTicker) {
		this.stockTicker = stockTicker;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getLastSale() {
		return lastSale;
	}
	public void setLastSale(String lastSale) {
		this.lastSale = lastSale;
	}
	public String getMarketCap() {
		return marketCap;
	}
	public void setMarketCap(String marketCap) {
		this.marketCap = marketCap;
	}
	public String getAdrsto() {
		return adrsto;
	}
	public void setAdrsto(String adrsto) {
		this.adrsto = adrsto;
	}
	public String getIpoYear() {
		return ipoYear;
	}
	public void setIpoYear(String ipoYear) {
		this.ipoYear = ipoYear;
	}
	public String getSector() {
		return sector;
	}
	public void setSector(String sector) {
		this.sector = sector;
	}
	public String getIndustry() {
		return industry;
	}
	public void setIndustry(String industry) {
		this.industry = industry;
	}
	public String getSummaryQuote() {
		return summaryQuote;
	}
	public void setSummaryQuote(String summaryQuote) {
		this.summaryQuote = summaryQuote;
	}
	
	// Constructor
	public CompanyParser(String record){
		
		String[] tokens = record.split("\\|");
		
		this.stockTicker = tokens [0];
		this.name = tokens [1];
		this.lastSale = tokens[2];
		this.marketCap = tokens[3];
		this.adrsto = tokens[4];
		this.ipoYear = tokens[5];
		this.sector = tokens[6];
		this.industry = tokens[7];
		this.summaryQuote = tokens[8];
	}
	
}
