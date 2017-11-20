package com.xpert.pig.udfs;

import java.io.IOException;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class VALID_DATE extends EvalFunc<DateTime> {

	    public DateTime exec(Tuple input) throws IOException {
	        if (input == null || input.size() < 1 || input.get(0) == null) {
	            return null;
	        }
	        try{
	        	
	        	DateTimeFormatter formatter = DateTimeFormat.forPattern((String)input.get(1));
	        	DateTime dt = DateTime.parse((String)input.get(0), formatter );
	        	return dt;
	        }
	        catch(Exception ex){
	        	return null;
	        }
	    }

	  
}
