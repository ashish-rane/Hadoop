package com.xpert.pig.udfs;
import java.io.IOException;

import junit.framework.Assert;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.joda.time.DateTime;
import org.junit.Test;
public class udftests {

	@Test
	public void TestValidDateUDF() throws IOException{
		VALID_DATE validDate = new VALID_DATE();	
		Tuple tuple = TupleFactory.getInstance().newTuple(2);
		try{
		
		
	
		tuple.set(0,  "2015-09-11 13:44:31,587");
		tuple.set(1, "yyyy-MM-dd HH:mm:ss,SSS");
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		
		Assert.assertNotNull(validDate.exec(tuple));
		
		
	}
}
