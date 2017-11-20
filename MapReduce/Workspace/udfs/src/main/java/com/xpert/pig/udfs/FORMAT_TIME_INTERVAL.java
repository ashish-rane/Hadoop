package com.xpert.pig.udfs;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class FORMAT_TIME_INTERVAL extends EvalFunc<String>{

	@Override
	public String exec(Tuple tuple) throws IOException {
		
		long intervalSecs = Integer.parseInt(tuple.get(0).toString());
		
		
		long minutesInSecs = 60;
		long hoursInSecs = minutesInSecs * 60;
		long daysInSecs = hoursInSecs * 24;
		
		long elapsedDays = intervalSecs/daysInSecs;
		long remaining = intervalSecs % daysInSecs;
		long elapsedHours = remaining/hoursInSecs;
		remaining = remaining % hoursInSecs;
		
		long elapsedMinutes = remaining / minutesInSecs;
		remaining = remaining % minutesInSecs;
		
		long elapsedSecs = remaining;
		
		return String.format("%sd %sh %sm %ss ", elapsedDays, elapsedHours, elapsedMinutes, elapsedSecs);

		
	}

}
