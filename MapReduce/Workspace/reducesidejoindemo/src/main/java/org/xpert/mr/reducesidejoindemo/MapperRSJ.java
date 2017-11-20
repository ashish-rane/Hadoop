package org.xpert.mr.reducesidejoindemo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MapperRSJ extends Mapper<LongWritable, Text, CompositeKeyWritableRSJ, Text>{

	CompositeKeyWritableRSJ ckwKey = new CompositeKeyWritableRSJ();
	
	Text txtValue = new Text();
	int srcIndex = 0;
	
	StringBuilder strMapValueBuilder = new StringBuilder();
	
	List<Integer> listRequiredAttributeList = new ArrayList<Integer>();

	@Override
	protected void setup(
			Mapper<LongWritable, Text, CompositeKeyWritableRSJ, Text>.Context context)
			throws IOException, InterruptedException {
		
		// get the source index 1: employee, 2: Salary
		
		// add as configuration in driver
		FileSplit fsFileSplit = (FileSplit)context.getInputSplit();
		srcIndex = Integer.parseInt(context.getConfiguration().get(fsFileSplit.getPath().getName()));
		
		// initialize list of keys to emit as map output based on 
		// srcindex(1=employee, 2= salary, 3= historical salary
		
		if(srcIndex == 1){
			listRequiredAttributeList.add(2); // FName
			listRequiredAttributeList.add(3); //LName
			listRequiredAttributeList.add(4); // Gender
			listRequiredAttributeList.add(6); // Dept no
			
		}
		else // salary
		{
			listRequiredAttributeList.add(1); // salary
			listRequiredAttributeList.add(3); //Effective-to-date, 9999-01-01 indicates current salary
		}
		
		
	}
	
	/**
	 * This method returns CSV list of values to emit based on data entity
	 * @param arrEntityAttributesList
	 * @return
	 */
	private String buildMapValue(String arrEntityAttributesList[]){
		
		// Initialize
		strMapValueBuilder.setLength(0);
		
		// Build list of values to emit based on source employee/salary
		for(int i = 1; i < arrEntityAttributesList.length; i++){
			// if the input field is in the list of required outputs 
			// append to the string builder
			if(listRequiredAttributeList.contains(i)){
				strMapValueBuilder.append(arrEntityAttributesList[i]).append(",");
			}
		}
		
		// drop last comma
		if(strMapValueBuilder.length() > 0){
			strMapValueBuilder.setLength(strMapValueBuilder.length() - 1);
		}
		
		return strMapValueBuilder.toString();
	}

	@Override
	protected void map(
			LongWritable key,
			Text value,
			Mapper<LongWritable, Text, CompositeKeyWritableRSJ, Text>.Context context)
			throws IOException, InterruptedException {
		
		
		if(value.toString().length() > 0){
			String[] arrEntityAttributes = value.toString().split(",");
			
			ckwKey.setJoinKey(arrEntityAttributes[0]);
			ckwKey.setSrcIndex(srcIndex);
			txtValue.set(buildMapValue(arrEntityAttributes));
			
			context.write(ckwKey, txtValue);
			
		}
	}
	
	
	
	
}
