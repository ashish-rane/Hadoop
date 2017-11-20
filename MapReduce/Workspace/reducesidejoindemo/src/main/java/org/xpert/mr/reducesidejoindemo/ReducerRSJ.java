package org.xpert.mr.reducesidejoindemo;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerRSJ extends Reducer<CompositeKeyWritableRSJ, Text, NullWritable, Text>{

	private StringBuilder reduceValueBuilder = new StringBuilder();
	private NullWritable nullWritableKey = NullWritable.get();
	private Text reduceOutputValue = new Text();
	private String seperator = ",";
	private MapFile.Reader deptMapReader = null;
	
	private Text txtMapFileLookupKey = new Text();
	private Text txtMapFileLookupValue = new Text();
	
	
	@Override
	protected void setup(
			Reducer<CompositeKeyWritableRSJ, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		// Get the side data (department) from the distributed cache
		Path[] cacheFileLocal = DistributedCache.getLocalCacheArchives(context.getConfiguration());
		
		for(Path path : cacheFileLocal){
			
			if(path.getName().trim() .equals("departments_map.tar.gz")){
				
				URI uriUncompressedFile = new File(path.toString() + "/departments_map" ).toURI();
				
				initializeDepartmentsMap(uriUncompressedFile, context);
			}
		}
	}


	@SuppressWarnings("deprecation")
	private void initializeDepartmentsMap(
			URI uriUncompressedFile,
			Reducer<CompositeKeyWritableRSJ, Text, NullWritable, Text>.Context context) throws IOException {
		
		
		// Initialize reader of the map file (side data)
		FileSystem dfs = FileSystem.get(context.getConfiguration());
		
		deptMapReader = new MapFile.Reader(dfs, uriUncompressedFile.toString(), context.getConfiguration());
		
	}
	
	
	private StringBuilder buildOutputValue (CompositeKeyWritableRSJ key, StringBuilder reduceValueBuilder, Text value){
		
		if(key.getSrcIndex() == 1){
			
			// 1= Employee Data
			
			// Get the department name from the Mapfile in distributed cache
			
			
			// insert the join key (empNo) to the beginning of the string builder
			reduceValueBuilder.append(key.getJoinKey()).append(seperator);
			
			String[] arrEmpAttributes = value.toString().split(",");
			txtMapFileLookupKey.set(arrEmpAttributes[3]);
			
			
			try {
				deptMapReader.get(txtMapFileLookupKey,txtMapFileLookupValue);
			} catch (IOException e) {
				
				txtMapFileLookupValue.set("");
				e.printStackTrace();
			}
			finally{
				
				txtMapFileLookupValue.set(
						(txtMapFileLookupValue.equals(null) || txtMapFileLookupValue.equals(""))? "NOT-FOUND":txtMapFileLookupValue.toString()
								);
			}
			
			//Append the department name  to the map values to form complete CSV of employee attributes
			reduceValueBuilder.append(value.toString())
							  .append(seperator)
							  .append(txtMapFileLookupValue.toString())
							  .append(seperator);
										
		}
		else if(key.getSrcIndex() == 2){
			
			// Current Salary data. 1-1 on join key
			
			// Salary Data: Just append the salary drop the effective date
			String[] arrSalAttributes = value.toString().split(",");
			
			reduceValueBuilder.append(arrSalAttributes[0])
							  .append(seperator);
			
		}
		else{
			
			// Historical Salary Data
			
			// Get the salary but extract only the current salary (i.e effective data 9999-01-01)
			String[] arrSalAttributes = value.toString().split(",");
			
			if(arrSalAttributes[1].toString() == "9999-01-01"){
				// Salary Data; append
				
				reduceValueBuilder.append(arrSalAttributes[0])
								  .append(seperator);
			}
		}
		
		// Reset 
		txtMapFileLookupKey.set("");
		txtMapFileLookupValue.set("");
		
		return reduceValueBuilder;
	}


	@Override
	protected void reduce(
			CompositeKeyWritableRSJ key,
			Iterable<Text> values,
			Reducer<CompositeKeyWritableRSJ, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
	
		// Iterate through the values; First set is CSV of employee data
		// Second set is the Salary Data
		// The Data is already sorted by virtue of secodnary sort; append each value
		
		for(Text val : values){
			
			buildOutputValue(key, reduceValueBuilder, val);
		}
		
		// Drop last comma, set value and emit output
		if(reduceValueBuilder.length() > 1){
			reduceValueBuilder.setLength(reduceValueBuilder.length() - 1);
			
			// Emit output
			reduceOutputValue.set(reduceValueBuilder.toString());
			context.write(nullWritableKey, reduceOutputValue);
		}
		else
		{
			System.out.println("Keys= " + key.getJoinKey() +
								"Src= " + key.getSrcIndex());
		}
		
		// reset variables
		reduceValueBuilder.setLength(0);
		reduceOutputValue.set("");
	}


	@Override
	protected void cleanup(
			Reducer<CompositeKeyWritableRSJ, Text, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		deptMapReader.close();
	}
	
	
	
}
