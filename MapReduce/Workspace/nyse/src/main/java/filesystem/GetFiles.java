package filesystem;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class GetFiles {

	public static void main(String[] args){
		
		String uri = args[0];
		Configuration conf = new Configuration();
		
		try {
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path path = new Path(uri + args[1]);
			
			FileStatus[] status = fs.globStatus(path);
			Path[] paths = FileUtil.stat2Paths(status);
			
			for(Path p : paths){
				System.out.println(p.toString());
			}
			
		} catch (IOException e) {

			e.printStackTrace();
		}
	}
	
}
