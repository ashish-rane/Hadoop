package filesystem;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

public class CopyMerge {

	public static void main(String[] args) throws IOException {

		String uri = args[0];
		Configuration conf = new Configuration();
		conf.set(FileSystem.FS_DEFAULT_NAME_KEY, "file:///");
		
		FileSystem fs = FileSystem.get(URI.create(uri), conf);
		Path srcpath = new Path(args[0]);
		
		Path targetPath = new Path(args[1]);
		
		boolean copyMerge = FileUtil.copyMerge(fs, srcpath, fs, targetPath, false, conf, null);
		
		if(copyMerge){
			System.out.println("Merge Successfull");
		}
		
	}

}
