import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;


public class TestMain {

	private static BufferedReader reader;

	public static void main(String[] args) {
		
		try {
		FileReader fr = new FileReader( "C:/tmp/samp.csv");
		HashMap<Long, String> records = new HashMap<Long, String>();
		
		
			java.io.File file = new File("C:/tmp/samp.csv");
			if(file.exists())
			{
			
				reader = new BufferedReader(fr);
				
				String line = null;
				while( (line = reader.readLine()) != null)		
				{
					records.put(1L, line);
				}
			}
			
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}
		
		//Demo();

	}
	
	 public static void Demo() 
	  {
	    FileReader fr = null;
	    BufferedReader br = null;
	    
	    FileWriter fw = null;
	    BufferedWriter bw = null;
	    
	    try
	    {

	      
	      fw = new FileWriter("C:/temp/TestFileWriter.txt");
	      bw = new BufferedWriter(fw);
	      
	      bw.write("This is so easy");
	      bw.flush();
	      fw.close();
	      fw = null;
	      bw.close();
	      bw = null;
	      
	      fr = new FileReader("C:/temp/TestFileWriter.txt");
	      br = new BufferedReader(fr);
	      String s;
	      while( (s = br.readLine()) != null)
	      {
	        System.out.println(s);
	      }
	    } 
	    catch (FileNotFoundException e)
	    {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    }
	    catch (IOException e)
	    {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
	    }
	    finally
	    {
	      try
	      {
	        if(fr != null)
	        {
	          fr.close();
	        }
	        if(br != null)
	        {
	          br.close();
	        }
	        
	        if(fw != null)
	        {
	          fw.close();
	        }
	        if(bw != null)
	        {
	          bw.close();
	        }
	      }
	      catch(Exception e)
	      {
	        e.printStackTrace();
	      }
	    }
	    
	   
	  }

}
