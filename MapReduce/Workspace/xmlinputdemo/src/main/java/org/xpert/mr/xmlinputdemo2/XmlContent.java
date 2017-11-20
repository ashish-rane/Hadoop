package org.xpert.mr.xmlinputdemo2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class XmlContent implements Writable{
	
	public byte[] bufferData;
    public int offsetData;
    public int lenghtData;
    
   
	  public XmlContent(byte[] bufferData, int offsetData, int lenghtData) 
	  {
		  this.bufferData = bufferData;
		  this.offsetData = offsetData;
		  this.lenghtData = lenghtData;
	  }
	  
	  public XmlContent(){
		  this(null,0,0);
	  }
	  
	  public void write(DataOutput out) throws IOException {
		  
		  out.write(bufferData);
		  out.writeInt(offsetData);
		  out.writeInt(lenghtData);
	  }

	  public void readFields(DataInput in) throws IOException {
		  
		  in.readFully(bufferData);
		  offsetData = in.readInt();
		  lenghtData = in.readInt();
	  }

	  public String toString() {
		  
		    return Integer.toString(offsetData) + ", "
		        + Integer.toString(lenghtData) +", "
		        + bufferData.toString();
      }	  

}
