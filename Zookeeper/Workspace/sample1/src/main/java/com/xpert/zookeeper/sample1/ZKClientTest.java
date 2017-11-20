package com.xpert.zookeeper.sample1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public class ZKClientTest {

	private static ZKClientManager zkManager = new ZKClientManager();
	
	private String path = "/Ashish-Node";
	byte[] data = "Rane ZK CLient Data".getBytes();
	

	public void Test() throws KeeperException, InterruptedException{
		zkManager.create(path, data);
		Stat stat = zkManager.getZNodeStats(path);
		
		if(stat != null){
			zkManager.getZNodeData(path, false);
			
			byte[] updatedData = "Rane ZK Client Updated".getBytes();
			zkManager.update(path, updatedData);
			
			String retrivedData = (String)zkManager.getZNodeData(path, true);
			
			
			zkManager.create(path + "/Children", "Aditya-Trisha".getBytes());
			
			stat = zkManager.getZNodeStats(path + "/Children");
			
			zkManager.getZNodeChildren(path);
		}
		
		zkManager.deleteZNode(path);
	}
	
	
}
