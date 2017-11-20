package com.xpert.zookeeper.sample1;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKClientManager implements ZKManager{
	
	private static ZooKeeper zk;
	private static ZKConnection zkConn;

	public ZKClientManager(){
		initialize();
	}
	
	private void initialize(){
		try{
			zkConn = new ZKConnection();
			zk = zkConn.connect("sandbox.hortonworks.com");
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public void closeConnection(){
		try{
			zkConn.close();
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
	}
	
	public void create(String path, byte[] data) throws KeeperException, InterruptedException {
		
		zk.create(path,data,ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	}

	public Stat getZNodeStats(String path) throws KeeperException, InterruptedException {
		
		Stat stat = zk.exists(path, true);
		if(stat != null){
			
			System.out.println("Node exists and the node version is " + stat.getVersion());
		}
		else{
			System.out.println("Node does not exist");
		}
		
		return stat;
	}

	public Object getZNodeData(String path, boolean watchFlag) throws KeeperException, InterruptedException {
		
		try{
			Stat stat = getZNodeStats(path);
			byte[] b = null;
			if(stat != null){
				if(watchFlag){
					ZKWatcher watch = new ZKWatcher();
					b = zk.getData(path, watch, stat);
					watch.await();
				}
				else{
					b = zk.getData( path, null, null);
				}
				
				
				String data = new String(b, "UTF-8");
				System.out.println(data);
				
				return data;
			}
			else{
				System.out.println("Node does not exist.");
			}
		}
		catch(Exception ex){
			ex.printStackTrace();
		}
		return null;
	}

	public void update(String path, byte[] data) throws KeeperException, InterruptedException {
		
		int version = zk.exists(path, true).getVersion();
		zk.setData(path, data, version);
	}

	public List<String> getZNodeChildren(String path) throws KeeperException, InterruptedException {
		
		Stat stat = getZNodeStats(path);
		List<String> children = null;
		
		if(stat != null){
			children = zk.getChildren( path, false);
			
			for(String child : children){
				System.out.println(child);
			}
		}
		else{
			System.out.println("Node does not exist.");
		}
		return null;
	}

	public void deleteZNode(String path) throws KeeperException, InterruptedException {
		
		int version = zk.exists(path, true).getVersion();
		zk.delete(path, version);
	}

}
