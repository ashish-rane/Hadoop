package com.xpert.zookeeper.sample1;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

public class ZKConnection {

	// Local zookeeper object to access the ensemble
	private ZooKeeper zoo;
	final CountDownLatch connectionLatch  = new CountDownLatch(1);
	
	public ZKConnection(){
		
	}
	
	public ZooKeeper connect(String host) throws IOException, InterruptedException{
		
		zoo = new ZooKeeper(host, 2000, 
				new Watcher(){
			
			public void process(WatchedEvent we){
				if(we.getState() == Watcher.Event.KeeperState.SyncConnected){
					connectionLatch.countDown();
				}
			}
		});
		
		connectionLatch.wait();
		
		return zoo;
	}
	
	public void close() throws InterruptedException{
		zoo.close();
	}
}
