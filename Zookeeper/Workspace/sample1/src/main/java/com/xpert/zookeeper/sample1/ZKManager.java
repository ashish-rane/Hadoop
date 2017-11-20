package com.xpert.zookeeper.sample1;

import java.util.List;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

public interface ZKManager{
	
	/**
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void create(String path, byte[] data) throws KeeperException,InterruptedException;
	
	/**
	 * 
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Stat getZNodeStats(String path) throws KeeperException, InterruptedException;
	
	/**
	 * 
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public Object getZNodeData(String path, boolean watchFlag) throws KeeperException, InterruptedException;
	
	/**
	 * 
	 * @param path
	 * @param data
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void update(String path, byte[] data) throws KeeperException, InterruptedException;
	
	/**
	 * 
	 * @param path
	 * @return
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public List<String> getZNodeChildren(String path) throws KeeperException, InterruptedException;
	
	/**
	 * 
	 * @param path
	 * @throws KeeperException
	 * @throws InterruptedException
	 */
	public void deleteZNode(String path) throws KeeperException, InterruptedException;
}

