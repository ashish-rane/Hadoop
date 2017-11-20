package com.xpert.zookeeper.sample1;

import org.apache.zookeeper.KeeperException;

/**
 * Hello world!
 *
 *	http://java.globinch.com/enterprise-services/zookeeper/apache-zookeeper-explained-tutorial-cases-zookeeper-java-api-examples/
 */
public class App 
{
    public static void main( String[] args ) throws KeeperException, InterruptedException
    {
        ZKClientTest test = new ZKClientTest();
        test.Test();
    }
}
