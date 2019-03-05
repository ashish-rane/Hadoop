package com.xpert.storm.drpc.local;

import com.xpert.storm.drpc.PlusTenBolt;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

/**
 * The DRPC Topology is used to execute a function (specified in the bolt)
 * and return a value to the DRPC client. It allows us to make RPC using storm
 */
public class LocalDRPCMain
{
    public static void main( String[] args ) throws InterruptedException {
        // The parameter supplied is the name of the function DRPC topology executes
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("plusTen");
        builder.addBolt(new PlusTenBolt(), 3);

        Config conf = new Config();
        //conf.setDebug(true);

        // Create a drpc client which can call the function and receive response.
        LocalDRPC drpcClient = new LocalDRPC();

        // Create a local cluster and submit the topology providing the reference to the DRPC client
        // to the topology.
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("DRPC-PlusTen", conf, builder.createLocalTopology(drpcClient));

        // Call the function using execute() method specifying same function name as specified
        // when creating the topology builder above and the number is the input.
        for (Integer num : new Integer[]{53, 67, 75}){
            System.out.println("Result for " + num + " is:  " +
            drpcClient.execute("plusTen", num.toString()));
        }

        Thread.sleep(60000);

        cluster.killTopology("DRPC-PlusTen");
        cluster.shutdown();
        drpcClient.shutdown();
    }
}
