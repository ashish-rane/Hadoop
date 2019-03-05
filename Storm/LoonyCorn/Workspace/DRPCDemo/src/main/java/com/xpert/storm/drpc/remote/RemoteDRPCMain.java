package com.xpert.storm.drpc.remote;

import com.xpert.storm.drpc.PlusTenBolt;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.drpc.LinearDRPCTopologyBuilder;

import java.util.ArrayList;

public class RemoteDRPCMain {

    public static void main(String[] args) throws Exception{

        // In case of remote DRPC server the name of the function should match the topology name
        LinearDRPCTopologyBuilder builder = new LinearDRPCTopologyBuilder("drpc-plusTen");
        builder.addBolt(new PlusTenBolt(), 3);

        Config conf = new Config();

        // Add drpc servers to configuration
        ArrayList<String> drpcServers = new ArrayList<>();
        drpcServers.add("localhost");

        conf.put(Config.DRPC_SERVERS, drpcServers);
        conf.put(Config.DRPC_PORT, "3772");

        // Note: The name of the topology is same as the function name
        StormSubmitter.submitTopology("drpc-plusTen", conf, builder.createRemoteTopology());
    }
}
