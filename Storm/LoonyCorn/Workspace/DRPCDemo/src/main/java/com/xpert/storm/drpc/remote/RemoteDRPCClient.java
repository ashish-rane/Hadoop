package com.xpert.storm.drpc.remote;

import org.apache.storm.utils.DRPCClient;
import org.apache.storm.utils.Utils;

import java.util.Map;

public class RemoteDRPCClient {

    public static void main(String[] args) throws  Exception{

        // Get the storm configuration
        Map conf = Utils.readStormConfig();

        // Using the storm config supply the address, port of DRPCServer
        DRPCClient client  = new DRPCClient(conf, "localhost", 3772);

        // The name of the function must match what is specified on the server.
        for(Integer num: new Integer[]{53, 67, 75}){
            System.out.println("Result for " + num + " is: " +
            client.execute("drpc-plusTen", num.toString()));
        }
    }
}
