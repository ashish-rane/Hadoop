package com.xpert.storm;

import org.apache.storm.LocalCluster;
import org.apache.storm.generated.TopologySummary;

import java.util.List;
import java.util.stream.Collectors;

public class Ultities {

    public static final boolean topologyExists(final String topologyName,
                                               final LocalCluster cluster){
        // List all the topologies on the local cluster
        final List<TopologySummary> toplogies = cluster.getClusterInfo().get_topologies();

        // Search with teh given name
        if(null != toplogies && !toplogies.isEmpty()){
            final List<TopologySummary> collect = toplogies.stream()
                    .filter(p -> p.get_name().equals(topologyName))
                    .collect(Collectors.toList());

            if(null != collect && !collect.isEmpty()){
                return true;
            }
        }
        return false;
    }
}
