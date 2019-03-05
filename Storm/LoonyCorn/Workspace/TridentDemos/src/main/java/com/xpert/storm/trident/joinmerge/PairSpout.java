package com.xpert.storm.trident.joinmerge;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * This class generates a pair of random numbers and emits them as tuple
 */
public class PairSpout extends BaseRichSpout {

    private Random random = new Random();
    private SpoutOutputCollector collector;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Integer x1 = random.nextInt(10);
        Integer x2 = random.nextInt(10);

        collector.emit(new Values(x1.toString(), x2.toString()));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("x1", "x2"));
    }
}
