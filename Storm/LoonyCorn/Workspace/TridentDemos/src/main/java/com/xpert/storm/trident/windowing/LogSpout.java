package com.xpert.storm.trident.windowing;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Spout class which generates logs containing either SUCCESS or FAILURE messages.
 * The max number of error messages generated are controlled using the constant eg 20%.
 */
public class LogSpout extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private Random random = new Random();

    private static final Integer MAX_PERCENT_FAIL = 20;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        Integer r = random.nextInt(100);
        if(r < MAX_PERCENT_FAIL){
            collector.emit(new Values("ERROR"));
        }
        else{
            collector.emit(new Values("SUCCESS"));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("log"));
    }
}
