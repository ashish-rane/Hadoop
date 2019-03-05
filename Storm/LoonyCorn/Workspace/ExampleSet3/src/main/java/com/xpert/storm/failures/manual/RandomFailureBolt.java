package com.xpert.storm.failures.manual;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

public class RandomFailureBolt extends BaseRichBolt {

    private static final Integer MAX_PERCENT_FAIL = 80;
    private OutputCollector collector ;
    private Random random = new Random();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {

        Integer r = random.nextInt(100);
        if( r < MAX_PERCENT_FAIL){
            collector.emit(tuple, new Values(tuple.getValue(0), tuple.getValue(1)));
            // do manual ack
            collector.ack(tuple);
        }
        else{
            // manually indicate failure
            collector.fail(tuple);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("number", "bucket"));
    }
}
