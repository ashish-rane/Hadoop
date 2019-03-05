package com.xpert.storm.failures.manual;

import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.*;

public class IntegerSpout extends BaseRichSpout {

    private static final Integer MAX_FAIL = 3;
    private SpoutOutputCollector collector;
    private LinkedList<Integer> toSend;
    private Map<Integer, Integer> failureCount;

    static Logger logger = Logger.getLogger(IntegerSpout.class);

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.failureCount = new HashMap<>();
        this.toSend = new LinkedList<>();
        for (int i = 0; i< 100; i++){
            toSend.push(i);
        }
    }

    @Override
    public void nextTuple() {
        if(!toSend.isEmpty()){
            int num = toSend.pop();
            int bucket = num/10;
            this.collector.emit(new Values(num, bucket));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("number", "bucket"));
    }

    // Methods to control the message delivery.

    @Override
    public void ack(Object msgId) {
        System.out.println(msgId + " Successfull");
    }

    @Override
    public void fail(Object msgId) {
        Integer number = (Integer)msgId;
        if(failureCount.containsKey(number)){
            failureCount.put(number, failureCount.get(number) + 1);
        }
        else{
            failureCount.put(number, 1);
        }

        if(failureCount.get(number) <= MAX_FAIL){
            toSend.push(number);
            logger.info("Re-sending the number: ["  + number + "]");
        }
        else{
            logger.info("Sending Message: [" + number + "]");
        }
    }
}
