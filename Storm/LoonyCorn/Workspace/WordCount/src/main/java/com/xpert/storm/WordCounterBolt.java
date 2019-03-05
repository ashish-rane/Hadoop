package com.xpert.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class WordCounterBolt extends BaseBasicBolt {

    private Map<String, Integer> wordCount = new HashMap<>();



    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String word = tuple.getString(0).toString();
        if(wordCount.containsKey(word)){
            wordCount.put(word,wordCount.get(word) + 1);
        }
        else{
            wordCount.put(word, 1);
        }

        basicOutputCollector.emit(new Values(word, wordCount.get(word)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word", "count"));
    }

    @Override
    public void cleanup() {
        super.cleanup();
    }
}
