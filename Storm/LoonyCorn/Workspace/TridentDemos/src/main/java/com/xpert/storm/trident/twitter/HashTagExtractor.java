package com.xpert.storm.trident.twitter;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import twitter4j.HashtagEntity;
import twitter4j.Status;

public class HashTagExtractor extends BaseFunction {
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        // Get the tweet
        Status  status = (Status)tridentTuple.get(0);

        // Loop through the hashtags
        for(HashtagEntity hashtag : status.getHashtagEntities()){

            // Emit each hashtag
            tridentCollector.emit(new Values(hashtag.getText()));
        }
    }
}
