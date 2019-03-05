package com.xpert.storm.trident.twitter;

import org.apache.storm.Config;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import twitter4j.*;
import twitter4j.conf.ConfigurationBuilder;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterTridentSpout implements IBatchSpout {

    // Queue for tweets
    private LinkedBlockingQueue<Status> queue;

    // Stream of tweets
    private TwitterStream twitterStream;


    @Override
    public void open(Map map, TopologyContext topologyContext) {
        ConfigurationBuilder builder = new ConfigurationBuilder();
        builder.setDebugEnabled(true)
                .setOAuthConsumerKey("tpAestpXtM2pYAlomfZr6LN7d")
                .setOAuthConsumerSecret("MCQ1aVPypaBOZIlg7MDp36znULAIcmf9Cj8xfxodyVyLpILpQu")
                .setOAuthAccessToken("124163864-koQiHbqAF1QvLUzGqMb2ITvWk60jaa5yOsgJeaT7")
                .setOAuthAccessTokenSecret("qdMSjnab0O49k1pnck0fgtVQre60VN7pb0qkSC2vSYwJE");
        this.twitterStream = new TwitterStreamFactory(builder.build()).getInstance();

        this.queue = new LinkedBlockingQueue<>();

        // Create a listener for the tweets
        final StatusListener listener = new StatusListener() {

            // If there is a tweet add to queue
            @Override
            public void onStatus(Status status) {
                queue.offer(status);
            }

            @Override
            public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

            }

            @Override
            public void onTrackLimitationNotice(int i) {

            }

            @Override
            public void onScrubGeo(long l, long l1) {

            }

            @Override
            public void onStallWarning(StallWarning stallWarning) {

            }

            @Override
            public void onException(Exception e) {

            }
        };

        // Add the listener to the twitter stream
        this.twitterStream.addListener(listener);

        final FilterQuery filter =  new FilterQuery();
        // topics
        filter.track(new String[] { "ronaldo"});

        // apply the filter
        twitterStream.filter(filter);
    }

    @Override
    public void emitBatch(long l, TridentCollector tridentCollector) {

        // try to fetch tweet from queue
        final Status status = queue.poll();

        if(status == null){
            Utils.sleep(50);
        }
        else{
            tridentCollector.emit(new Values(status));
        }
    }

    @Override
    public void ack(long l) {

    }

    @Override
    public void close() {
        twitterStream.shutdown();
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return new Config();
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("tweet");
    }
}
