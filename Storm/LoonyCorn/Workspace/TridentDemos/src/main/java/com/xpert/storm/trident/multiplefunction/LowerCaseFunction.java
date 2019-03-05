package com.xpert.storm.trident.multiplefunction;

import org.apache.storm.trident.operation.MapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

/**
 * Our custom map function. the map function requires the schema for input and
 * output tuple is same.
 */
public class LowerCaseFunction implements MapFunction {
    @Override
    public Values execute(TridentTuple tridentTuple) {
        return new Values(tridentTuple.getString(0).toLowerCase());
    }
}
