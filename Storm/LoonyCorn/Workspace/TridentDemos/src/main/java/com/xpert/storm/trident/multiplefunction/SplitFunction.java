package com.xpert.storm.trident.multiplefunction;

import org.apache.storm.trident.operation.FlatMapFunction;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;

/**
 * The flatmap function requires that the output tuple is a list of
 * tuples. The schema of each tuple in the list must match to the input tuple.
 */
public class SplitFunction implements FlatMapFunction {
    @Override
    public Iterable<Values> execute(TridentTuple tridentTuple) {
        List<Values> list = new ArrayList<>();
        for (String word: tridentTuple.getString(0).split(" ")){
            list.add(new Values(word));
        }
        return list;
    }
}
