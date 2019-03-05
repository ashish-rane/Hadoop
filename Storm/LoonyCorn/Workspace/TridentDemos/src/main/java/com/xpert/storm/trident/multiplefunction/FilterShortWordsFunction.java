package com.xpert.storm.trident.multiplefunction;

import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.tuple.TridentTuple;

/**
 * The Filter Function returns a true (to keep) or false(to filter out)
 */
public class FilterShortWordsFunction extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tridentTuple) {
        return tridentTuple.getString(0).length() > 3;
    }
}
