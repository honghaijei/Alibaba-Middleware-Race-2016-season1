package com.alibaba.middleware.race;

import backtype.storm.topology.BasicOutputCollector;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;

import java.util.*;

/**
 * Created by hahong on 2016/7/2.
 */
public class TairLRUCache extends LinkedHashMap<String, Double> {
    private int capacity;
    BasicOutputCollector collector;
    public TairLRUCache(int capacity) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
    }

    protected boolean removeEldestEntry(Map.Entry<String, Double> entry) {
        boolean res = this.size() > capacity;
        if (res) {
            collector.emit(new backtype.storm.tuple.Values(entry.getKey(), entry.getValue()));
        }
        return res;

    }

    public double get(String key) {
        if (this.containsKey(key)) {
            return super.get(key);
        } else {
            return -1;
        }
    }

    public void set(String key, double value, BasicOutputCollector basicOutputCollector) {
        this.collector = basicOutputCollector;
        super.put(key, value);
    }

    public void force(BasicOutputCollector basicOutputCollector) {
        for (Map.Entry<String, Double> e : this.entrySet()) {
            basicOutputCollector.emit(new backtype.storm.tuple.Values(e.getKey(), e.getValue()));
        }
        this.clear();
    }
}