package com.alibaba.middleware.race;

import backtype.storm.topology.BasicOutputCollector;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;

import java.util.*;

/**
 * Created by hahong on 2016/7/2.
 */
public class TairLRUCache extends LinkedHashMap<Long, Double> {
    private int capacity;
    BasicOutputCollector collector;
    String prefix;
    public TairLRUCache(int capacity, String prefix) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
        this.prefix = prefix;
    }

    protected boolean removeEldestEntry(Map.Entry<Long, Double> entry) {
        boolean res = this.size() > capacity;
        if (res) {
            collector.emit(new backtype.storm.tuple.Values(prefix + entry.getKey(), entry.getValue()));
        }
        return res;

    }

    public double get(Long key) {
        if (this.containsKey(key)) {
            return super.get(key);
        } else {
            return -1;
        }
    }

    public void set(Long key, double value, BasicOutputCollector basicOutputCollector) {
        this.collector = basicOutputCollector;
        super.put(key, value);
    }

    public void force(BasicOutputCollector basicOutputCollector) {
        for (Map.Entry<Long, Double> e : this.entrySet()) {
            basicOutputCollector.emit(new backtype.storm.tuple.Values(prefix + e.getKey(), e.getValue()));
        }
        this.clear();
    }
}