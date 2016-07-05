package com.alibaba.middleware.race;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;

import java.util.*;

/**
 * Created by hahong on 2016/7/2.
 */
public class LRUCache extends LinkedHashMap<String, Double> {
    private int capacity;
    private TairOperatorImpl tairOperator;
    private Logger LOG;
    public LRUCache(int capacity, TairOperatorImpl tairOperator, Logger LOG) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
        this.tairOperator = tairOperator;
        this.LOG = LOG;
    }

    protected boolean removeEldestEntry(Map.Entry<String, Double> entry) {
        boolean res = this.size() > capacity;
        if (res) {
            RaceUtils.save(LOG, tairOperator, entry.getKey(), entry.getValue());
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

    public void set(String key, double value) {
        super.put(key, value);
    }

    public void force() {
        for (Map.Entry<String, Double> e : this.entrySet()) {
            RaceUtils.save(this.LOG, tairOperator, e.getKey(), e.getValue());
        }
        this.clear();
    }
}