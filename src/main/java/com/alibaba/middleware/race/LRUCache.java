package com.alibaba.middleware.race;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.SplitSentence;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by hahong on 2016/7/2.
 */
public class LRUCache extends LinkedHashMap<String, Double> {
    private int capacity;
    private TairOperatorImpl tairOperator;
    public LRUCache(int capacity, TairOperatorImpl tairOperator) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
        this.tairOperator = tairOperator;
    }

    protected boolean removeEldestEntry(Map.Entry<String, Double> entry) {
        synchronized (this) {
            boolean res = this.size() > capacity;
            if (res) {
                SplitSentence.save(tairOperator, entry.getKey(), entry.getValue());
            }
            return res;
        }
    }

    public double get(String key) {
        synchronized (this) {
            if (this.containsKey(key)) {
                return super.get(key);
            } else {
                return -1;
            }
        }
    }
    public Set<String> keySet() {
        synchronized (this) {
            return super.keySet();
        }
    }

    public void set(String key, double value) {
        synchronized (this) {
            super.put(key, value);
        }
    }
}