package com.alibaba.middleware.race;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by hahong on 2016/7/3.
 */
public class LRUFilter extends LinkedHashMap<String, Boolean> {
    private int capacity;
    public LRUFilter(int capacity) {
        super(capacity, 0.75f, true);
        this.capacity = capacity;
    }

    protected boolean removeEldestEntry(Map.Entry<String, Boolean> entry) {
        synchronized (this) {
            boolean res = this.size() > capacity;
            return res;
        }
    }

    public Boolean get(String key) {
        synchronized (this) {
            if (this.containsKey(key)) {
                return super.get(key);
            } else {
                return null;
            }
        }
    }

    public void set(String key, Boolean value) {
        synchronized (this) {
            super.put(key, value);
        }
    }
}