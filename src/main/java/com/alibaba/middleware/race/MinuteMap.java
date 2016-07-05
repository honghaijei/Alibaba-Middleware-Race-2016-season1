package com.alibaba.middleware.race;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by hahong on 2016/7/6.
 */
public class MinuteMap implements Serializable {
    private Double[] values;
    private List<Long> keys = new ArrayList<Long>();
    public MinuteMap() {
        values = new Double[1200000];
    }
    public Double get(long minute) {
        return values[(int)(minute - MiddlewareRaceConfig.start_minute)];
    }
    public void put(long minute, double value) {
        int idx = (int)(minute - MiddlewareRaceConfig.start_minute);
        if (values[idx] == null) {
            keys.add(minute);
        }
        values[idx] = value;

    }
    public List<Long> keySet() {
        return keys;
    }
}
