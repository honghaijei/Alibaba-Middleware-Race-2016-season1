package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.MinuteMap;
import com.alibaba.middleware.race.MiddlewareRaceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class RatioCounter implements IBasicBolt {
    private static Logger LOG = LoggerFactory.getLogger(RatioCounter.class);
    //Map<Long, Double> counter1 = new HashMap<Long, Double>(3000);
    //Map<Long, Double> counter2 = new HashMap<Long, Double>(3000);
    MinuteMap counter1 = new MinuteMap();
    MinuteMap counter2 = new MinuteMap();
    Map<Long, Double> ratio = new HashMap<Long, Double>();
    int count;
    boolean dirty = false;
    long lastForce = System.currentTimeMillis();
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        int platform = tuple.getInteger(0);
        long minute = tuple.getLong(1);
        double amount = tuple.getDouble(2);

        if (amount < 0) {
            if (dirty && System.currentTimeMillis() - lastForce > 5000) {
                lastForce = System.currentTimeMillis();
                double prev1 = 0.0, prev2 = 0.0;
                TreeSet<Long> ls = new TreeSet<Long>(counter1.keySet());
                ls.addAll(counter2.keySet());
                for (long t : ls) {
                    Double v1 = counter1.get(t);
                    Double v2 = counter2.get(t);
                    if (v1 != null) {
                        prev1 += v1;
                    }
                    if (v2 != null) {
                        prev2 += v2;
                    }
                    long tm = t * 60;
                    String key = MiddlewareRaceConfig.prex_ratio + MiddlewareRaceConfig.team_code + tm;
                    double value = prev2 / prev1;
                    Double r = ratio.get(tm);
                    if (r == null || Math.abs(r - value) > 0.001) {
                        basicOutputCollector.emit(new Values(key, value));
                        ratio.put(tm, value);
                    }

                }
            }
            dirty = false;
            return;
        }
        dirty = true;
        if (platform == 0) {
            Double value = counter1.get(minute);
            double prev = value == null ? 0.0 : value;
            prev += amount;
            counter1.put(minute, prev);
        } else {
            Double value = counter2.get(minute);
            double prev = value == null ? 0.0 : value;
            prev += amount;
            counter2.put(minute, prev);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
    }

    @Override
    public void cleanup() {
        // TODO Auto-generated method stub

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}