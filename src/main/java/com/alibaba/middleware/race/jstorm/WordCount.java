package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.sun.corba.se.impl.encoding.OSFCodeSetRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class WordCount implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(WordCount.class);
    OutputCollector collector;
    TairOperatorImpl tairOperator;
    TreeMap<Long, Double> counter1 = new TreeMap<Long, Double>();
    TreeMap<Long, Double> counter2 = new TreeMap<Long, Double>();
    TreeMap<Long, Double> ratio = new TreeMap<Long, Double>();
    int count;
    @Override
    public void execute(Tuple tuple) {
        int platform = tuple.getInteger(0);
        long timestamp = tuple.getLong(1);
        double amount = tuple.getDouble(2);
        long minute = timestamp / 1000 / 60;
        if (platform == 0) {
            Map.Entry<Long, Double> entry = counter1.lowerEntry(minute);
            double prev = entry == null ? 0.0 : entry.getValue();
            prev += amount;
            counter1.put(minute, prev);
            for (Map.Entry<Long, Double> t : new ArrayList<Map.Entry<Long, Double>>(counter1.tailMap(minute, false).entrySet())) {
                prev += t.getValue();
                counter1.put(t.getKey(), prev);
            }

        } else {
            Map.Entry<Long, Double> entry = counter2.lowerEntry(minute);
            double prev = entry == null ? 0.0 : entry.getValue();
            prev += amount;
            counter2.put(minute, prev);
            for (Map.Entry<Long, Double> t : new ArrayList<Map.Entry<Long, Double>>(counter2.tailMap(minute, false).entrySet())) {
                prev += t.getValue();
                counter2.put(t.getKey(), prev);
            }
        }
        TreeSet<Long> ls = new TreeSet<Long>(counter1.tailMap(minute).keySet());
        ls.addAll(counter2.tailMap(minute).keySet());
        for (long t : ls) {
            Map.Entry<Long, Double> entry1 = counter1.floorEntry(t);
            Map.Entry<Long, Double> entry2 = counter2.floorEntry(t);
            double r1 = entry1 == null ? 0.0 : entry1.getValue();
            double r2 = entry2 == null ? 0.0 : entry2.getValue();
            long tm = t * 60;
            boolean succ = tairOperator.write(RaceConfig.prex_ratio + RaceConfig.team_code + tm, r2 / r1);
            if (succ) {
                LOG.info("Write to tair success, " + RaceConfig.prex_ratio + RaceConfig.team_code + tm + "\t" + (r2 / r1));
            } else {
                LOG.info("Write to tair error, " + RaceConfig.prex_ratio + RaceConfig.team_code + tm + "\t" + (r2 / r1));
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
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