package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.LRUCache;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class SplitSentence implements IRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(SplitSentence.class);
    OutputCollector collector;
    TairOperatorImpl tairOperator;
    Map<Long, Double> counter = new HashMap<Long, Double>();
    LRUCache cache;
    long recvCount = 0;
    public static void save(TairOperatorImpl tairOperator, String key, double value) {
        boolean succ = tairOperator.write(key, value);
        if (succ) {
            SplitSentence.LOG.info("Write to tair success, " + key + "\t" + value);
        } else {
            SplitSentence.LOG.info("Write to tair error, " + key + "\t" + value);
        }
    }

    @Override
    public void execute(Tuple tuple) {
        int platform = tuple.getInteger(0);
        long timestamp = tuple.getLong(1);
        double amount = tuple.getDouble(2);
        if (platform != 0) {
            return;
        }
        if (amount < 0) {
            LOG.info("get end signal, force all cache to tair.");
            for (String key : new ArrayList<String>(cache.keySet())) {
                this.save(tairOperator, key, cache.get(key));
            }
            cache.clear();
            return;
        }
        LOG.info(String.format("get a payment message, platform: %d, timestamp: %d, amount: %f, count=%d", platform, timestamp, amount, recvCount++));
        long minute = timestamp / 1000 / 60;
        double value = counter.containsKey(minute) ? counter.get(minute) + amount : amount;
        counter.put(minute, value);
        String platformPrefix = platform == 0 ? RaceConfig.prex_taobao : RaceConfig.prex_tmall;
        String key = platformPrefix + RaceConfig.team_code + (minute * 60);
        cache.set(key, value);
        /*
        boolean succ = tairOperator.write(platformPrefix + RaceConfig.team_code + minute, value);
        if (succ) {
            LOG.info("Write to tair success, " + platformPrefix + RaceConfig.team_code + (minute * 60) + "\t" + value);
        } else {
            LOG.info("Write to tair error, " + platformPrefix + RaceConfig.team_code + (minute * 60) + "\t" + value);
        }
        */
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("platform", "minute", "amount"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        cache = new LRUCache(2, tairOperator);
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
