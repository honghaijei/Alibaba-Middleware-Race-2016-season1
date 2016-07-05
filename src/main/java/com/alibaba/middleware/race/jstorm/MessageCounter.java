package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.LRUCache;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class MessageCounter implements IRichBolt {
    public static Logger LOG = LoggerFactory.getLogger(MessageCounter.class);
    OutputCollector collector;
    TairOperatorImpl tairOperator;
    ArrayList<HashMap<Long, Double>> counter;

    LRUCache cache;
    long recvCount = 0;


    @Override
    public void execute(Tuple tuple) {
        int platform = tuple.getInteger(0);
        long minute = tuple.getLong(1);
        double amount = tuple.getDouble(2);
        if (amount < 0) {
            //LOG.info("get end signal, force all cache to tair.");
            cache.force();
            return;
        }
        //LOG.info(String.format("get a payment message, platform: %d, minute: %d, amount: %f, count=%d", platform, minute, amount, recvCount++));
        double value = counter.get(platform).containsKey(minute) ? counter.get(platform).get(minute) + amount : amount;
        counter.get(platform).put(minute, value);
        String platformPrefix = platform == 0 ? RaceConfig.prex_taobao : RaceConfig.prex_tmall;
        String key = platformPrefix + RaceConfig.team_code + (minute * 60);
        cache.set(key, value);
        //this.save(tairOperator, key, value);

        //LOG.info(String.format("put payment to cache, platform: %d, minute: %d, amount: %f, cache size: %d", platform, minute, value, cache.size()));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("platform", "minute", "amount"));
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        counter = new ArrayList<HashMap<Long, Double>>();
        counter.add(new HashMap<Long, Double>());
        counter.add(new HashMap<Long, Double>());
        tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        cache = new LRUCache(5, tairOperator, this.LOG);
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
