package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.TairLRUCache;
import com.alibaba.middleware.race.MinuteMap;
import com.alibaba.middleware.race.MiddlewareRaceConfig;

import java.util.ArrayList;
import java.util.Map;


public class MessageCounter implements IBasicBolt {
    ArrayList<MinuteMap> counter;
    ArrayList<TairLRUCache> cache;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        int platform = tuple.getInteger(0);
        long minute = tuple.getLong(1);
        double amount = tuple.getDouble(2);
        if (amount < 0) {
            //LOG.info("get end signal, force all cache to tair.");
            cache.get(platform).force(basicOutputCollector);
            return;
        }
        //LOG.info(String.format("get a payment message, platform: %d, minute: %d, amount: %f, count=%d", platform, minute, amount, recvCount++));
        Double t = counter.get(platform).get(minute);
        double value = t != null ? t + amount : amount;
        counter.get(platform).put(minute, value);
        cache.get(platform).set(minute * 60, value, basicOutputCollector);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        counter = new ArrayList<MinuteMap>();
        counter.add(new MinuteMap());
        counter.add(new MinuteMap());
        cache = new ArrayList<TairLRUCache>();
        cache.add(new TairLRUCache(10, MiddlewareRaceConfig.prex_taobao + MiddlewareRaceConfig.team_code));
        cache.add(new TairLRUCache(10, MiddlewareRaceConfig.prex_tmall + MiddlewareRaceConfig.team_code));
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
