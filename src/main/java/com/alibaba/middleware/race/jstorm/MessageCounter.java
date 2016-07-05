package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.LRUCache;
import com.alibaba.middleware.race.MinuteMap;
import com.alibaba.middleware.race.MiddlewareRaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Map;


public class MessageCounter implements IBasicBolt {
    public static Logger LOG = LoggerFactory.getLogger(MessageCounter.class);
    OutputCollector collector;
    TairOperatorImpl tairOperator;
    ArrayList<MinuteMap> counter;

    LRUCache cache;
    long recvCount = 0;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        int platform = tuple.getInteger(0);
        long minute = tuple.getLong(1);
        double amount = tuple.getDouble(2);
        if (amount < 0) {
            //LOG.info("get end signal, force all cache to tair.");
            cache.force();
            return;
        }
        //LOG.info(String.format("get a payment message, platform: %d, minute: %d, amount: %f, count=%d", platform, minute, amount, recvCount++));
        Double t = counter.get(platform).get(minute);
        double value = t != null ? t + amount : amount;
        counter.get(platform).put(minute, value);
        String platformPrefix = platform == 0 ? MiddlewareRaceConfig.prex_taobao : MiddlewareRaceConfig.prex_tmall;
        String key = platformPrefix + MiddlewareRaceConfig.team_code + (minute * 60);
        cache.set(key, value);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("platform", "minute", "amount"));
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext) {
        counter = new ArrayList<MinuteMap>();
        counter.add(new MinuteMap());
        counter.add(new MinuteMap());
        tairOperator = new TairOperatorImpl(MiddlewareRaceConfig.TairConfigServer, MiddlewareRaceConfig.TairSalveConfigServer,
                MiddlewareRaceConfig.TairGroup, MiddlewareRaceConfig.TairNamespace);
        cache = new LRUCache(10, tairOperator, this.LOG);
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
