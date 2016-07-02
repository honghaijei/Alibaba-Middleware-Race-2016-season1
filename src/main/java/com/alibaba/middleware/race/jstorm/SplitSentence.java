package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


public class SplitSentence implements IRichBolt {
    private static Logger LOG = LoggerFactory.getLogger(SplitSentence.class);
    OutputCollector collector;
    TairOperatorImpl tairOperator;
    Map<Long, Double> counter = new HashMap<Long, Double>();
    @Override
    public void execute(Tuple tuple) {
        int platform = tuple.getInteger(0);
        long timestamp = tuple.getLong(1);
        double amount = tuple.getDouble(2);
        LOG.info(String.format("get a payment message, platform: %d, timestamp: %d, amount: %f", platform, timestamp, amount));
        long minute = timestamp / 1000 / 60;
        double value = counter.containsKey(minute) ? counter.get(minute) + amount : amount;
        counter.put(minute, value);
        String platformPrefix = platform == 0 ? RaceConfig.prex_taobao : RaceConfig.prex_tmall;
        tairOperator.write(platformPrefix + RaceConfig.team_code + minute, value);
        LOG.info(platformPrefix + RaceConfig.team_code + (minute * 60) + "\t" + value);

        collector.emit(new Values(platform, minute, value));
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
