package com.alibaba.middleware.race.jstorm;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.middleware.race.MiddlewareRaceConfig;
import com.alibaba.middleware.race.MinuteMap;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

/**
 * Created by hahong on 2016/7/6.
 */
public class TairWriter implements IBasicBolt {
    private static Logger LOG = LoggerFactory.getLogger(TairWriter.class);
    TairOperatorImpl tairOperator;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        String key = tuple.getString(0);
        double value = tuple.getDouble(1);
        tairOperator.write(key, value);
        if (MiddlewareRaceConfig.LOCAL) {
            LOG.info(String.format("Write to tair success, %s\t%f", key, value));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        tairOperator = new TairOperatorImpl(MiddlewareRaceConfig.TairConfigServer, MiddlewareRaceConfig.TairSalveConfigServer,
                MiddlewareRaceConfig.TairGroup, MiddlewareRaceConfig.TairNamespace);
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