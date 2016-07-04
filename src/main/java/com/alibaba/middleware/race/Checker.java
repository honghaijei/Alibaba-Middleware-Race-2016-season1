package com.alibaba.middleware.race;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * Created by hahong on 2016/7/4.
 */
public class Checker {
    public static void main(String[] args) throws Exception {
        TairOperatorImpl tairOperator = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer,
                RaceConfig.TairGroup, RaceConfig.TairNamespace);
        //tairOperator.write("1", 4.3);
        long start_time = tairOperator.getModifyTime("start_flag");
        Map<String, Double> kv = new HashMap<String, Double>();
        try (BufferedReader br = new BufferedReader(new FileReader("3509496lg7-worker-6904.log"))) {
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.contains("Write to tair success")) continue;
                String[] keys = line.split(",")[1].split("\t");
                String key = keys[0].trim();
                double value = Double.parseDouble(keys[1].trim());
                kv.put(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("Key value pair size: " + kv.size());
        double tot = 0.0, accuracy = 0.0;
        for (String k : kv.keySet()) {
            System.out.println("key: " + k);
            double value = (double)tairOperator.get(k);
            System.out.println(String.format("key: %s, expected value: %f, actual value: %f", k, kv.get(k), value));
            if (Math.abs(value - kv.get(k)) > 0.001) {
                System.out.print("error.");
            } else {
                accuracy += 1.0;
            }
            long cost = tairOperator.getModifyTime(k) - start_time;
            if (cost < 0) {
                accuracy -= 1.0;
            } else {
                tot += cost;
            }
        }
        tot /= kv.size();
        accuracy /= kv.size();
        System.out.println("done.");
        System.out.println(String.format("Accuracy: %f, Time: %f", accuracy, tot));
    }
}
