package com.alibaba.middleware.race;

import com.alibaba.middleware.race.Tair.TairOperatorImpl;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by hahong on 2016/7/4.
 */
public class Checker {
    public static Map<String, Double> getResult(String filename) {
        TairOperatorImpl tairOperator = new TairOperatorImpl(MiddlewareRaceConfig.TairConfigServer, MiddlewareRaceConfig.TairSalveConfigServer,
                MiddlewareRaceConfig.TairGroup, MiddlewareRaceConfig.TairNamespace);
        //tairOperator.write("1", 4.3);
        long start_time = tairOperator.getModifyTime("start_flag");
        Map<String, Double> kv = new TreeMap<String, Double>();
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
        return kv;
    }
    public static void main(String[] args) throws Exception {
        Map<String, Double> r = getResult("3509496lg7-worker-69042.log");
        Map<String, Double> ans = getResult("3509496lg7-worker-6904.log");
        Double acc = 0.0;
        for (String key : ans.keySet()) {
            if (!r.containsKey(key)) continue;
            if (Math.abs(r.get(key) - ans.get(key)) > 0.001) continue;
            acc += 1.0;
        }
        acc /= ans.size();
        System.out.println("acc is" + acc);
    }
}
