package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
    public static boolean DEBUG = false;
    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";
    public static String team_code = "3509496lg7_";

    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "3509496lg7";
    public static String MetaConsumerGroup = "3509496lg7";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String MqNamesrvAddr = "10.60.45.78:9876";
    public static String TairConfigServer = DEBUG ? "10.60.45.77:5198" : "10.101.72.127:5198";
    public static String TairSalveConfigServer = DEBUG ? "10.60.45.77:5198" : "10.101.72.128:5198";
    public static String TairGroup = DEBUG ? "group_1" : "group_tianchi";
    public static Integer TairNamespace = DEBUG ? 1 : 31943;
}
