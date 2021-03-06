package com.alibaba.middleware.race;

import java.io.Serializable;

public class MiddlewareRaceConfig implements Serializable {
    public static boolean LOCAL = false;
    public static boolean LOCAL_CLUSTER = false;

    public static long start_minute = 23462316L;
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
    public static String TairConfigServer = LOCAL ? "10.60.45.77:5198" : "10.101.72.127:5198";
    public static String TairSalveConfigServer = LOCAL ? "10.60.45.77:5198" : "10.101.72.128:5198";
    public static String TairGroup = LOCAL ? "group_1" : "group_tianchi";
    public static Integer TairNamespace = LOCAL ? 0 : 31943;
}
