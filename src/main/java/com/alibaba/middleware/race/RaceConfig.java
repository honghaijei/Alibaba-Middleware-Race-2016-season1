package com.alibaba.middleware.race;

import java.io.Serializable;

public class RaceConfig implements Serializable {
    public static boolean DEBUG = true;
    //��Щ��дtair key��ǰ׺
    public static String prex_tmall = "platformTmall_";
    public static String prex_taobao = "platformTaobao_";
    public static String prex_ratio = "ratio_";
    public static String team_code = "3509496lg7_";

    //��Щjstorm/rocketMq/tair �ļ�Ⱥ������Ϣ����Щ������Ϣ����ʽ�ύ����ǰ�ᱻ����
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
