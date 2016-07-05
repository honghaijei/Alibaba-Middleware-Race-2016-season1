package com.alibaba.middleware.race;

import java.io.Serializable;

/**
 * Created by hahong on 2016/7/4.
 */
public class PaymentCacheEntry {
    public long minute;
    public double amount;
    public PaymentCacheEntry(long minute, double amount) {
        this.minute = minute;
        this.amount = amount;
    }
}
