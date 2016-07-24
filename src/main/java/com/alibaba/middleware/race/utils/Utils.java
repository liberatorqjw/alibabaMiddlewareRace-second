package com.alibaba.middleware.race.utils;

import com.alibaba.middleware.race.OrderSystemImpl;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;

/**
 * Created by Liberatorqjw on 2016/7/20.
 */
public class Utils {

    /**
     * 返回商品与买家的后缀名
     * @return
     */
    public static String getGoodSuffix(String goodid)
    {
        String suffix = goodid.toLowerCase().substring(goodid.length()-2) + ".txt";
        return suffix;
    }

    /**
     * 返回订单的后缀名
     * @param orderid
     * @return
     */
    public static String getOrderSuffix(long orderid)
    {
        long suffix = orderid % 1000;
        return suffix + ".txt";

    }

    public static void PrintCache(LRUCache<String, Queue<OrderSystemImpl.Row>> cache) {

        for (Map.Entry<String, Queue<OrderSystemImpl.Row>> entry : cache.getAll())
        {
            System.out.println("key :" + entry.getKey() + "value :" + entry.getValue());
        }

    }
}
