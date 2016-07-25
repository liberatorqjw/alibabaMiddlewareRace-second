package com.alibaba.middleware.race.utils;

import com.alibaba.middleware.race.OrderSystemImpl;

import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
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

    public static void PrintCache(LRUCache<String, Object> cache, String query) {

        System.out.println("query: " + query);
        for (Map.Entry<String, Object> entry : cache.getAll())
        {
            System.out.println("*******key :" + entry.getKey() + "**value :" + ((PriorityQueue<OrderSystemImpl.Row>) entry.getValue()).size());
        }

    }
    public static void PrintCacheTest(LRUCache<String, Object> cache) {


        for (Map.Entry<String, Object> entry : cache.getAll())
        {
            PriorityQueue value = (PriorityQueue) entry.getValue();
            System.out.println("*******key :" + entry.getKey() + "**value :" + value.size());
            System.out.println("#####################################");
        }

    }
}
