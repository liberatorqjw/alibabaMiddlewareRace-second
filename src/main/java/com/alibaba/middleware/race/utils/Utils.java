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
    public static long getOrderSuffix(long orderid)
    {
        long suffix = orderid % 2500;
        return suffix;

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

    /**
     * 改进的FNV1算法
     * @param data
     * @return
     */
    public static int FNVHash1(String data)
    {
        final int p = 16777619;
        int hash = (int)2166136261L;
        for(int i=0;i<data.length();i++)
            hash = (hash ^ data.charAt(i)) * p;
        hash += hash << 13;
        hash ^= hash >> 7;
        hash += hash << 3;
        hash ^= hash >> 17;
        hash += hash << 5;
        return Math.abs(hash % 2048);
    }

    public static void  main(String[] args)
    {
        long startTime = System.currentTimeMillis();
        int hashcode = FNVHash1("al-aca4-93f110a11f1b");
        System.out.println(hashcode);
        System.out.println(System.currentTimeMillis()- startTime);

    }
}
