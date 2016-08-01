
package com.alibaba.middleware.race.utils;

import com.alibaba.middleware.race.OrderSystemImpl;

import java.util.*;

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
        return Math.abs(hash % 4096);
    }

    public static void  main(String[] args)
    {
        long startTime = System.currentTimeMillis();
        int hashcode = FNVHash1("al-aca4-93f110a11f1b");
        System.out.println(hashcode);
        System.out.println((System.currentTimeMillis()- startTime) + "ms" );

        Comparator<Long> comparator = new Comparator<Long>() {
            @Override
            public int compare(Long o1, Long o2) {
                if (o2 > o1)
                    return -1;
                else if (o2 < o1)
                    return 1;
                else
                    return  0;
            }
        };

        PriorityQueue<Long> queue = new PriorityQueue<Long>(11, comparator);
        queue.offer((long) 10);
        queue.offer((long) 6);
        queue.offer((long) 5);
        queue.offer((long) 4);
        queue.offer((long) 1);
        queue.offer((long) 2);
        queue.offer((long) 5);

        while (queue.size() > 0)
        {
            System.out.println(queue.poll());
        }



    }
}
