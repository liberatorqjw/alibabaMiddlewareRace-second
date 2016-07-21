package com.alibaba.middleware.race.utils;

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
    public static void main(String[] args)
    {
        String id = "123456";
        System.out.println(getGoodSuffix(id));
    }
}
