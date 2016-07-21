package com.alibaba.middleware.race.data;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;

import java.io.BufferedOutputStream;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by qinjiawei on 16-7-18.
 */
public class UtilsDataStorge {


    //存储索引数据的磁盘路径
    public static String storeFolder;

    //存储商品的打开文件
    public static HashMap<String, BufferedOutputStream> goodFilesMap = new HashMap<String, BufferedOutputStream>();

    //存储订单的打开文件
    public static HashMap<String, BufferedOutputStream> orderFilesMap = new HashMap<String, BufferedOutputStream>();

    //存储买家的打开文件
    public static HashMap<String, BufferedOutputStream> buyerFilesMap = new HashMap<String, BufferedOutputStream>();

    //存储商品的打开文件
    public static ConcurrentHashMap<String, FileWriter> goodFileswriterMap = new ConcurrentHashMap<String, FileWriter>();

    //存储订单的打开文件
    public static ConcurrentHashMap<String, FileWriter> orderFileswriterMap = new ConcurrentHashMap<String, FileWriter>();

    //存储买家的打开文件
    public static ConcurrentHashMap<String, FileWriter> buyerFileswriterMap = new ConcurrentHashMap<String, FileWriter>();


    /**
     * 应对热点查询的缓存机制
     */
    public static ConcurrentHashMap<String, OrderSystem.Result> orderSearchCache = new ConcurrentHashMap<String, OrderSystem.Result>();

    public static ConcurrentHashMap<String, Queue> queryOrdersByBuyCache = new ConcurrentHashMap<String, Queue>();

    public static ConcurrentHashMap<String, Queue> queryOrdersBySalerCache = new ConcurrentHashMap<String, Queue>();

    public static ConcurrentHashMap<String, Queue> sumOrdersByGoodCache = new ConcurrentHashMap<String, Queue>();



}
