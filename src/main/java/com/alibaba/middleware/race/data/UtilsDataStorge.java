package com.alibaba.middleware.race.data;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;
import org.omg.PortableInterceptor.INACTIVE;

import java.io.BufferedOutputStream;
import java.io.FileWriter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by qinjiawei on 16-7-18.
 */
public class UtilsDataStorge {


    //存储索引数据的磁盘路径
    public static String storeFolderOrder;
    public static String storeFolderOrderBybuyer;
    public static String storeFolderOrderByGood;


    //存储商品的打开文件
//    public static HashMap<String, BufferedOutputStream> goodFilesMap = new HashMap<String, BufferedOutputStream>();
//
//    存储订单的打开文件
//    public static HashMap<String, BufferedOutputStream> orderFilesMap = new HashMap<String, BufferedOutputStream>();
//
//    存储买家的打开文件
//    public static HashMap<String, BufferedOutputStream> buyerFilesMap = new HashMap<String, BufferedOutputStream>();

    //存储商品的打开文件
    public static ConcurrentHashMap<Integer, FileWriter> goodFileswriterMap = new ConcurrentHashMap<Integer, FileWriter>();

    //存储订单的打开文件
    public static ConcurrentHashMap<Integer, FileWriter> orderFileswriterMap = new ConcurrentHashMap<Integer, FileWriter>();

    //存储买家的打开文件
    public static ConcurrentHashMap<Integer, FileWriter> buyerFileswriterMap = new ConcurrentHashMap<Integer, FileWriter>();

    public static ConcurrentHashMap<Integer, FileWriter> orderbuyerFileswriterMap = new ConcurrentHashMap<Integer, FileWriter>();

    public static ConcurrentHashMap<Integer, FileWriter> ordergoodFileswriterMap = new ConcurrentHashMap<Integer, FileWriter>();

    //order files
    public static ArrayList<String> order_files = new ArrayList<String>();

//   存储队列
    public static ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> orderFileWriterqueue = new ConcurrentHashMap<String, ConcurrentLinkedQueue<String>>();

    //已经处理完的order文件总数量
    public static AtomicInteger countFile = new AtomicInteger(0);

    //order的文件总数量
    public static int countAllFiles;


    //order构建结束的标志
    public static boolean end = false;

    //order的已经处理完的条数
    public static AtomicInteger orderFileLines = new AtomicInteger(0);

}
