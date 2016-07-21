package com.alibaba.middleware.race;

import com.alibaba.middleware.race.data.OperationFiles;
import com.alibaba.middleware.race.data.UtilsDataStorge;
import com.alibaba.middleware.race.utils.Utils;
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 * 
 * @author wangxiang@alibaba-inc.com
 *
 */
public class OrderSystemImpl implements OrderSystem {

  static private String booleanTrueValue = "true";
  static private String booleanFalseValue = "false";

  final List<String> comparableKeysOrderingByOrderId;

  final List<String> comparableKeysOrderingByBuyerCreateTimeOrderId;
  final List<String> comparableKeysOrderingBySalerGoodOrderId;
  final List<String> comparableKeysOrderingByGood;
  final List<String> comparableKeysOrderingByGoodOrderId;
  final List<String> comparableKeysOrderingByBuyer;

  static private class KV implements Comparable<KV>, KeyValue {
    String key;
    String rawValue;

    boolean isComparableLong = false;
    long longValue;

    private KV(String key, String rawValue) {
      this.key = key;
      this.rawValue = rawValue;
      if (key.equals("createtime") || key.equals("orderid")) {
        isComparableLong = true;
        longValue = Long.parseLong(rawValue);
      }
    }

    public String key() {
      return key;
    }

    public String valueAsString() {
      return rawValue;
    }

    public long valueAsLong() throws TypeException {
      try {
        return Long.parseLong(rawValue);
      } catch (NumberFormatException e) {
        throw new TypeException();
      }
    }

    public double valueAsDouble() throws TypeException {
      try {
        return Double.parseDouble(rawValue);
      } catch (NumberFormatException e) {
        throw new TypeException();
      }
    }

    public boolean valueAsBoolean() throws TypeException {
      if (this.rawValue.equals(booleanTrueValue)) {
        return true;
      }
      if (this.rawValue.equals(booleanFalseValue)) {
        return false;
      }
      throw new TypeException();
    }

    public int compareTo(KV o) {
      if (!this.key().equals(o.key())) {
        throw new RuntimeException("Cannot compare from different key");
      }
      if (isComparableLong) {
        return Long.compare(this.longValue, o.longValue);
      }
      return this.rawValue.compareTo(o.rawValue);
    }

    @Override
    public String toString() {
      return "[" + this.key + "]:" + this.rawValue;
    }
  }

  @SuppressWarnings("serial")
  static private class Row extends HashMap<String, KV> {
    Row() {
      super();
    }

    Row(KV kv) {
      super();
      this.put(kv.key(), kv);
    }

    KV getKV(String key) {
      KV kv = this.get(key);
      if (kv == null) {
        throw new RuntimeException(key + " is not exist");
      }
      return kv;
    }

    Row putKV(String key, String value) {
      KV kv = new KV(key, value);
      this.put(kv.key(), kv);
      return this;
    }

    Row putKV(String key, long value) {
      KV kv = new KV(key, Long.toString(value));
      this.put(kv.key(), kv);
      return this;
    }
  }

  private static class ResultImpl implements Result {
    private long orderid;
    private Row kvMap;

    private ResultImpl(long orderid, Row kv) {
      this.orderid = orderid;
      this.kvMap = kv;
    }

    static private ResultImpl createResultRow(Row orderData, Row buyerData,
        Row goodData, Set<String> queryingKeys) {
      if (orderData == null || buyerData == null || goodData == null) {
        throw new RuntimeException("Bad data!");
      }
      Row allkv = new Row();
      long orderid;
      try {
        orderid = orderData.get("orderid").valueAsLong();
      } catch (TypeException e) {
        throw new RuntimeException("Bad data!");
      }

      for (KV kv : orderData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      for (KV kv : buyerData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      for (KV kv : goodData.values()) {
        if (queryingKeys == null || queryingKeys.contains(kv.key)) {
          allkv.put(kv.key(), kv);
        }
      }
      return new ResultImpl(orderid, allkv);
    }

    public KeyValue get(String key) {
      return this.kvMap.get(key);
    }

    public KeyValue[] getAll() {
      return kvMap.values().toArray(new KeyValue[0]);
    }

    public long orderId() {
      return orderid;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("orderid: " + orderid + " {");
      if (kvMap != null && !kvMap.isEmpty()) {
        for (KV kv : kvMap.values()) {
          sb.append(kv.toString());
          sb.append(",\n");
        }
      }
      sb.append('}');
      return sb.toString();
    }
  }

  static private class ComparableKeys implements Comparable<ComparableKeys> {
    List<String> orderingKeys;
    Row row;

    private ComparableKeys(List<String> orderingKeys, Row row) {
      if (orderingKeys == null || orderingKeys.size() == 0) {
        throw new RuntimeException("Bad ordering keys, there is a bug maybe");
      }
      this.orderingKeys = orderingKeys;
      this.row = row;
    }

    public int compareTo(ComparableKeys o) {
      if (this.orderingKeys.size() != o.orderingKeys.size()) {
        throw new RuntimeException("Bad ordering keys, there is a bug maybe");
      }
      for (String key : orderingKeys) {
        KV a = this.row.get(key);
        KV b = o.row.get(key);
        if (a == null || b == null) {
          throw new RuntimeException("Bad input data: " + key);
        }
        int ret = a.compareTo(b);
        if (ret != 0) {
          return ret;
        }
      }
      return 0;
    }
  }

  /**
   * 构建索引Treemap
   */
  private  class DataIndexFileHandler {

    public TreeMap<ComparableKeys, Row> orderDataSorted = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();


    public void handleRow(Row row, List<String> comparekeys) {
      orderDataSorted.put(new ComparableKeys(
              comparekeys, row), row);
    }

    /**
     * 全建成树的形式
     * @param file
     * @param comparekeys
     * @return
     * @throws IOException
     */
    public TreeMap<ComparableKeys, Row> handle(String file, List<String> comparekeys) throws IOException {

      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolder + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          handleRow(kvMap, comparekeys);

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return this.orderDataSorted;
    }

    /**
     * 查询order的treemap
     * @param file
     * @param comparekeys
     * @param orderid
     * @return
     * @throws IOException
     * @throws TypeException
     */
    public TreeMap<ComparableKeys, Row> handleOrder(String file, List<String> comparekeys, long orderid) throws IOException, TypeException {

      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolder + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (Math.abs(kvMap.get("orderid").valueAsLong() - orderid) < 0.0001)
              handleRow(kvMap, comparekeys);

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return this.orderDataSorted;
    }

    public Row  handleOrderLine(String file, List<String> comparekeys, long orderid) throws FileNotFoundException, Exception {
      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolder + file);
      System.out.println(UtilsDataStorge.storeFolder + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的

          if (Math.abs(kvMap.get("orderid").valueAsLong() - orderid) < 0.0001)
            return kvMap;

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return null;
    }

    /**
     * 查询买家的
     * @param file
     * @param comparekeys
     * @param buyerid
     * @return
     * @throws IOException
     */
    public TreeMap<ComparableKeys, Row> handleBuyer(String file, List<String> comparekeys, String buyerid) throws IOException {

      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolder + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("buyerid").valueAsString().equals(buyerid))
                handleRow(kvMap, comparekeys);

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return this.orderDataSorted;
    }

    public Row handleBuyerLine(String file, List<String> comparekeys, String buyerid) throws IOException {


      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolder + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("buyerid").valueAsString().equals(buyerid))
            return kvMap;

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return null;
    }

    public Queue<Row> handleBuyerRowQue(String file, long startTime, long endTime, String buyerid) throws IOException {

      //从大到小
      Comparator<Row> OrderIsdn =  new Comparator<Row>(){
        public int compare(Row o1, Row o2) {
          // TODO Auto-generated method stub
          long numbera = 0;
          long numberb = 0;
          try {
            numbera = o1.getKV("createtime").valueAsLong();

            numberb = o2.getKV("createtime").valueAsLong();
        } catch (TypeException e) {
          e.printStackTrace();
        }if(numberb > numbera)
          {
            return 1;
          }
          else if(numberb < numbera)
          {
            return -1;
          }
          else
          {
            return 0;
          }

        }


      };
      PriorityQueue<Row> buyerQue = new PriorityQueue<Row>(11, OrderIsdn);

      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolder + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("buyerid").valueAsString().equals(buyerid) && kvMap.getKV("createtime").valueAsLong() >=startTime && kvMap.getKV("createtime").valueAsLong() <= endTime)
          {
            buyerQue.add(kvMap);
//            System.out.println("add to the queue " + kvMap.getKV("orderid").valueAsLong());
          }

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      }catch (Exception e)
      {
        e.printStackTrace();
      }
      finally {
        bfr.close();
      }

      return buyerQue;
    }
    /**
     * 查询商品
     * @param file
     * @param comparekeys
     * @param goodid
     * @return
     * @throws IOException
     */
    public TreeMap<ComparableKeys, Row> handleGood(String file, List<String> comparekeys, String goodid) throws IOException {

      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolder + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("goodid").valueAsString().equals(goodid))
               handleRow(kvMap, comparekeys);

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return this.orderDataSorted;
    }
    public Row handleGoodLine(String file, List<String> comparekeys, String goodid) throws IOException {

      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolder + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("goodid").valueAsString().equals(goodid))
            return kvMap;

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return null;
    }

    public Queue<Row> handleGoodRowQueue(String file, List<String> comparekeys, String goodid) throws IOException {

      Comparator<Row> OrderIsdn =  new Comparator<Row>(){
        public int compare(Row o1, Row o2) {
          // TODO Auto-generated method stub
          long numbera = 0;
          long numberb = 0;
          try {
            numbera = o1.getKV("orderid").valueAsLong();

            numberb = o2.getKV("orderid").valueAsLong();
          } catch (TypeException e) {
            e.printStackTrace();
          }if(numberb > numbera)
          {
            return -1;
          }
          else if(numberb < numbera)
          {
            return 1;
          }
          else
          {
            return 0;
          }

        }


      };
      PriorityQueue<Row> goodQue = new PriorityQueue<Row>(11, OrderIsdn);
      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolder + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("goodid").valueAsString().equals(goodid))
            goodQue.add(kvMap);

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return goodQue;
    }
  }

  /**
   * 多线程构建索引文件
   */
  private class ReadAllFilesThread implements Runnable {

    private Collection<String> files;
    private ConcurrentHashMap<String, FileWriter> outputWriters;
    private int flag;
    private CountDownLatch latch;


    public ReadAllFilesThread(Collection<String> files, ConcurrentHashMap<String, FileWriter> outputWriters, int flag, CountDownLatch latch) {
      this.files = files;
      this.outputWriters = outputWriters;
      this.flag = flag;
      this.latch = latch;
    }


    @Override
    public void run() {
      try {
        for (String file : files) {
          BufferedReader bfr = createReader(file);
          try {
            int linecount = 0;
            String line = bfr.readLine();
            while (line != null) {

              Row row = createKVMapFromLineToSome(line, flag);// 返回的是一条数据的map
              String address = file.trim() + "," + String.valueOf(linecount);
              //goodfiles
              if (flag == 0)
              {
                try {
                  //按照goodid的最后一位形成索引文件的命名
                  String goodid = row.getKV("goodid").valueAsString();
                  String suffix = Utils.getGoodSuffix(goodid);
                  String content = "goodid:" + row.getKV("goodid").valueAsString()+ "\t" + "address:"+ address.trim() + "\n";


                  outputWriters.get(OrderSystemImpl.goodIndexFile + suffix).write(line + "\n");

                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
              //order
              else if (flag ==1)
              {
                try {
                  String suffixByorderid = Utils.getOrderSuffix(row.getKV("orderid").valueAsLong());
                  String suffixBygoodid = Utils.getGoodSuffix(row.getKV("goodid").valueAsString());
                  //String suffixBysalergood = Utils.getSalerGoodSuffix(row.getKV("salerid").valueAsString(), row.getKV("goodid").valueAsString());
                  String suffixBybuyerid = Utils.getGoodSuffix(row.getKV("buyerid").valueAsString());
                  String contentOne ="orderid:" + row.getKV("orderid").valueAsString()+ "\t" + "address:"+ address.trim() + "\n";
                  String contentTwo ="buyerid:" + row.getKV("buyerid").valueAsString() + "\t" + "createtime:"+ row.getKV("createtime").valueAsString() +"\t"+ "orderid:" + row.getKV("orderid").valueAsString()+ "\t" + "address:"+ address.trim() + "\n";
                  String contentThree = "goodid:" + row.getKV("goodid").valueAsString() + "\t" + "orderid:" + row.getKV("orderid").valueAsString() + "\t" + "address:" + address.trim() + "\n";

                  outputWriters.get(OrderSystemImpl.orderIdexFile + suffixByorderid).write(line + "\n");
                  outputWriters.get(OrderSystemImpl.orderBuyerCreateTimeOrderIdFile + suffixBybuyerid).write(line+ "\n");
                  outputWriters.get(OrderSystemImpl.orderGoodOrderIdFile + suffixBygoodid).write(line+ "\n");

                } catch (Exception e) {
                  e.printStackTrace();
                }
              }
              //buyerfiles
              else
              {
                try {
                  String buyerid =  row.getKV("buyerid").valueAsString();
                  String suffix = Utils.getGoodSuffix(buyerid);
                  String content = "buyerid:" + row.getKV("buyerid").valueAsString() + "\t"  + "address:" + address.trim() + "\n";

                  outputWriters.get(OrderSystemImpl.buyerIndexFile + suffix).write(line+ "\n");
                } catch (Exception e) {
                  e.printStackTrace();
                }
              }

              //读取下一行
              line = bfr.readLine();
              linecount += 1;
            }
          } finally {
            bfr.close();
          }
        }
        OperationFiles.closeFileWriter(flag);
        latch.countDown();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }
//  TreeMap<ComparableKeys, Row> orderDataSortedByOrder = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//  TreeMap<ComparableKeys, Row> orderDataSortedByBuyerCreateTime = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//  TreeMap<ComparableKeys, Row> orderDataSortedBySalerGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//  TreeMap<ComparableKeys, Row> orderDataSortedByGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//  TreeMap<ComparableKeys, Row> buyerDataStoredByBuyer = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//  TreeMap<ComparableKeys, Row> goodDataStoredByGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
  public static String goodIndexFile= "goodIndexFile";
  public static String orderIdexFile = "orderIndexFile";
  public static String buyerIndexFile = "buyerIndexFile";
  public static String orderBuyerCreateTimeOrderIdFile = "orderIndxByBuyerCreateTimeFile";
  public static String orderSalerGoodOrderIdFile = "orderIndexBySalerGoodFile";
  public static String orderGoodOrderIdFile = "orderIndexByGoodFile";
  //存order缓存
  public static ConcurrentHashMap<String, Row> orderSearchCache = new ConcurrentHashMap<String, Row>();

  Lock orderlock = new ReentrantLock();
  Lock orderByBuyerlock = new ReentrantLock();
  Lock orderBySalerlock = new ReentrantLock();
  Lock orderSumlock = new ReentrantLock();

  public OrderSystemImpl() {
    comparableKeysOrderingByOrderId = new ArrayList<String>();
    comparableKeysOrderingByBuyerCreateTimeOrderId = new ArrayList<String>();
    comparableKeysOrderingBySalerGoodOrderId = new ArrayList<String>();
    comparableKeysOrderingByGood = new ArrayList<String>();
    comparableKeysOrderingByGoodOrderId = new ArrayList<String>();
    comparableKeysOrderingByBuyer = new ArrayList<String>();

    comparableKeysOrderingByOrderId.add("orderid");

    comparableKeysOrderingByBuyerCreateTimeOrderId.add("buyerid");
    comparableKeysOrderingByBuyerCreateTimeOrderId.add("createtime");
    comparableKeysOrderingByBuyerCreateTimeOrderId.add("orderid");

//    comparableKeysOrderingBySalerGoodOrderId.add("salerid");
    comparableKeysOrderingBySalerGoodOrderId.add("goodid");
    comparableKeysOrderingBySalerGoodOrderId.add("orderid");

    comparableKeysOrderingByGoodOrderId.add("goodid");
    comparableKeysOrderingByGoodOrderId.add("orderid");

    comparableKeysOrderingByGood.add("goodid");

    comparableKeysOrderingByBuyer.add("buyerid");
  }

  public static void main(String[] args) throws IOException,
      InterruptedException {

    // init order system
    List<String> orderFiles = new ArrayList<String>();
    List<String> buyerFiles = new ArrayList<String>();
    ;
    List<String> goodFiles = new ArrayList<String>();
    List<String> storeFolders = new ArrayList<String>();

    String dirpath = "/media/qinjiawei/000C8D1A0003D585/prerun_data/";
    orderFiles.add(dirpath + "order.0.0");
    orderFiles.add(dirpath + "order.1.1");
    orderFiles.add(dirpath + "order.0.3");
    orderFiles.add(dirpath + "order.2.2");

    buyerFiles.add(dirpath + "buyer.0.0");
    buyerFiles.add(dirpath + "buyer.1.1");

    goodFiles.add(dirpath + "good.0.0");
    goodFiles.add(dirpath + "good.1.1");
    goodFiles.add(dirpath + "good.2.2");

    storeFolders.add("/media/qinjiawei/000C8D1A0003D585/prerun_data/indexAll/");
    UtilsDataStorge.storeFolder = "/media/qinjiawei/000C8D1A0003D585/prerun_data/indexAll/";
    OrderSystem os = new OrderSystemImpl();

    long start = System.currentTimeMillis();

//    os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

    long end1 = System.currentTimeMillis();
    long end =0;
    System.out.println( "construct cost of time :" + (end1 - start) + "ms");

    // 用例

    /*
    start = System.currentTimeMillis();
    long orderid = 589555952;
    System.out.println("\n查询订单号为" + orderid + "的订单");
    System.out.println(os.queryOrder(orderid, null));
    end= System.currentTimeMillis();


    System.out.println( "construct cost of time :" + (end - start) + "ms");
    */

   /*
    String buyerid = "ap-8a57-454ce6fcfb19";
    long startTime = 1470344717;
    long endTime = 1483767105;
    System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
    Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
    while (it.hasNext()) {
      System.out.println(it.next());
    }
    //long end = System.currentTimeMillis();
    System.out.println( "construct cost of time :" + (end - start) + "ms");

   */

    String goodid = "gd-8870-0537e54f51ca";
    String salerid = "ay-a576-84cc0ef460d3";
    System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
    List<String> querykeys  = new ArrayList<String>();
    querykeys.add("goodid");
    Iterator it = os.queryOrdersBySaler(salerid, goodid, querykeys);
    int count =0;
    while (it.hasNext()) {
      System.out.println(it.next());
      count++;
    }
    System.out.println(count);


    /*
    String goodid = "dd-8834-c6874b116c42";
    String attr = "amount";
    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
    System.out.println(os.sumOrdersByGood(goodid, attr));

    attr = "done";
    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
    KeyValue sum = os.sumOrdersByGood(goodid, attr);
    if (sum == null) {
      System.out.println("由于该字段是布尔类型，返回值是null");
    }

    attr = "foo";
    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
    sum = os.sumOrdersByGood(goodid, attr);
    if (sum == null) {
      System.out.println("由于该字段不存在，返回值是null");
    }
*/
  }

  private BufferedReader createReader(String file) throws FileNotFoundException {
    return new BufferedReader(new FileReader(file));
  }

  private Row createKVMapFromLine(String line) {
    String[] kvs = line.split("\t");
    Row kvMap = new Row();
    for (String rawkv : kvs) {
      int p = rawkv.indexOf(':');
      String key = rawkv.substring(0, p);
      String value = rawkv.substring(p + 1);
      if (key.length() == 0 || value.length() == 0) {
        throw new RuntimeException("Bad data:" + line);
      }
      KV kv = new KV(key, value);
      kvMap.put(kv.key(), kv);
    }
    return kvMap;
  }
  /**
   * 只是获取少量的字段
   * @param line
   * @return
   */
  private Row createKVMapFromLineToSome(String line, int flag) {
    Row kvMap = new Row();
    //good 只是需要gooid
    if (flag ==0)
    {
      String value = line.split("goodid:")[1].split("\t")[0];
      //键值对的数据进行封装
      KV kv = new KV("goodid", value);

      //封装的hashmap
      kvMap.put(kv.key(), kv);
    }

    //buyer 只是需要buyerid
    else if(flag ==2)
    {
      String value = line.split("buyerid:")[1].split("\t")[0];
      //键值对的数据进行封装
      KV kv = new KV("buyerid", value);

      //封装的hashmap
      kvMap.put(kv.key(), kv);
    }

    //order需要其中几个字段
    else {
      //buyerid
      String value = line.split("buyerid:")[1].split("\t")[0];
      KV kv = new KV("buyerid", value);
      kvMap.put(kv.key(), kv);

      //orderid
      value = line.split("orderid:")[1].split("\t")[0];
      kv = new KV("orderid", value);
      kvMap.put(kv.key(), kv);

      //createtime
      value = line.split("createtime:")[1].split("\t")[0];
      kv = new KV("createtime", value);
      kvMap.put(kv.key(), kv);

      //goodid
      value = line.split("goodid:")[1].split("\t")[0];
      kv = new KV("goodid", value);
      kvMap.put(kv.key(), kv);

    }

    return kvMap;
  }
  private abstract class DataFileHandler {
    abstract void handleRow(Row row);
    abstract void handleRow(Row row, String address);

    void handle(Collection<String> files, int flag) throws IOException {
      for (String file : files) {
        BufferedReader bfr = createReader(file);
        try {
          int linecount =0;
          String line = bfr.readLine();
          while (line != null) {

            Row kvMap = createKVMapFromLineToSome(line, flag);// 返回的是一条数据的map
            handleRow(kvMap, file.trim() + "," + String.valueOf(linecount));

            //读取下一行
            line = bfr.readLine();
            linecount +=1;
          }
        } finally {
          bfr.close();
        }
      }
    }
  }

  public void construct(Collection<String> orderFiles,
      Collection<String> buyerFiles, Collection<String> goodFiles,
      Collection<String> storeFolders) throws IOException, InterruptedException {

    //选择存储索引的磁盘, 选择第一个
    for (String store : storeFolders)
    {
      UtilsDataStorge.storeFolder = store;
      break;
    }
    //创建文件流
    OperationFiles.CreateFileWriter();

    CountDownLatch latch = new CountDownLatch(3);

    new Thread(new ReadAllFilesThread(goodFiles, UtilsDataStorge.goodFileswriterMap, 0, latch)).start();
    new Thread(new ReadAllFilesThread(orderFiles, UtilsDataStorge.orderFileswriterMap, 1, latch)).start();
    new Thread(new ReadAllFilesThread(buyerFiles, UtilsDataStorge.buyerFileswriterMap, 2, latch)).start();

    latch.await();

  }

  public Result queryOrder(long orderId, Collection<String> keys) {


    Row orderData = null;
    //缓存的key
    String cacheKey = String.valueOf(orderId);
    if (keys !=null) {
      for (String key : keys)
        cacheKey += "_" + key;
    }
    if (OrderSystemImpl.orderSearchCache.get(cacheKey) == null) {


      Row query = new Row();
      query.putKV("orderid", orderId);
      System.out.println("*****query order " + orderId);


      //索引文件的后缀名称
      String suffix = Utils.getOrderSuffix(orderId);
      try {
        DataIndexFileHandler DIF = new DataIndexFileHandler();
        orderData = DIF.handleOrderLine(OrderSystemImpl.orderIdexFile + suffix, comparableKeysOrderingByOrderId, orderId);
      } catch (IOException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }

//      orderlock.lock();
//      OrderSystemImpl.orderSearchCache.put(cacheKey, orderData);
//      orderlock.unlock();

      if (orderData == null)
        return null;

    }

    else
    {
      orderData = OrderSystemImpl.orderSearchCache.get(cacheKey);
      if (orderData == null)
        return null;
    }
    Result result = createResultFromOrderData(orderData, createQueryKeys(keys));

    return result;
  }


  private ResultImpl createResultFromOrderData(Row orderData,
      Collection<String> keys) {


    //从获取到的索引来获取真实的数据
    Row buyerData = null;
    Row goodData = null;

      String suffix = Utils.getGoodSuffix(orderData.getKV("buyerid").valueAsString());

      //索引map
      try {
        DataIndexFileHandler DIF = new  DataIndexFileHandler();
      buyerData =  DIF.handleBuyerLine(OrderSystemImpl.buyerIndexFile + suffix, comparableKeysOrderingByBuyer, orderData.getKV("buyerid").valueAsString());


      } catch (IOException e) {
        e.printStackTrace();
      }


      String suffixx = Utils.getGoodSuffix(orderData.getKV("goodid").valueAsString());
      //索引map
      try {

        DataIndexFileHandler DIF = new DataIndexFileHandler();
       goodData = DIF.handleGoodLine(OrderSystemImpl.goodIndexFile + suffixx, comparableKeysOrderingByGood, orderData.getKV("goodid").valueAsString());

      } catch (IOException e) {
        e.printStackTrace();
      }

    return ResultImpl.createResultRow(orderData, buyerData, goodData,
            createQueryKeys(keys));
  }

  private HashSet<String> createQueryKeys(Collection<String> keys) {
    if (keys == null) {
      return null;
    }
    return new HashSet<String>(keys);
  }

  public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
      String buyerid) {

    Queue<Row> buyerQUeue = null;
    //缓存的key
    String cacheKey = String.valueOf(startTime) +"_"+String.valueOf(endTime)+"_" + buyerid;
    if (UtilsDataStorge.queryOrdersByBuyCache.get(cacheKey) == null) {

      System.out.println("*****query buyerid by time " + buyerid);
      System.out.println("***** start time " + startTime + " endtime " + endTime);


      Row queryStart = new Row();
      queryStart.putKV("buyerid", buyerid);
      queryStart.putKV("createtime", startTime);
      queryStart.putKV("orderid", Long.MIN_VALUE);

      Row queryEnd = new Row();
      queryEnd.putKV("buyerid", buyerid);
      queryEnd.putKV("createtime", endTime - 1); // exclusive end
      queryEnd.putKV("orderid", Long.MAX_VALUE);

      String suffixIndexFile = Utils.getGoodSuffix(buyerid);


      try {
        DataIndexFileHandler DIF = new DataIndexFileHandler();
        buyerQUeue = DIF.handleBuyerRowQue(OrderSystemImpl.orderBuyerCreateTimeOrderIdFile + suffixIndexFile, startTime, endTime - 1, buyerid);

      } catch (IOException e) {
        e.printStackTrace();
      }

//      orderByBuyerlock.lock();
//      UtilsDataStorge.queryOrdersByBuyCache.put(cacheKey, buyerQUeue);
//      orderByBuyerlock.unlock();
    }
    else
    {
      buyerQUeue = UtilsDataStorge.queryOrdersByBuyCache.get(cacheKey);

    }
    final Queue<Row> orderIndexs =buyerQUeue;

    Iterator<OrderSystem.Result> result =  new Iterator<OrderSystem.Result>() {

      Queue<Row> o =orderIndexs;

      public boolean hasNext() {
        return o != null && o.size() > 0;
      }

      public Result next() {
        if (!hasNext()) {
          return null;
        }
        Row orderData = o.poll();

        return createResultFromOrderData(orderData, null);
      }

      public void remove() {

      }
    };

    return  result;
  }

  public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
      Collection<String> keys) {

    Queue<Row> orderDataSortedBySalerQueue = null;
    final Collection<String> queryKeys = keys;

    String cacheKey = salerid + "_"+ goodid;
    if (keys !=null) {
      for (String key : keys) {
        cacheKey += "_" + key;
      }
    }
    if (UtilsDataStorge.queryOrdersBySalerCache.get(cacheKey) == null) {
      System.out.println("*****query saler by id " + salerid);
      System.out.println("*****query goodid by id " + goodid);



      Row queryStart = new Row();
      queryStart.putKV("goodid", goodid);
      queryStart.putKV("orderid", Long.MIN_VALUE);

      Row queryEnd = new Row();
      queryEnd.putKV("goodid", goodid);
      queryEnd.putKV("orderid", Long.MAX_VALUE);


      String suffixIndexFile = Utils.getGoodSuffix(goodid);



      try {
        DataIndexFileHandler DIF = new DataIndexFileHandler();
        orderDataSortedBySalerQueue = DIF.handleGoodRowQueue(OrderSystemImpl.orderGoodOrderIdFile + suffixIndexFile, comparableKeysOrderingBySalerGoodOrderId, goodid);

      } catch (IOException e) {
        e.printStackTrace();
      }

//      orderBySalerlock.lock();
//      UtilsDataStorge.queryOrdersBySalerCache.put(cacheKey, orderDataSortedBySalerQueue);
//      orderBySalerlock.unlock();

    }

    else {
      orderDataSortedBySalerQueue = UtilsDataStorge.queryOrdersBySalerCache.get(cacheKey);
    }

    final  Queue<Row> orderIndexsBySaler = orderDataSortedBySalerQueue;

    Iterator<OrderSystem.Result> result =  new Iterator<OrderSystem.Result>() {

    Queue<Row> o = orderIndexsBySaler;

      public boolean hasNext() {
        return o != null && o.size() > 0;
      }

      public Result next() {
        if (!hasNext()) {
          return null;
        }

        Row orderData = o.poll();


        return createResultFromOrderData(orderData, createQueryKeys(queryKeys));
      }

      public void remove() {
        // ignore
      }
    };
//    UtilsDataStorge.queryOrdersBySalerCache.put(cacheKey, result);
    return result;
  }

  public KeyValue sumOrdersByGood(String goodid, String key) {

    Queue<Row> orderDataSortedByGoodQueue = null;

    String cacheKey = goodid;
    if (key != null) {
      cacheKey += "_" + key;
    }
//
    if (UtilsDataStorge.sumOrdersByGoodCache.get(cacheKey) == null) {

      System.out.println("***** query the sum of some keys in goodid : " + goodid + "key :" + key);

      Row queryStart = new Row();
      queryStart.putKV("goodid", goodid);
      queryStart.putKV("orderid", Long.MIN_VALUE);
      Row queryEnd = new Row();
      queryEnd.putKV("goodid", goodid);
      queryEnd.putKV("orderid", Long.MAX_VALUE);


      String suffixIndexFile = Utils.getGoodSuffix(goodid);


      try {
        DataIndexFileHandler DIF = new DataIndexFileHandler();
        orderDataSortedByGoodQueue = DIF.handleGoodRowQueue(OrderSystemImpl.orderGoodOrderIdFile + suffixIndexFile, comparableKeysOrderingByGoodOrderId, goodid);

      } catch (IOException e) {
        e.printStackTrace();
      }

//      orderSumlock.lock();
//      UtilsDataStorge.sumOrdersByGoodCache.put(cacheKey, orderDataSortedByGoodQueue);
//      orderSumlock.unlock();

      if (orderDataSortedByGoodQueue == null || orderDataSortedByGoodQueue.isEmpty()) {
        return null;
      }


    }
    else
    {
      orderDataSortedByGoodQueue = UtilsDataStorge.sumOrdersByGoodCache.get(cacheKey);
      if (orderDataSortedByGoodQueue == null || orderDataSortedByGoodQueue.isEmpty()) {
        return null;
      }

    }
    HashSet<String> queryingKeys = new HashSet<String>();
    queryingKeys.add(key);
    List<ResultImpl> allData = new ArrayList<ResultImpl>(orderDataSortedByGoodQueue.size());

    while (orderDataSortedByGoodQueue.size() > 0){

      allData.add(createResultFromOrderData(orderDataSortedByGoodQueue.poll(), queryingKeys));
    }

    // accumulate as Long
    try {
      boolean hasValidData = false;
      long sum = 0;
      for (ResultImpl r : allData) {
        KeyValue kv = r.get(key);
        if (kv != null) {
          sum += kv.valueAsLong();
          hasValidData = true;
        }
      }
      if (hasValidData) {

        KV result = new KV(key, Long.toString(sum));
//        UtilsDataStorge.sumOrdersByGoodCache.put(cacheKey, result);
        return result;
      }
    } catch (TypeException e) {
    }

    // accumulate as double
    try {
      boolean hasValidData = false;
      double sum = 0;
      for (ResultImpl r : allData) {
        KeyValue kv = r.get(key);
        if (kv != null) {
          sum += kv.valueAsDouble();
          hasValidData = true;
        }
      }
      if (hasValidData) {
        KV result = new KV(key, Double.toString(sum));
//        UtilsDataStorge.sumOrdersByGoodCache.put(cacheKey, result);
        return result;

      }
    } catch (TypeException e) {
    }

    return null;
  }

  /**
   * 获取指定的一行数据 通过正常的buffer获取
   * @param filePath
   * @param linecount
   * @return
   * @throws Exception
   */
  public static Row getFileLineRow(String filePath, int linecount) throws Exception{

    LineNumberReader lineNumberReader = new LineNumberReader(new FileReader(filePath));

    int curlineNum = 0;

    String line = null;
    try{
      while (curlineNum < linecount) {

        lineNumberReader.readLine();
        curlineNum++;
      }
      line = lineNumberReader.readLine();

    } catch (FileNotFoundException ex) {
      ex.printStackTrace();
    } catch (IOException ex) {
      ex.printStackTrace();
    } finally {
      //关闭lineNumberReader
      try {
        if (lineNumberReader != null) {
          lineNumberReader.close();
        }
      } catch (IOException ex) {
        ex.printStackTrace();
      }
    }
    return   createKVMapFromLineStatic(line);

  }

  private static Row createKVMapFromLineStatic(String line) {
    String[] kvs = line.split("\t");
    Row kvMap = new Row();
    for (String rawkv : kvs) {
      int p = rawkv.indexOf(':');
      String key = rawkv.substring(0, p);
      String value = rawkv.substring(p + 1);
      if (key.length() == 0 || value.length() == 0) {
        throw new RuntimeException("Bad data:" + line);
      }
      KV kv = new KV(key, value);
      kvMap.put(kv.key(), kv);
    }
    return kvMap;
  }
}
