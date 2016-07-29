package com.alibaba.middleware.race;

import com.alibaba.middleware.race.data.OperationFiles;
import com.alibaba.middleware.race.data.UtilsDataStorge;
import com.alibaba.middleware.race.utils.BufferedRandomAccessFile;
import com.alibaba.middleware.race.utils.LRUCache;
import com.alibaba.middleware.race.utils.Utils;
import com.alibaba.middleware.race.utils.WriteIntoFileThread;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
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
  static public class Row extends HashMap<String, KV> {
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
      BufferedReader bfr = createReader( file);
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
      BufferedReader bfr = createReader( file);
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
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolderOrder +"order/" + file);
//      System.out.println(UtilsDataStorge.storeFolderOrder +"order/" + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        /*
        while (line != null) {
//          System.out.println("本行数据 ：" + line);
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的

          if (Math.abs(kvMap.get("orderid").valueAsLong() - orderid) < 0.0001)
          {
            //此时读取到的数索引文件
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            return autalData;
          }

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
        */
        while (line != null) {
//          System.out.println("本行数据 ：" + line);
          // 返回的是一条数据的map
          //这个函数是由子类实现的

          if (line.indexOf("orderid:" + String.valueOf(orderid)) != -1)
          {
            Row kvMap = createKVMapFromLine(line);
            //此时读取到的数索引文件
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            return autalData;
          }

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
      BufferedReader bfr = createReader( file);
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
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolderOrder + "buyer/"+file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        /*
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("buyerid").valueAsString().equals(buyerid)) {
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            return autalData;
          }
          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
        */
        while (line != null) {

          //这个函数是由子类实现的
          if (line.indexOf("buyerid:"+buyerid) != -1) {
            Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            return autalData;
          }
          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return null;
    }

    public PriorityQueue<Row> handleBuyerRowQue(String file, long startTime, long endTime, String buyerid) throws IOException {

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
      List<Row> buyerList = new ArrayList<Row>();

      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolderOrderBybuyer+"order/" + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
//          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (line.indexOf("buyerid:"+ buyerid) != -1)
          {
            Row kvMap = createKVMapFromLine(line);
            if (kvMap.getKV("createtime").valueAsLong() >=startTime && kvMap.getKV("createtime").valueAsLong() <= endTime) {
              String filename = kvMap.getKV("address").valueAsString().split(",")[0];
              long offset = Long.valueOf(kvMap.getKV("address").valueAsString().split(",")[1]);
              Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

              buyerQue.add(autalData);
              buyerList.add(autalData);

            }
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
      BufferedReader bfr = createReader(file);
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
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolderOrder+"good/" + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        /*
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("goodid").valueAsString().equals(goodid)) {
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            return autalData;
          }
          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
        */
        while (line != null) {

          //这个函数是由子类实现的
          if (line.indexOf("goodid:"+ goodid) != -1) {
            Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            return autalData;
          }
          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return null;
    }

    public PriorityQueue<Row> handleGoodRowQueue(String file, List<String> comparekeys, String goodid) throws IOException {

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
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolderOrderByGood+"order/" + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        /*
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("goodid").valueAsString().equals(goodid))
          {
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            goodQue.add(autalData);
          }

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
        */
        while (line != null) {
//          //这个函数是由子类实现的

          if (line.indexOf("goodid:" + goodid) != -1)
          {
            Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            goodQue.add(autalData);
          }

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return goodQue;
    }

    /**
     *
     * @param file
     * @param comparekeys
     * @param goodid
     * @return
     * @throws IOException
     */
    public List<Row> handleSumGoodRowList(String file, List<String> comparekeys, String goodid) throws IOException {


      List<Row> goodlist =new ArrayList<Row>();

      //read the index file 读取指定的索引文件
      BufferedReader bfr = createReader(UtilsDataStorge.storeFolderOrderByGood+"order/" + file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        /*
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("goodid").valueAsString().equals(goodid))
          {
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            goodQue.add(autalData);
          }

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
        */
        while (line != null) {
//          //这个函数是由子类实现的

          if (line.indexOf("goodid:" + goodid) != -1)
          {
            Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
            String filename = kvMap.getKV("address").valueAsString().split(",")[0];
            long offset =Long.valueOf( kvMap.getKV("address").valueAsString().split(",")[1]);
            Row autalData = createKVMapFromLine(OperationFiles.ReadLineByRandomAccess(filename, offset));

            goodlist.add(autalData);
          }

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } finally {
        bfr.close();
      }

      return goodlist;
    }
  }

  /**
   * 多线程构建索引文件
   */
  private class ReadAllFilesThread implements Runnable {

    private Collection<String> files;
    private ConcurrentHashMap<Integer, FileWriter> outputWriters;
    private int flag;
    private CountDownLatch latch;


    public ReadAllFilesThread(Collection<String> files, ConcurrentHashMap<Integer, FileWriter> outputWriters, int flag, CountDownLatch latch) {
      this.files = files;
      this.outputWriters = outputWriters;
      this.flag = flag;
      this.latch = latch;
    }


    @Override
    public void run() {
      try {
        for (String file : files) {
          //正常的buffer读取
//          BufferedReader bfr = createReader(file);
          //利用randomaccessfile读取
          BufferedRandomAccessFile bfr = new BufferedRandomAccessFile(file, "r");
          try {

            long offset = 0;
//            String line = bfr.readLine();
//           String line =  new String(bfr.readLine().getBytes("iso-8859-1"), "utf-8");
            String line = bfr.readLine();
            while (line != null) {

//              Row row = createKVMapFromLineToSome(new String(line.getBytes("iso-8859-1"), "utf-8"), flag);// 返回的是一条数据的map
              String goodid = createGoodStringfromLine(new String(line.getBytes("iso-8859-1"), "utf-8"));
              String address = file.trim() + "," + String.valueOf(offset);
              //goodfiles
//              if (flag == 0)
//              {
                try {
                  //按照goodid的最后一位形成索引文件的命名
//                  String goodid = row.getKV("goodid").valueAsString();
//                  String suffix = Utils.getGoodSuffix(goodid);
//                  String content = "goodid:" + row.getKV("goodid").valueAsString()+ "\t" + "address:"+ address.trim();
                  String content = "goodid:" + goodid+ "\t" + "address:"+ address.trim();

                  int index = Utils.FNVHash1(goodid);
//                  System.out.println("good的文件索引: " + index);
                  //在索引文件中创建索引记录
                  outputWriters.get(index).write(content + "\n");

                } catch (Exception e) {
                  e.printStackTrace();
                }
//              }

              //buyerfiles
//              else
//              {
//                try {
//                  String buyerid =  row.getKV("buyerid").valueAsString();
////                  String suffix = Utils.getGoodSuffix(buyerid);
//                  String content = "buyerid:" + row.getKV("buyerid").valueAsString() + "\t"  + "address:" + address.trim() ;
//                  int index = Utils.FNVHash1(buyerid);
////                  System.out.println("buyer的文件索引: " + index);
//                  outputWriters.get(index).write(content+ "\n");
//                } catch (Exception e) {
//                  e.printStackTrace();
//                }
//              }

              offset +=line.length() +1;

              //读取下一行
              line = bfr.readLine();

            }
          } finally {
            bfr.close();
          }
        }
        OperationFiles.closeFileWriter(flag);
        System.out.println("buyer or good 的文件读完");
        if (flag == 1)
          UtilsDataStorge.end = true;
        latch.countDown();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }


  private class ReadBuyerFiles implements Runnable
  {
    private Collection<String> files;
    private ConcurrentHashMap<Integer, FileWriter> outputWriters;
    private int flag;
    private CountDownLatch latch;


    public ReadBuyerFiles(Collection<String> files, ConcurrentHashMap<Integer, FileWriter> outputWriters, int flag, CountDownLatch latch) {
      this.files = files;
      this.outputWriters = outputWriters;
      this.flag = flag;
      this.latch = latch;
    }


    @Override
    public void run() {
      try {
        for (String file : files) {
          //正常的buffer读取
          //利用randomaccessfile读取
          BufferedRandomAccessFile bfr = new BufferedRandomAccessFile(file, "r");
          try {

            long offset = 0;
            String line = bfr.readLine();
            while (line != null) {

//              Row row = createKVMapFromLineToSome(new String(line.getBytes("iso-8859-1"), "utf-8"), flag);// 返回的是一条数据的map
              String buyerid = createBuyerStringfromLine(new String(line.getBytes("iso-8859-1"), "utf-8"));
              String address = file.trim() + "," + String.valueOf(offset);

              //buyerfiles
                try {

                  String content = "buyerid:" + buyerid + "\t"  + "address:" + address.trim() ;
                  int index = Utils.FNVHash1(buyerid);
//                  System.out.println("buyer的文件索引: " + index);
                  outputWriters.get(index).write(content+ "\n");
                } catch (Exception e) {
                  e.printStackTrace();
                }


              offset +=line.length() +1;
              //读取下一行
              line = bfr.readLine();

            }
          } finally {
            bfr.close();
          }
        }
        OperationFiles.closeFileWriter(flag);
        System.out.println("buyer or good 的文件读完");
        if (flag == 1)
          UtilsDataStorge.end = true;
        latch.countDown();

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * 多线程读取order的文件
   */
  private class ReadOrderFilesInQueue implements Runnable
  {

    private String file;
    private ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> writeQueue;
    private CountDownLatch latch;
    private ConcurrentHashMap<Integer, FileWriter> outputWriter;
    private ConcurrentHashMap<Integer, FileWriter> outputGoodWriter;
    private ConcurrentHashMap<Integer, FileWriter> outputBuyerWriter;

    private boolean flag;

    public ReadOrderFilesInQueue(String file, ConcurrentHashMap<String, ConcurrentLinkedQueue<String>> writeQueue, CountDownLatch latch) {
      this.file = file;
      this.writeQueue = writeQueue;
      this.latch = latch;
    }

    public ReadOrderFilesInQueue(String file, ConcurrentHashMap<Integer, FileWriter> outputWriter, ConcurrentHashMap<Integer, FileWriter> outputGoodWriter, ConcurrentHashMap<Integer, FileWriter> outputBuyerWriter, boolean flag, CountDownLatch latch) {
      this.file = file;
      this.outputWriter = outputWriter;
      this.outputGoodWriter = outputGoodWriter;
      this.outputBuyerWriter = outputBuyerWriter;
      this.flag = flag;
      this.latch = latch;
    }

    @Override
    public void run() {

      System.out.println("开始读取order文件" + file);

      UtilsDataStorge.order_files.add(file);

//      BufferedReader bfr = null;
//      try {
//        bfr = createReader(file);
//      } catch (FileNotFoundException e) {
//        e.printStackTrace();
//      }
      BufferedRandomAccessFile bfr = null;
      try {
        bfr = new BufferedRandomAccessFile(file, "r");
      } catch (IOException e) {
        e.printStackTrace();
      }
      try {
//        int linecount = 0;
//        String line = bfr.readLine();
//        String line = new String(bfr.readLine().getBytes("iso-8859-1"), "utf-8");

        String line = bfr.readLine();
        String address = null;
        long offset =0;

        while (line != null) {

          //记录处理过的order文件条数
          UtilsDataStorge.orderFileLines.incrementAndGet();

//          Row row = createKVMapFromLineToSome(new String(line.getBytes("iso-8859-1"), "utf-8"), 1);// 返回的是一条数据的map

          String[] odata = createOrderStringfromLine(new String(line.getBytes("iso-8859-1"), "utf-8"));
          String orderid = odata[1];
          String buyerid = odata[0];
          String goodid =  odata[3];
          String createtime =odata[2];

          address = file.trim() + "," + String.valueOf(offset);

          //order
            try {

               int indexorder = Utils.FNVHash1(orderid);
               int indexOrderBuyer = Utils.FNVHash1(buyerid);
               int indexOrderGood = Utils.FNVHash1(goodid);
              String contentOrder = "orderid:" + orderid + "\t"  + "address:" + address.trim() ;
              String contentOrderBuyer = "buyerid:" + buyerid + "\t" +"createtime:" + createtime + "\t" + "address:" + address.trim();
              String contentOrderGood = "goodid:" + goodid + "\t"  + "address:" + address.trim();

              outputWriter.get(indexorder).write(contentOrder + "\n");
              outputBuyerWriter.get(indexOrderBuyer).write(contentOrderBuyer + "\n");
              outputGoodWriter.get(indexOrderGood).write(contentOrderGood + "\n");


            } catch (Exception e) {
              e.printStackTrace();
            }
          //下一条数据的偏移量
          offset += line.length() +1;

          line = bfr.readLine();
        }
      }catch(Exception e)
      {
        e.printStackTrace();
      }
      System.out.println("****************结束一个文件的读************************" + file);

      System.out.println("latch down");
      latch.countDown();
      UtilsDataStorge.countFile.incrementAndGet();
    }
  }
  /**
   *  多线程构建树
   */
  private class ConstructTree implements Runnable
  {

    private Collection<String> files;
    private CountDownLatch latch;
    private TreeMap tmpTree;
    private List<String> compareKeys;

    public ConstructTree(Collection<String> files, CountDownLatch latch, TreeMap tmpTree, List<String> compareKeys) {
      this.files = files;
      this.latch = latch;
      this.tmpTree = tmpTree;
      this.compareKeys = compareKeys;
    }

    @Override
    public void run() {
      try {
        for (String file : files) {
          BufferedReader bfr = createReader(file);
          try {
            String line = bfr.readLine();
            while (line != null) {
              Row kvMap = createKVMapFromLine(line);
              handleRow(kvMap);
              line = bfr.readLine();
            }
          } finally {
            bfr.close();
          }
        }
        latch.countDown();

      }catch (Exception e)
      {
        e.printStackTrace();
      }

    }

    private void handleRow(Row row)
    {
      tmpTree.put(new ComparableKeys(
              compareKeys, row), row);
    }
  }
//  TreeMap<ComparableKeys, Row> orderDataSortedByOrder = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//  TreeMap<ComparableKeys, Row> orderDataSortedByBuyerCreateTime = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//  TreeMap<ComparableKeys, Row> orderDataSortedBySalerGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
//  TreeMap<ComparableKeys, Row> orderDataSortedByGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
  TreeMap<ComparableKeys, Row> buyerDataStoredByBuyer = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
  TreeMap<ComparableKeys, Row> goodDataStoredByGood = new TreeMap<OrderSystemImpl.ComparableKeys, Row>();
  public static String goodIndexFile= "goodIndexFile";
  public static String orderIdexFile = "orderIndexFile";
  public static String buyerIndexFile = "buyerIndexFile";
  public static String orderBuyerCreateTimeOrderIdFile = "orderIndxByBuyerCreateTimeFile";
  public static String orderSalerGoodOrderIdFile = "orderIndexBySalerGoodFile";
  public static String orderGoodOrderIdFile = "orderIndexByGoodFile";
  //存order缓存
//  public static ConcurrentHashMap<String, Row> orderSearchCache = new ConcurrentHashMap<String, Row>();

  Lock orderlock = new ReentrantLock();
  Lock orderByBuyerlock = new ReentrantLock();
  Lock orderBySalerlock = new ReentrantLock();
  Lock orderSumlock = new ReentrantLock();

  LRUCache<String, Row> queryOrderCache ;
  LRUCache<String, Object> queryByBuyerCache;
  LRUCache<String, Object> queryBySalerCache;
  LRUCache<String, Object> sumOrderCache;
//  LRUCache<String, Object>  testcache;

  // 记录开始构建的时间
  long startContTime;

  //全部结束构建的时间 主要是为了查询占用了查询多长时间
  long endContTime;

  ExecutorService service;

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

    queryOrderCache = new LRUCache<String, Row>(10000);
    queryByBuyerCache = new LRUCache<String, Object>(10000);
    queryBySalerCache = new LRUCache<String, Object>(10000);
    sumOrderCache = new LRUCache<String, Object>(10000);
//    testcache = new LRUCache<String, Object>(10000);
    service = Executors.newFixedThreadPool(4);

  }

  public static void main(String[] args) throws IOException,
      InterruptedException {

    // init order system
    List<String> orderFiles = new ArrayList<String>();
    List<String> buyerFiles = new ArrayList<String>();
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

    storeFolders.add("/media/qinjiawei/000C8D1A0003D585/prerun_data/disk1/");
    storeFolders.add("/media/qinjiawei/000C8D1A0003D585/prerun_data/disk2/");
    storeFolders.add("/media/qinjiawei/000C8D1A0003D585/prerun_data/disk3/");


//    UtilsDataStorge.storeFolder = "/media/qinjiawei/000C8D1A0003D585/prerun_data/indexgood_buyer/";
    OrderSystem os = new OrderSystemImpl();

    long start = System.currentTimeMillis();

    os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

    long end =0;
//    System.out.println( "construct cost of time :" + (end1 - start) + "ms");


    // 用例


//    start = System.currentTimeMillis();
    long orderid = 589555952;
    start = System.currentTimeMillis();
    System.out.println("\n查询订单号为" + orderid + "的订单");
    System.out.println(os.queryOrder(orderid, null));
    end= System.currentTimeMillis();


    System.out.println( "query order cost of time :" + (end - start) + "ms");



    String buyerid = "ap-8a57-454ce6fcfb19";
    long startTime = 1470344717;
    long endTime = 1483767105;
    start = System.currentTimeMillis();
    System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
    Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
    while (it.hasNext()) {
      System.out.println(it.next());
    }

    end = System.currentTimeMillis();
    //long end = System.currentTimeMillis();
    System.out.println( "saerch buyer cost of time :" + (end - start) + "ms");



    /*
    String goodid = "al-814a-e3bba7062bdd";
    String salerid = "ay-9f78-e1fe3f5fb5ce";
    System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
    List<String> querykeys  = new ArrayList<String>();
    querykeys.add("good_name");
    querykeys.add("a_o_12490");
    querykeys.add("a_o_4082");
    querykeys.add("buyerid");
    querykeys.add("a_o_9238");

    Iterator<Result> it = os.queryOrdersBySaler(salerid, goodid, querykeys);
    int count =0;
//    while (it.hasNext()) {
//      System.out.println(it.next());
//      count++;
//    }
//    System.out.println(count);
    while (it.hasNext())
    {
      System.out.println(it.next());

      count++;
    }
     System.out.println(count);

//     goodid = "al-9c4b-d5d5da969170";
//     salerid = "tm-aff2-7a1793da34da";
//     System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
//     querykeys  = new ArrayList<String>();
//     querykeys.add("goodid");
     it = os.queryOrdersBySaler(salerid, goodid, querykeys);
     count =0;
    while (it.hasNext())
    {
      System.out.println(it.next());
      count++;
    }
    System.out.println(count);
    */
//
//
//
//

    String goodid = "dd-8834-c6874b116c42";
    String attr = "amount";
    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");

    start = System.currentTimeMillis();
    System.out.println(os.sumOrdersByGood(goodid, attr));
    end = System.currentTimeMillis();
    System.out.println("sum sonst of the time is " + (end - start));

    attr = "amount";
    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
    KeyValue sum = os.sumOrdersByGood(goodid, attr);
    if (sum == null) {
      System.out.println("由于该字段是布尔类型，返回值是null");
    }
    /*
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

  /**
   * 形成good数据的有用数组
   * @param line
   * @return
   */
  public static String createGoodStringfromLine(String line)
  {
//    String[] str = new String[1];
    String value = line.split("goodid:")[1].split("\t")[0];
//    str[0] = value;
    return value;

  }

  /**
   * 形成buyer数据的数组
   * @param line
   * @return
   */
  public static String createBuyerStringfromLine(String line)
  {
//    String[] str = new String[1];
    String value = line.split("buyerid:")[1].split("\t")[0];
//    str[0] = value;
    return value;

  }

  /**
   * 形成order有用数据的数组
   * @param line
   * @return
   */
  public static String[] createOrderStringfromLine(String line)
  {
    String[] str = new String[4];
    String value = line.split("buyerid:")[1].split("\t")[0];
    str[0] = value;

    //orderid
    value = line.split("orderid:")[1].split("\t")[0];
    str[1] = value;

    //createtime
    value = line.split("createtime:")[1].split("\t")[0];
    str[2] = value;

    //goodid
    value = line.split("goodid:")[1].split("\t")[0];
    str[3] = value;
    return str;

  }

  private abstract class DataFileHandler {
    abstract void handleRow(Row row);

    void handle(Collection<String> files) throws IOException {
      for (String file : files) {
        BufferedReader bfr = createReader(file);
        try {
          String line = bfr.readLine();
          while (line != null) {
            Row kvMap = createKVMapFromLine(line);
            handleRow(kvMap);
            line = bfr.readLine();
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
    int count =0;
    for (String storge: storeFolders) {

      if (count == 0)
      UtilsDataStorge.storeFolderOrder = storge;
      else if (count == 1)
      UtilsDataStorge.storeFolderOrderBybuyer = storge;
      else {
        UtilsDataStorge.storeFolderOrderByGood = storge;
        break;
      }
      count++;
    }


    //记录所有的order文件路径
//    UtilsDataStorge.order_files = orderFiles;
    //整体开始构建的时间
    startContTime = System.currentTimeMillis();

    //创建文件流
    OperationFiles.CreateFileWriter();

    //记录总的文件数量
    UtilsDataStorge.countAllFiles = orderFiles.size();
    System.out.println("总的order文件数量" + orderFiles.size());

    CountDownLatch latch = new CountDownLatch(2 + orderFiles.size());

    new Thread(new ReadAllFilesThread(goodFiles, UtilsDataStorge.goodFileswriterMap, 0, latch)).start();
    new Thread(new ReadBuyerFiles(buyerFiles, UtilsDataStorge.buyerFileswriterMap, 2, latch)).start();
//    new Thread(new ReadAllFilesThread(orderFiles, UtilsDataStorge.orderFileswriterMap, 1, latch)).start();
//    new Thread(new ConstructTree(goodFiles, latch, goodDataStoredByGood, comparableKeysOrderingByGood)).start();
//    new Thread(new ConstructTree(buyerFiles, latch, buyerDataStoredByBuyer, comparableKeysOrderingByBuyer)).start();


    for (String file : orderFiles)
    {
      service.execute(new ReadOrderFilesInQueue(file,UtilsDataStorge.orderFileswriterMap, UtilsDataStorge.ordergoodFileswriterMap, UtilsDataStorge.orderbuyerFileswriterMap , false, latch));
    }

    //wait the limit time
//    new Thread(new WriteIntoFileThread(latch)).start();
//    latch.await(2,TimeUnit.SECONDS);
     latch.await(3590, TimeUnit.SECONDS);

    System.out.println("构建结束的时候, 已经处理过的order文件条数：" + UtilsDataStorge.orderFileLines.get());
     System.out.println("构建结束的时候, 已经处理过的order文件数：" + UtilsDataStorge.countFile.get());
     if (UtilsDataStorge.countFile.get() == UtilsDataStorge.countAllFiles)
     {
       endContTime = System.currentTimeMillis();
       System.out.println("结束构建的时候用时:" + (endContTime - startContTime) + "ms");
       UtilsDataStorge.end = true;
       try {
         service.shutdown();
         OperationFiles.closeFileWriter(1);
         System.out.println("construct#############################order 文件关闭");
         System.out.println("在construct中完成了order构建, 总的order条目是：" + UtilsDataStorge.orderFileLines.get());
       } catch (IOException e) {
         System.out.println("文件已经关闭了");
       }
     }
//    service.shutdown();
    //关闭文件流
//    OperationFiles.closeFileWriter(1);
//    service.shutdown();
  }

  public Result queryOrder(long orderId, Collection<String> keys) {


    //写个循环等待
    while (!UtilsDataStorge.end)
    {
      if (UtilsDataStorge.countFile.get() == UtilsDataStorge.countAllFiles) {
        try {
          UtilsDataStorge.end = true;
          endContTime = System.currentTimeMillis();
          System.out.println("结束构建的时候用时:" + (endContTime - startContTime) + "ms");

          service.shutdown();
          OperationFiles.closeFileWriter(1);
          System.out.println("queryOrder#############################order 文件关闭");
          System.out.println("在queryOrder中完成了order构建, 总的order条目是：" + UtilsDataStorge.orderFileLines.get());
        } catch (IOException e) {
          System.out.println("文件已经关闭了");
        }
        break;
      }
        try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    Row orderData = null;
    //缓存的key
    String cacheKey = String.valueOf(orderId);
    if (keys !=null) {
      for (String key : keys)
        cacheKey += "_" + key;
    }
    if (queryOrderCache.get(cacheKey) == null) {


      Row query = new Row();
      query.putKV("orderid", orderId);
      System.out.println("*****query order " + orderId);


      //索引文件的后缀名称
//      String suffix = Utils.getOrderSuffix(orderId);
//      long indexSuffix = Utils.getOrderSuffix(orderId);
      int indexSuffix = Utils.FNVHash1(String.valueOf(orderId));
      try {
        DataIndexFileHandler DIF = new DataIndexFileHandler();
//        System.out.println("查询的文件:" + OrderSystemImpl.orderIdexFile + indexSuffix + ".txt");
        orderData = DIF.handleOrderLine(OrderSystemImpl.orderIdexFile + indexSuffix + ".txt", comparableKeysOrderingByOrderId, orderId);
//        orderData = queryOrderByViolence(orderId);

      } catch (IOException e) {
        e.printStackTrace();
      } catch (Exception e) {
        e.printStackTrace();
      }


      queryOrderCache.put(cacheKey, orderData);

      if (orderData == null)
        return null;

    }

    else
    {
      orderData = queryOrderCache.get(cacheKey);
      System.out.println("get the order form the cache " + orderData.size());
      if (orderData == null)
        return null;
    }
//    System.out.println(orderData);

    Result result = createResultFromOrderData(orderData, createQueryKeys(keys));

    return result;
  }


  private ResultImpl createResultFromOrderData(Row orderData,
      Collection<String> keys) {


    //从获取到的索引来获取真实的数据

       Row buyerData = null;
       Row goodData = null;

//      String suffix = Utils.getGoodSuffix(orderData.getKV("buyerid").valueAsString());
      int buyerindex = Utils.FNVHash1(orderData.getKV("buyerid").valueAsString());

      //索引map
      try {
        DataIndexFileHandler DIF = new  DataIndexFileHandler();
        buyerData =  DIF.handleBuyerLine(OrderSystemImpl.buyerIndexFile + buyerindex + ".txt", comparableKeysOrderingByBuyer, orderData.getKV("buyerid").valueAsString());

      } catch (IOException e) {
        e.printStackTrace();
      }


//      String suffixx = Utils.getGoodSuffix(orderData.getKV("goodid").valueAsString());
      int goodindex = Utils.FNVHash1(orderData.getKV("goodid").valueAsString());
      //索引map
      try {

        DataIndexFileHandler DIF = new DataIndexFileHandler();
       goodData = DIF.handleGoodLine(OrderSystemImpl.goodIndexFile + goodindex + ".txt", comparableKeysOrderingByGood, orderData.getKV("goodid").valueAsString());

      } catch (IOException e) {
        e.printStackTrace();
      }

//    Row buyerQuery = new Row(orderData.getKV("buyerid"));
//    Row buyerData = buyerDataStoredByBuyer.get(new ComparableKeys(
//            comparableKeysOrderingByBuyer, buyerQuery));
//
//    Row goodQuery = new Row(orderData.getKV("goodid"));
//    Row goodData = goodDataStoredByGood.get(new ComparableKeys(
//            comparableKeysOrderingByGood, goodQuery));

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


    //写个循环等待
    while (!UtilsDataStorge.end)
    {
      if (UtilsDataStorge.countFile.get() == UtilsDataStorge.countAllFiles) {
        try {
          UtilsDataStorge.end = true;
          endContTime = System.currentTimeMillis();
          System.out.println("结束构建的时候用时:" + (endContTime - startContTime) + "ms");

          service.shutdown();
          OperationFiles.closeFileWriter(1);
          System.out.println("###################################order 文件关闭");
          System.out.println("在queryOrdersByBuyer中完成了order构建, 总的order条目是：" + UtilsDataStorge.orderFileLines.get());

        } catch (IOException e) {
          System.out.println("文件已经关闭了");
        }
        break;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


    PriorityQueue<Row> buyerQUeue = null;
    List<Row> tmpQueue = new ArrayList<Row>();

    //缓存的key
    String cacheKey = String.valueOf(startTime) +"_"+String.valueOf(endTime)+"_" + buyerid;
    if (queryByBuyerCache.get(cacheKey) == null) {

//      System.out.println("*****query buyerid by time " + buyerid);
//      System.out.println("***** start time " + startTime + " endtime " + endTime);


//      Row queryStart = new Row();
//      queryStart.putKV("buyerid", buyerid);
//      queryStart.putKV("createtime", startTime);
//      queryStart.putKV("orderid", Long.MIN_VALUE);
//
//      Row queryEnd = new Row();
//      queryEnd.putKV("buyerid", buyerid);
//      queryEnd.putKV("createtime", endTime - 1); // exclusive end
//      queryEnd.putKV("orderid", Long.MAX_VALUE);

//      String suffixIndexFile = Utils.getGoodSuffix(buyerid);
      int buyerindex = Utils.FNVHash1(buyerid);

      try {
        DataIndexFileHandler DIF = new DataIndexFileHandler();
        buyerQUeue = DIF.handleBuyerRowQue(OrderSystemImpl.orderBuyerCreateTimeOrderIdFile + buyerindex + ".txt", startTime, endTime - 1, buyerid);
//        buyerQUeue = queryOrderByBuyerByviloence(startTime, endTime-1, buyerid);

      } catch (IOException e) {
        e.printStackTrace();
      }

//      while (buyerQUeue.size() >0)
//      {
//        tmpQueue.add(buyerQUeue.poll());
//      }

//      tmpQueue = new ArrayList<Row>(buyerQUeue);

//      queryByBuyerCache.put(cacheKey, tmpQueue);

    }
    else
    {
//      buyerQUeue = (PriorityQueue<Row>) queryByBuyerCache.get(cacheKey);
      tmpQueue = (List<Row>)queryByBuyerCache.get(cacheKey);
      System.out.println("#####buyercache get the row size " + tmpQueue.size());
//      Utils.PrintCache(queryByBuyerCache, "queryBuyer");

    }
    final PriorityQueue<Row> orderIndexs =buyerQUeue;
//    final List<Row> orderIndexs = tmpQueue;

    Iterator<OrderSystem.Result> result =  new Iterator<OrderSystem.Result>() {

      Queue<Row> o =orderIndexs;

//      Iterator<Row> iterator = orderIndexs.iterator();

      public boolean hasNext() {
        return o != null && o.size() > 0;
//      return iterator.hasNext();
      }

      public Result next() {
        if (!hasNext()) {
          return null;
        }
        Row orderData = o.poll();
//        Row orderData = iterator.next();

        return createResultFromOrderData(orderData, null);
      }

      public void remove() {

      }
    };

    return  result;
  }

  public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
      Collection<String> keys) {

    //写个循环等待
    while (!UtilsDataStorge.end)
    {
      if (UtilsDataStorge.countFile.get() == UtilsDataStorge.countAllFiles) {
        try {
          UtilsDataStorge.end = true;
          endContTime = System.currentTimeMillis();
          System.out.println("结束构建的时候用时:" + (endContTime - startContTime) + "ms");

          service.shutdown();
          OperationFiles.closeFileWriter(1);
          System.out.println("###################################order 文件关闭");
          System.out.println("在queryOrdersBySaler中完成了order构建, 总的order条目是：" + UtilsDataStorge.orderFileLines.get());

        } catch (IOException e) {
          System.out.println("文件已经关闭了");
        }
        break;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    PriorityQueue<Row> orderDataSortedBySalerQueue = null;
    List<Row> tmpQueue = new ArrayList<Row>();

    final Collection<String> queryKeys = keys;

    String cacheKey = salerid + "_"+ goodid;
    if (keys !=null) {
      for (String key : keys) {
        cacheKey += "_" + key;
      }
    }
    if (queryBySalerCache.get(cacheKey) == null) {
      System.out.println("*****query saler by id " + salerid);
      System.out.println("*****query goodid by id " + goodid);



//      Row queryStart = new Row();
//      queryStart.putKV("goodid", goodid);
//      queryStart.putKV("orderid", Long.MIN_VALUE);
//
//      Row queryEnd = new Row();
//      queryEnd.putKV("goodid", goodid);
//      queryEnd.putKV("orderid", Long.MAX_VALUE);


//      String suffixIndexFile = Utils.getGoodSuffix(goodid);
      int goodindex = Utils.FNVHash1(goodid);

      try {
        DataIndexFileHandler DIF = new DataIndexFileHandler();
        orderDataSortedBySalerQueue = DIF.handleGoodRowQueue(OrderSystemImpl.orderGoodOrderIdFile + goodindex + ".txt", comparableKeysOrderingBySalerGoodOrderId, goodid);
//        orderDataSortedBySalerQueue = queryOrderBySalerViolence(goodid);
      } catch (IOException e) {
        e.printStackTrace();
      }

      //把数值进行有序的存储
//      while (orderDataSortedBySalerQueue.size() > 0)
//      {
//        tmpQueue.add(orderDataSortedBySalerQueue.poll());
//      }

//        queryBySalerCache.put(cacheKey, orderDataSortedBySalerQueue);
//      queryBySalerCache.put(cacheKey, tmpQueue);

    }

    else {

//      orderDataSortedBySalerQueue = (PriorityQueue<Row>)queryBySalerCache.get(cacheKey);
//      Utils.PrintCache(queryBySalerCache, "querySaler");
      tmpQueue = (List<Row>) queryBySalerCache.get(cacheKey);

      System.out.println("#####saler get from the cache the size is " + tmpQueue.size());
//      System.out.println(this.queryBySalerCache.get(cacheKey));

    }

    final  PriorityQueue<Row> orderIndexsBySaler = orderDataSortedBySalerQueue;

//    final  List<Row> orderIndexsBySaler = tmpQueue;

    Iterator<OrderSystem.Result> result =  new Iterator<OrderSystem.Result>() {

    Queue<Row> o = orderIndexsBySaler;
//      Iterator<Row> iterator = orderIndexsBySaler.iterator();

      public boolean hasNext() {
        return o != null && o.size() > 0;
//        return iterator.hasNext();
      }

      public Result next() {
        if (!hasNext()) {
          return null;
        }

        Row orderData = o.poll();
//        Row orderData = iterator.next();


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

    //写个循环等待
    while (!UtilsDataStorge.end)
    {
      if (UtilsDataStorge.countFile.get() == UtilsDataStorge.countAllFiles) {
        try {
          UtilsDataStorge.end = true;
          endContTime = System.currentTimeMillis();
          System.out.println("结束构建的时候用时:" + (endContTime - startContTime) + "ms");

          service.shutdown();
          OperationFiles.closeFileWriter(1);
          System.out.println("###################################order 文件关闭");
          System.out.println("在sumOrdersByGood中完成了order构建, 总的order条目是：" + UtilsDataStorge.orderFileLines.get());
        } catch (IOException e) {
          System.out.println("文件已经关闭了");
        }
        break;
      }
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

//    PriorityQueue<Row> orderDataSortedByGoodQueue = null;
    List<Row> goodList = null;
    String cacheKey = goodid;
    if (key != null) {
      cacheKey += "_" + key;
    }
//
    if (sumOrderCache.get(cacheKey) == null) {

      System.out.println("***** query the sum of some keys in goodid : " + goodid + " key :" + key);

//      Row queryStart = new Row();
//      queryStart.putKV("goodid", goodid);
//      queryStart.putKV("orderid", Long.MIN_VALUE);
//      Row queryEnd = new Row();
//      queryEnd.putKV("goodid", goodid);
//      queryEnd.putKV("orderid", Long.MAX_VALUE);


//      String suffixIndexFile = Utils.getGoodSuffix(goodid);

      int goodindex = Utils.FNVHash1(goodid);

      try {
        DataIndexFileHandler DIF = new DataIndexFileHandler();
//        orderDataSortedByGoodQueue = DIF.handleGoodRowQueue(OrderSystemImpl.orderGoodOrderIdFile + goodindex + ".txt", comparableKeysOrderingByGoodOrderId, goodid);
//        orderDataSortedByGoodQueue = queryOrderBySalerViolence(goodid);
        goodList = DIF.handleSumGoodRowList(OrderSystemImpl.orderGoodOrderIdFile + goodindex + ".txt", comparableKeysOrderingByGoodOrderId, goodid);

      } catch (IOException e) {
        e.printStackTrace();
      }

      sumOrderCache.put(cacheKey, goodList);

//      if (orderDataSortedByGoodQueue == null || orderDataSortedByGoodQueue.size() < 1) {
//        return null;
//      }


    }
    else
    {
      /*
      orderDataSortedByGoodQueue = (PriorityQueue<Row>)sumOrderCache.get(cacheKey);

//      Utils.PrintCache(sumOrderCache, "sumorder");
      System.out.println("######sum get from the cache the size is " + orderDataSortedByGoodQueue.size());

      if (orderDataSortedByGoodQueue == null || orderDataSortedByGoodQueue.size() < 1) {
        return null;
      }
     */
      goodList = (List<Row>) sumOrderCache.get(cacheKey);
      if (goodList == null || goodList.size() < 1) {
        return null;
      }

    }
    HashSet<String> queryingKeys = new HashSet<String>();
    queryingKeys.add(key);
    List<ResultImpl> allData = new ArrayList<ResultImpl>();

//    Iterator<Row> sumit = orderDataSortedByGoodQueue.iterator();
    Iterator<Row> sumit = goodList.iterator();

    while (sumit.hasNext())
    {
      allData.add(createResultFromOrderData(sumit.next(), queryingKeys));
    }

//    while (orderDataSortedByGoodQueue.size() > 0){
//
//      allData.add(createResultFromOrderData(orderDataSortedByGoodQueue.poll(), queryingKeys));
//    }

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

  /**
   * 暴力求解的方式 进行queryorder的查询 就是是获取那个orderid
   * @param orderid
   * @return
   */
  public  Row queryOrderByViolence(long orderid) throws FileNotFoundException {
    //遍历每个文件
    for (String file: UtilsDataStorge.order_files)
    {

      BufferedReader bfr = createReader(file);
      try {
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);
          if (Math.abs(kvMap.getKV("orderid").valueAsLong() - orderid) < 0.001)
            return kvMap;
          line = bfr.readLine();
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (TypeException e) {
        e.printStackTrace();
      } finally {
        try {
          if (bfr != null)
                bfr.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return null;
  }

  /**
   * 暴力求解方法 获取时间段内的用户消费
   * @param startTime
   * @param endTime
   * @param buyerid
   * @return
   */
  public PriorityQueue<Row> queryOrderByBuyerByviloence(long startTime, long endTime, String buyerid) throws FileNotFoundException {

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


    //遍历每个文件
    for (String file: UtilsDataStorge.order_files)
    {

      BufferedReader bfr = createReader(file);
      try {
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);
          if (kvMap.get("buyerid").valueAsString().equals(buyerid) && kvMap.getKV("createtime").valueAsLong() >=startTime && kvMap.getKV("createtime").valueAsLong() <= endTime)
          {
            buyerQue.offer(kvMap);
//            System.out.println("add to the queue " + kvMap.getKV("orderid").valueAsLong());
          }
          line = bfr.readLine();
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (TypeException e) {
        e.printStackTrace();
      } finally {
        try {
          if (bfr != null)
            bfr.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
     return buyerQue;
  }

  /**
   * 暴力求解
   * @param goodid
   * @return
   * @throws FileNotFoundException
   */
  public PriorityQueue<Row> queryOrderBySalerViolence(String goodid) throws FileNotFoundException {
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

    //遍历所有的文件
    for (String file : UtilsDataStorge.order_files) {
      BufferedReader bfr = createReader(file);
      try {
        //int linecount =0;
        String line = bfr.readLine();
        while (line != null) {
          Row kvMap = createKVMapFromLine(line);// 返回的是一条数据的map
          //这个函数是由子类实现的
          if (kvMap.get("goodid").valueAsString().equals(goodid))
            goodQue.offer(kvMap);

          //读取下一行
          line = bfr.readLine();
          //linecount +=1;
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          if (bfr != null)
              bfr.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return goodQue;
  }



}
