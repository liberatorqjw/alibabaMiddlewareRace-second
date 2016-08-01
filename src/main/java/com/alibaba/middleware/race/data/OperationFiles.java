
package com.alibaba.middleware.race.data;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.utils.BufferedRandomAccessFile;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by Liberatorqjw on 2016/7/19.
 */
public class OperationFiles {

    /**
     * 创建好文件连接, 存在map中
     *
     */
    public static void CreateFileWriter()
    {
        String[] suffix = {"a","b","c","d","e","f","g","h","i","j","k","l", "m", "n", "o", "p", "q",
                "r", "s", "t", "u", "v", "w", "x", "y", "z", "0", "1", "2", "3", "4", "5", "6", "7", "8",
                "9"};

        String[] ordersuffix = {"10","11","12","13","14","15","16","17","18","19","0", "1", "2", "3", "4", "5", "6", "7", "8",
                "9"};
        //后缀
        String sffix = ".txt";

        //创建文件夹
        //存储原始的order文件
        File filesource = new File(UtilsDataStorge.storeFolderOrder + "source");
        if (!filesource.exists() && !filesource.isDirectory())
        {
            filesource.mkdir();
        }


        File fileuyer = new File(UtilsDataStorge.storeFolderOrder + "buyer");
        if (!fileuyer.exists() && !fileuyer.isDirectory())
        {
            fileuyer.mkdir();
        }

        File filegood = new File(UtilsDataStorge.storeFolderOrder + "good");
        if (!filegood.exists() && !filegood.isDirectory())
        {
            filegood.mkdir();
        }

        File fileorder = new File(UtilsDataStorge.storeFolderOrder + "order");
        if (!fileorder.exists() && !fileorder.isDirectory())
        {
            fileorder.mkdir();
        }

        File fileorderBybuyer = new File(UtilsDataStorge.storeFolderOrderBybuyer + "order");
        if (!fileorderBybuyer.exists() && !fileorderBybuyer.isDirectory())
        {
            fileorderBybuyer.mkdir();
        }

        File fileorderBygood = new File(UtilsDataStorge.storeFolderOrderByGood + "order");
        if (!fileorderBygood.exists() && !fileorderBygood.isDirectory())
        {
            fileorderBygood.mkdir();
        }



        System.out.println(suffix.length);
        try {

            for (int i = 0; i<4096; i++)
            {
                FileWriter suffixWriter = new FileWriter(UtilsDataStorge.storeFolderOrder + "buyer/" + OrderSystemImpl.buyerIndexFile + i + sffix , true);
                UtilsDataStorge.buyerFileswriterMap.put(i, suffixWriter);

                FileWriter suffixgood = new FileWriter(UtilsDataStorge.storeFolderOrder + "good/" + OrderSystemImpl.goodIndexFile + i+ sffix, true);
                UtilsDataStorge.goodFileswriterMap.put(i, suffixgood);

                FileWriter suffixorderBuyer = new FileWriter(UtilsDataStorge.storeFolderOrderBybuyer + "order/" + OrderSystemImpl.orderBuyerCreateTimeOrderIdFile + i+ sffix, true);
                UtilsDataStorge.orderbuyerFileswriterMap.put(i, suffixorderBuyer);

                FileWriter suffixordergood = new FileWriter(UtilsDataStorge.storeFolderOrderByGood + "order/" + OrderSystemImpl.orderGoodOrderIdFile + i + sffix, true);
                UtilsDataStorge.ordergoodFileswriterMap.put(i, suffixordergood);

                FileWriter suffixorder = new FileWriter(UtilsDataStorge.storeFolderOrder + "order/" + OrderSystemImpl.orderIdexFile +  String.valueOf(i) + sffix, true);
                UtilsDataStorge.orderFileswriterMap.put(i, suffixorder);

            }


            /*
            //buyer
//            for (String su1: suffix)

            for (String su2: suffix)
            for (String su : suffix) {
                FileWriter suffixWriter = new FileWriter(UtilsDataStorge.storeFolderOrder + "buyer/" + OrderSystemImpl.buyerIndexFile +su2+su + sffix , true);
                UtilsDataStorge.buyerFileswriterMap.put(OrderSystemImpl.buyerIndexFile +su2+su + sffix, suffixWriter);

            }
            //good
//            for (String su1: suffix)
            for (String su2: suffix)
            for (String su : suffix) {
                FileWriter suffixWriter = new FileWriter(UtilsDataStorge.storeFolderOrder + "good/" + OrderSystemImpl.goodIndexFile + su2+su + sffix, true);
                UtilsDataStorge.goodFileswriterMap.put(OrderSystemImpl.goodIndexFile +su2+su + sffix, suffixWriter);

            }



            //order buyer
//            for (String su1: suffix)
            for (String su2: suffix)
            for (String su : suffix) {
                FileWriter suffixWriter = new FileWriter(UtilsDataStorge.storeFolderOrderBybuyer + "order/" + OrderSystemImpl.orderBuyerCreateTimeOrderIdFile + su2+su + sffix, true);
                ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
                UtilsDataStorge.orderFileswriterMap.put(OrderSystemImpl.orderBuyerCreateTimeOrderIdFile + su2+su + sffix, suffixWriter);
//                UtilsDataStorge.orderFileWriterqueue.put(OrderSystemImpl.orderBuyerCreateTimeOrderIdFile + su2+su + sffix, queue);
            }
            //order good
//            for (String su1: suffix)
            for (String su2: suffix)
            for (String su : suffix) {
                FileWriter suffixWriter = new FileWriter(UtilsDataStorge.storeFolderOrderByGood + "order/" + OrderSystemImpl.orderGoodOrderIdFile + su2+su + sffix, true);
                ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();
                UtilsDataStorge.orderFileswriterMap.put(OrderSystemImpl.orderGoodOrderIdFile + su2+su + sffix, suffixWriter);
//                UtilsDataStorge.orderFileWriterqueue.put(OrderSystemImpl.orderGoodOrderIdFile + su2+su + sffix, queue);
            }

            //order index
            for (int i =0 ; i < 1000; i++) {
                FileWriter suffixWriter = new FileWriter(UtilsDataStorge.storeFolderOrder + "order/" + OrderSystemImpl.orderIdexFile +  String.valueOf(i) + sffix, true);
                ConcurrentLinkedQueue<String> queue = new ConcurrentLinkedQueue<String>();

                UtilsDataStorge.orderFileswriterMap.put(OrderSystemImpl.orderIdexFile + String.valueOf(i)+ sffix, suffixWriter);
//                UtilsDataStorge.orderFileWriterqueue.put(OrderSystemImpl.orderIdexFile + String.valueOf(i)+ sffix, queue);
            }
            */



        }catch (Exception e)
        {
            e.printStackTrace();
        }
    }



    /**
     *
     * @param flag
     * @throws IOException
     */
    public static void closeFileWriter(int flag) throws IOException {
        //good
        if (flag ==0)
        {
            Collection<FileWriter> goodfilesOut = UtilsDataStorge.goodFileswriterMap.values();
            for (FileWriter bout: goodfilesOut)
                bout.close();
        }
        //order
        else if(flag == 1)
        {
            Collection<FileWriter> orderfilesOut = UtilsDataStorge.orderFileswriterMap.values();
            for (FileWriter bout: orderfilesOut)
                bout.close();
            orderfilesOut = UtilsDataStorge.orderbuyerFileswriterMap.values();
            for (FileWriter bout: orderfilesOut)
                bout.close();
            orderfilesOut = UtilsDataStorge.ordergoodFileswriterMap.values();
            for (FileWriter bout: orderfilesOut)
                bout.close();

        }
        //buyer
        else {
            Collection<FileWriter> buyerfilesOut = UtilsDataStorge.buyerFileswriterMap.values();
            for (FileWriter bout: buyerfilesOut)
                bout.close();
        }
    }

    /**
     * 效率高的写入
     * @param filename
     * @param index
     * @param contentAddress
     * @param flag
     * @throws IOException
     */
    public static void WriteIntoFile(String filename, String index, String contentAddress, int flag) throws IOException {

//        FileWriter writer = new FileWriter(UtilsDataStorge.storeFolder + path, true);
//        writer.write(index + "\t" + contentAddress + "\n");
//        writer.close();
        BufferedOutputStream Buff= null;
//        String path = UtilsDataStorge.storeFolder + filename;
        String content = index + "\t" + contentAddress + "\n";

        //good
        if (flag ==0)
        {

            UtilsDataStorge.goodFileswriterMap.get(filename).write(content);
        }
        //order
        else if (flag ==1)
        {

            UtilsDataStorge.orderFileswriterMap.get(filename).write(content);
        }
        //buyer
        else
        {

            UtilsDataStorge.buyerFileswriterMap.get(filename).write(content);

        }

    }

    /**
     * 利用buffer randomAccesssfile读取指定的一一条数据
     * @param filepath
     * @param offset
     * @return
     */
    public static String ReadLineByRandomAccess(String filepath, long offset)
    {
        UtilsDataStorge.countRandomAccessfile.incrementAndGet();

        long start = System.currentTimeMillis();
        BufferedRandomAccessFile bfr = null;
        try {
             bfr = new BufferedRandomAccessFile(filepath, "r");
            bfr.seek(offset);
            return new String(bfr.readLine().getBytes("iso-8859-1"), "utf-8");

        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (bfr != null)
                try {
                    bfr.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            //每一万次打印一次, 并且只打印order的信息
            if (UtilsDataStorge.countRandomAccessfile.get() % 10000 == 0 && filepath.indexOf("order") != -1)
            System.out.println("search " + filepath + "-"+ offset +"耗费的时间"+ (System.currentTimeMillis() - start));
        }
      return null;
    }

}
