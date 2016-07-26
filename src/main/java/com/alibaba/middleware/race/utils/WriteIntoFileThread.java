package com.alibaba.middleware.race.utils;

import com.alibaba.middleware.race.data.UtilsDataStorge;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

/**
 * Created by qinjiawei on 16-7-26.
 */
public class WriteIntoFileThread implements Runnable {


    private boolean flag;
    private CountDownLatch latch;

    public WriteIntoFileThread(CountDownLatch latch) {
        this.latch = latch;
    }

    @Override
    public void run() {

        System.out.println("队列开始写入了");
        while (true)
        {
            flag = false;
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for (Map.Entry<String, ConcurrentLinkedQueue<String>> entry :UtilsDataStorge.orderFileWriterqueue.entrySet())
            {
                if (!entry.getValue().isEmpty())
                {
                    flag = true;
                    try {
                        UtilsDataStorge.orderFileswriterMap.get(entry.getKey()).write(entry.getValue().poll());
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            if (!flag)
                break;
        }
        System.out.println("##############队列的写入操作结束#####################");
        latch.countDown();

    }
}
