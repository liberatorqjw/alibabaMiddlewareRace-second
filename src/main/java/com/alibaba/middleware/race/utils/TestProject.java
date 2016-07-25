package com.alibaba.middleware.race.utils;

import java.util.Iterator;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * Created by qinjiawei on 16-7-25.
 */
public class TestProject {

    LRUCache<String, Object> test ;

    public TestProject() {
       test =  new LRUCache<String, Object>(5);
    }

    public void doLru(String id, String value)
    {
//        test.put(id, value);

        if (test.get(id) == null)
        {
            test.put(id, value);
        }
        else
        {
            String tmp = (String)test.get(id);
            System.out.println("get from the cache");
        }

        for (Map.Entry<String, Object> entry : test.getAll())
        {
            System.out.println("key: " + entry.getKey() + " value :"+ entry.getValue());
        }
    }

    public static void main(String[] args)
    {
        TestProject t = new TestProject();
        t.doLru("123", "bob");
        t.doLru("123", "bob");
        TestProject t2;

        PriorityQueue<String> qu = new PriorityQueue<String>(11);
        qu.offer("1");
        qu.offer("2");
        qu.offer("3");
        qu.offer("4");

        Iterator<String> it = qu.iterator();
        while (it.hasNext())
        {
            it.next();
        }
        System.out.println(qu.size());

    }
}
