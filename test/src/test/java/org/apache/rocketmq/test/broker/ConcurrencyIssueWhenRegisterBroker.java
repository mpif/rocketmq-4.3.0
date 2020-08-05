package org.apache.rocketmq.test.broker;

import com.google.common.collect.Lists;
import org.apache.rocketmq.broker.latency.BrokerFixedThreadPoolExecutor;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.namesrv.RegisterBrokerResult;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @Author: codefans
 * @Date: 2020-08-05 8:17
 */

public class ConcurrencyIssueWhenRegisterBroker {

    @Test
    public void concurrencyIssueTest() throws InterruptedException {
        for (int i = 0; i < 100; i++) {
            final int finalI = i;
            Thread thread = new Thread(new Runnable() {
                @Override
                public void run() {
                    deal(finalI);
                }
            });
            thread.start();

            thread.join();
        }
    }

    public synchronized void deal(final int index) {
        // 初始化一个List，存放每个NameServer注册结果的
        //Lists.newArrayList()存在并发问题
//        final List<RegisterBrokerResult> registerBrokerResultList = Lists.newArrayList();
        final List<RegisterBrokerResult> registerBrokerResultList = new Vector<>(10);
        // 获取 NameServer 地址列表
        List<String> nameServerAddressList = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
            nameServerAddressList.add("192.168.0." + j);
        }
        final AtomicInteger atomicInteger = new AtomicInteger(0);
        // 执行其他逻辑
        final CountDownLatch countDownLatch = new CountDownLatch(nameServerAddressList.size());

        BrokerFixedThreadPoolExecutor brokerOuterExecutor = new BrokerFixedThreadPoolExecutor(4, 10, 1, TimeUnit.MINUTES,
                new ArrayBlockingQueue<Runnable>(32), new ThreadFactoryImpl("brokerOutApi_thread_", true));

        // 遍历NameServer 地址列表，使用线程池去注册
        for (final String namesrvAddr : nameServerAddressList) {
            brokerOuterExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        // 调用 registerBroker 真正执行注册
                        RegisterBrokerResult result = new RegisterBrokerResult();
                        result.setHaServerAddr("chen" + atomicInteger.getAndIncrement());
                        Thread.sleep(100);
                        if (result != null) {
                            // 注册成功结果放到一个List里去
                            registerBrokerResultList.add(result);
                        }

                    } catch (Exception e) {
                        System.out.println("-----------wei---------------> " + index);
                        System.out.println(e);
                        System.out.println("-----------wei---------------> " + index);
                    } finally {
                        // 注册完，执行 countDownLatch.countDown(); 同步计数器 -1
                        countDownLatch.countDown();
                    }
                }
            });
        }

        try {
            // 等待所有 NameServer 都注册完，才返回注册结果
            countDownLatch.await(1000000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
        }

        System.out.println(registerBrokerResultList.size());
//        System.out.println(registerBrokerResultList);
        System.out.println("-----------chen-[" + index + "]--------------------------------------> ");
    }

}
