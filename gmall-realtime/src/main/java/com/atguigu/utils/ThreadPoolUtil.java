package com.atguigu.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ThreadPoolUtil {
    public static ThreadPoolExecutor pool;

    private ThreadPoolUtil(){
    }

    // 单例对象
    public static ThreadPoolExecutor getInstance(){
        if (pool == null){
            synchronized(ThreadPoolUtil.class){
                if (pool == null){
                    /*
                    corePoolSize:池中线程数量，决定添加的任务是开辟新的线程去执行，还是放到 workQueue 任务队列中去；
                    maximumPoolSize:线程池中最大线程数量,根据 workQueue 类型，决定线程池会开辟的最大线程数量；
                    keepAliveTime:当线程池中空闲线程数量超过corePoolSize时，多余的线程会在多长时间内被销毁；
                    unit:keepAliveTime 的单位
                    workQueue:任务队列，被添加到线程池中，但尚未被执行的任务
                    * */
                    pool = new ThreadPoolExecutor(6,
                            12,
                            300L,
                            TimeUnit.SECONDS,
                            new LinkedBlockingDeque<>());
                }
            }
        }
        return pool;
    }
}
