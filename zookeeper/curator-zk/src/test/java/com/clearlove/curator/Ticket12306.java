package com.clearlove.curator;

import java.util.concurrent.TimeUnit;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;

/**
 * @author promise
 * @date 2025/2/15 - 19:27
 */
public class Ticket12306 implements Runnable {

  // 数据库的票数
  private int tickets = 10;

  private InterProcessLock lock;

  public Ticket12306() {
    // 重试策略
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);

    CuratorFramework client = CuratorFrameworkFactory.builder().connectString("192.168.47.100:2181")
        .sessionTimeoutMs(60 * 1000)
        .connectionTimeoutMs(15 * 1000)
        .retryPolicy(retryPolicy)
        .build();
    client.start();
    this.lock = new InterProcessMutex(client, "/lock");
  }

  @Override
  public void run() {

    while (true) {
      // 获取锁
      try {
        if (lock.acquire(3, TimeUnit.SECONDS)) {
          if (tickets > 0) {
            System.out.println(Thread.currentThread().getName() + "正在出售第" + tickets + "张票");
            Thread.sleep(100);
            tickets--;
          } else {
            break;
          }
        }
      } catch (Exception e) {
        e.printStackTrace();
      }finally{
        // 释放锁
        try {
          lock.release();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }



    }

  }
}
