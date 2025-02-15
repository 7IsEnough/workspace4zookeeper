package com.clearlove.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author promise
 * @date 2025/2/9 - 21:13
 */
public class LockTest {

  public static void main(String[] args){
    Ticket12306 ticket12306 = new Ticket12306();

    // 创建客户端
    Thread t1 = new Thread(ticket12306, "携程");
    Thread t2 = new Thread(ticket12306, "飞猪");

    t1.start();
    t2.start();
  }



}
