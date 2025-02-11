package com.clearlove.curator;

import java.util.List;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
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
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author promise
 * @date 2025/2/9 - 21:13
 */
public class CuratorWatcherTest {

  private CuratorFramework client;

  @Before
  public void testConnect2() {

    // 重试策略
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);
    //2. 第二种方式
    client = CuratorFrameworkFactory.builder().connectString("192.168.47.100:2181")
        .sessionTimeoutMs(60 * 1000)
        .connectionTimeoutMs(15 * 1000)
        .retryPolicy(retryPolicy)
        .namespace("clearlove")
        .build();
    client.start();
  }

  @After
  public void testClose() {
    if (client != null) {
      client.close();
    }
  }

  @Test
  public void testNodeCache() throws Exception {
    /*
      给指定一个节点添加监听器
     */
    // 1.创建NodeCache对象
    final NodeCache nodeCache = new NodeCache(client, "/app1");
    // 2.注册监听
    nodeCache.getListenable().addListener(new NodeCacheListener() {
      @Override
      public void nodeChanged() throws Exception {
        System.out.println("节点变化了");
        // 获取修改节点后的数据
        byte[] data = nodeCache.getCurrentData().getData();
        System.out.println("修改后的数据为：" + new String(data));
      }
    });
    // 3.开启监听
    // 如果设置为true，则开启监听时，加载缓存数据
    nodeCache.start(true);

    while (true) {

    }
  }

  @Test
  public void testPathChildrenCache() throws Exception {
     /*
      监听某个节点的所有子节点们
     */

    // 1.创建监听对象
    PathChildrenCache pathChildrenCache = new PathChildrenCache(client, "/app2", true);

    // 2.绑定监听器
    pathChildrenCache
        .getListenable()
        .addListener(
            new PathChildrenCacheListener() {
              @Override
              public void childEvent(
                  CuratorFramework curatorFramework, PathChildrenCacheEvent pathChildrenCacheEvent)
                  throws Exception {
                System.out.println("子节点变化了");
                System.out.println(pathChildrenCacheEvent);
                // 监听子节点的数据变更，并拿到变更后的数据
                // 1. 获取类型
                Type type = pathChildrenCacheEvent.getType();
                // 2.判断类型是不是update
                if (type.equals(Type.CHILD_UPDATED)) {
                  // 3.获取子节点的数据
                  byte[] data = pathChildrenCacheEvent.getData().getData();
                  System.out.println("修改后的数据为：" + new String(data));
                }
              }
            });

    // 3.开启
    pathChildrenCache.start();
    while (true) {

    }

  }

  @Test
  public void testTreeCache() throws Exception {

    /*
      监听某个节点自己和所有子节点们
     */
    // 1.创建监听对象
    TreeCache treeCache = new TreeCache(client, "/app2");
    // 2.绑定监听器
    treeCache.getListenable().addListener(new TreeCacheListener() {
      @Override
      public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent)
          throws Exception {
        System.out.println("节点变化了");
        System.out.println(treeCacheEvent);

      }
    });
    // 3.开启监听
    treeCache.start();

    while (true) {

    }

  }

}
