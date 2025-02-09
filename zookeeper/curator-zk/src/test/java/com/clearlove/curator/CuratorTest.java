package com.clearlove.curator;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author promise
 * @date 2025/2/9 - 21:13
 */
public class CuratorTest {

  private CuratorFramework client;



  // 建立连接
  @Test
  public void testConnect1() {

    // 重试策略
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 10);

    //1. 第一种方式
    /*
    Params:
      connectString – 连接字符串 zk服务地址和端口 "192.168.xx.xx:2181,192.168.xx.xx:2181"
      sessionTimeoutMs – 会话超时时间，单位ms
      connectionTimeoutMs – 连接超时时间，单位ms
      retryPolicy – 重试策略
     */
    client = CuratorFrameworkFactory.newClient("192.168.47.100:2181",
        60 * 1000, 15 * 1000, retryPolicy);
    // 开启连接
    client.start();
  }

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
  public void testCreate1() throws Exception {
    /*
      create 持久 临时 顺序 数据
      1. 基本创建
      2. 创建节点时，添加数据
      3. 设置节点的类型
      4. 创建多级结点  /app1/p1
     */
    // 1基本创建
    // 如果创建节点，没有指定数据，则默认会将当前客户端的 ip 作为节点的数据
    String path = client.create().forPath("/app1");
    System.out.println(path);
  }

  @Test
  public void testCreate2() throws Exception {
    /*
      create 持久 临时 顺序 数据
      1. 基本创建
      2. 创建节点时，添加数据
      3. 设置节点的类型
      4. 创建多级结点  /app1/p1
     */
    // 2.创建节点，带有数据
    String path = client.create().forPath("/app2", "hello".getBytes());
    System.out.println(path);
  }

  @Test
  public void testCreate3() throws Exception {
    /*
      create 持久 临时 顺序 数据
      1. 基本创建
      2. 创建节点时，添加数据
      3. 设置节点的类型
      4. 创建多级结点  /app1/p1
     */
    // 3. 设置节点的类型
    // 默认类型，持久化
    String path = client.create().withMode(CreateMode.EPHEMERAL).forPath("/app3");
    System.out.println(path);
  }


  @Test
  public void testCreate4() throws Exception {
    /*
      create 持久 临时 顺序 数据
      1. 基本创建
      2. 创建节点时，添加数据
      3. 设置节点的类型
      4. 创建多级结点  /app1/p1
     */
    // 4. 创建多级结点  /app1/p1
    // creatingParentsIfNeeded 如果父节点不存在，则创建父节点
    String path = client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/app4/p1");
    System.out.println(path);
  }
}
