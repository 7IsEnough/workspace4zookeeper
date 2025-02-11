package com.clearlove.curator;

import java.util.List;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
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

  @Test
  public void testGet1() throws Exception {
    /*
      查询节点：
      1. 查询节点数据
      2. 查询子节点
      3. 查询节点状态信息
     */
    // 1. 查询节点数据
    byte[] data = client.getData().forPath("/app1");
    System.out.println(new String(data));
  }

  @Test
  public void testGet2() throws Exception {
    /*
      查询节点：
      1. 查询节点数据
      2. 查询子节点
      3. 查询节点状态信息
     */
    // 2. 查询子节点
    List<String> path = client.getChildren().forPath("/");
    System.out.println(path);
  }

  @Test
  public void testGet3() throws Exception {
    /*
     查询节点：
     1. 查询节点数据
     2. 查询子节点
     3. 查询节点状态信息
    */
    // 3. 查询节点状态信息
    // Stat stat = new Stat();
    // System.out.println(stat);
    // client.getData().storingStatIn(stat).forPath("/app1");
    // System.out.println(stat);

    Stat stat = client.checkExists().forPath("/app1");
    System.out.println(stat);
  }

  @Test
  public void testSet() throws Exception {
    /*
      修改数据
      1. 修改节点数据
      2. 根据版本修改数据
     */
    //1. 修改节点数据
    client.setData().forPath("/app1", "clearlove7".getBytes());
  }

  @Test
  public void testSetForVersion() throws Exception {
    /*
      修改数据
      1. 修改节点数据
      2. 根据版本修改数据
     */
    //2. 根据版本修改数据
    Stat stat = client.checkExists().forPath("/app1");
    System.out.println(stat.getVersion());
    client.setData().withVersion(stat.getVersion()).forPath("/app1", "clearlove8".getBytes());
  }

  @Test
  public void testDelete() throws Exception {
    /*
      删除节点
      1. 删除单个节点
      2. 删除带有子节点的节点
      3. 必须成功的删除
      4. 回调
     */
    //1. 删除单个节点
    client.delete().forPath("/app1");
  }

  @Test
  public void testDelete2() throws Exception {
    /*
      删除节点
      1. 删除单个节点
      2. 删除带有子节点的节点
      3. 必须成功的删除
      4. 回调
     */
    //2. 删除带有子节点的节点
    client.delete().deletingChildrenIfNeeded().forPath("/app4");
  }

  @Test
  public void testDelete3() throws Exception {
    /*
      删除节点
      1. 删除单个节点
      2. 删除带有子节点的节点
      3. 必须成功的删除
      4. 回调
     */
    //3. 必须成功的删除
    client.delete().guaranteed().forPath("/app2");
  }

  @Test
  public void testDelete4() throws Exception {
    /*
     删除节点
     1. 删除单个节点
     2. 删除带有子节点的节点
     3. 必须成功的删除
     4. 回调
    */
    // 4. 回调
    client
        .delete()
        .guaranteed()
        .inBackground(
            new BackgroundCallback() {
              @Override
              public void processResult(CuratorFramework client, CuratorEvent event)
                  throws Exception {
                System.out.println("deleted");
                System.out.println(event);
              }
            })
        .forPath("/app1");
  }
}
