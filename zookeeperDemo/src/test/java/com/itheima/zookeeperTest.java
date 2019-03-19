package com.itheima;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.Test;

public class zookeeperTest {
    /**
     * 创建节点
     */
    @Test
    public void createNode() throws Exception {
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181", new ExponentialBackoffRetry(3000, 3));
        //开启客户端操作
        client.start();
        //通过create来进行创建节点，并且需要指定节点类型
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath("/bbb", "helloworld".getBytes());
//        Thread.sleep(5000);
        client.close();
    }

    /**
     * 修改节点数据
     */

    @Test
    public void updateNode() throws Exception {
        //重试机制
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000,3);
        //创建客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient("node02:2181", retryPolicy);
        //开启客户端
        client.start();
        //修改节点数据
        client.setData().forPath("/aaa", "bbb".getBytes());
        client.close();
    }
    /**
     * 节点数据查询
     */
    @Test
    public void findNode() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("node03:2181", new ExponentialBackoffRetry(3000, 3));
        client.start();
        byte[] bytes = client.getData().forPath("/aaa");
        System.out.println(new String(bytes));
        client.close();
    }
    /**
     * watch机制
     */

    @Test
    public void watchNode() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181", new ExponentialBackoffRetry(3000, 3));
        client.start();
        //设置节点的cache
        TreeCache treeCache = new TreeCache(client, "/aaa");
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                TreeCacheEvent.Type type = treeCacheEvent.getType();
                if (null != data) {
                    switch (type) {
                        case CONNECTION_LOST:
                            System.out.println("连接中断");
                            break;
                        case NODE_ADDED:
                            System.out.println("添加数据："+new String(data.getData()));
                            break;
                        default:
                            break;
                    }
                }
            }
        });
        treeCache.start();
        Thread.sleep(500000000);
    }
}

