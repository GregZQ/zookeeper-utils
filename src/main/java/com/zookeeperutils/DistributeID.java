package com.zookeeperutils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;

/**
 * @author GregZQ
 * @create 2019-02-01 19:28
 * @des: 基于zookeeper实现的分布式id生成器，
 * 可以满足在分布式条件下生成一个全局唯一ID。
 */
public class DistributeID {

    private static String hosts ="192.168.25.128:2181";
    private static CuratorFramework client;
    private static String parentPath = "/dis-id";
    private static String nodeName="/jobs";
    static {
        client = CuratorFrameworkFactory.builder()
                .connectString(hosts)
                .sessionTimeoutMs(3000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .build();

        client.start();

        //判断一下父节点是否存在
        try {
            Stat stat = client.checkExists().forPath(parentPath);

            if (Objects.isNull(stat)){
                client.create().withMode(CreateMode.PERSISTENT)
                               .forPath(parentPath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static String generateID() throws Exception {
        String jobName = ZKPaths.makePath(parentPath,nodeName);
        String path = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(jobName);
        ZKPaths.getNodeFromPath(path);
        //创建完成之后删除，防止占用系统资源
        client.delete().forPath(path);
        return path;
    }

    public static void main(String args[]){
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        for (int i=0;i<100;i++){
            new Thread(new Runnable() {
                public void run() {
                    try {
                        countDownLatch.await();
                        System.out.println("ID为"+generateID());
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                }
            }).start();
        }
        countDownLatch.countDown();
    }

}
