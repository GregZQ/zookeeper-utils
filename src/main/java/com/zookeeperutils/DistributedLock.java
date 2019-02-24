package com.zookeeperutils;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * @author GregZQ
 * @create 2019-02-24 19:39
 * @des: 分布式锁实现
 *
 *   场景：
 *    对于读操作，只能一个事务独占去访问。（排它锁）
 *    对于写操作，可以多个读事务同时访问。（共享锁）
 *
 *  设计要点：
 *    如何判断读操作写操作执行顺序。
 *
 *   设计流程：
 *     1。在zk的某个节点A下面创建临时子节点，获取所有子节点，并注册对于A节点的监听事件
 *       对于读操作（共享锁）：
 *          创建节点： READ-序号
 *       对于写操作（排他锁）：
 *          创建节点： WRITE-序号
 *    2. 当创建好节点后，进行判断：
 *        对于读操作：
 *          如果没有比自己小的子节点或者所有比自己需要小的节点都是读节点，则可以获取操作
 *          如果比自己小的节点中有写请求，就进入等待。
 *        对于写操作：
 *          如果自己不是序号最小的节点，就进行等待
 *    优化的地方：如果A节点下面变更会通知所有节点都唤醒进行操作，这显然是浪费的。
 *     因为对于写操作，如果前面全是读，则还是继续等待。
 *     对于读操作，如果前面全是写，也还是进入等待状态。
 *
 *    可以做如下优化：
 *       对于读请求：向比自己小的最后一个写节点注册watch监听。
 *       对于写请求：向比自己小的最后一个节点注册监听
 *
 *    还未完成。。。。。
 *
 */
public class DistributedLock {

    private static String hosts ="192.168.25.128:2181";
    private static CuratorFramework client;
    private static String parentPath = "/locks";
    private static String readLock="READ-";
    private static String writeLock="WRITE-";

    public DistributedLock() throws Exception {
        client = CuratorFrameworkFactory.builder()
                .connectString(hosts)
                .sessionTimeoutMs(3000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .build();

        client.start();

        Stat stat = client.checkExists().forPath(parentPath);

        if (Objects.isNull(stat)){
            client.create().withMode(CreateMode.PERSISTENT)
                    .forPath(parentPath);
        }

    }

    public void acquireReadLock() throws Exception {
        String jobName = ZKPaths.makePath(parentPath,readLock);
        List <String> list =  client.getChildren().forPath(parentPath);
        Collections.reverse(list);

        boolean flag = true;

        String lockName;

        for (String value : list) {
            if (value.contains(writeLock)){
                flag = false;
                lockName = value;
                break;
            }
        }
        if (!flag){

        }
    }

}
