package com.zookeeperutils;

import org.apache.curator.CuratorZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZKUtil;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

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
    private static String parentPath = "/locks";
    private static String readLock="READ-";
    private static String writeLock="WRITE-";
    private static ThreadLocal<CuratorFramework> threadLocal = new ThreadLocal();

    private CuratorFramework getClient() throws Exception {
        if (threadLocal.get()!=null){
            throw new Exception("不可重复创建锁");
        }

        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(hosts)
                .sessionTimeoutMs(3000)
                .retryPolicy(new ExponentialBackoffRetry(10000,3))
                .build();
        client.start();

        Stat stat = client.checkExists().forPath(parentPath);
        //如果父节点不存在则先创建父节点
        if (Objects.isNull(stat)){
            client.create().withMode(CreateMode.PERSISTENT)
                    .forPath(parentPath);
        }
        //每个会话与线程绑定
        threadLocal.set(client);
        return client;
    }

    public DistributedLock() throws Exception {
    }


    public void acquireReadLock() throws Exception {
        CuratorFramework client = getClient();
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        String jobName = ZKPaths.makePath(parentPath,readLock);
        //添加一个临时写写节点放到树的末尾
        client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(jobName);
        //获取子节点
        List <String> list =  client.getChildren().forPath(parentPath);
        boolean flag = true;
        //从后往前找是否能获取到锁，如果前面存在写锁，那么阻塞并在前面节点注册监听等待
        Collections.reverse(list);

        String lockName = null;

        for (String value : list) {
            if (value.contains(writeLock)){
                flag = false;
                lockName = value;
                break;
            }
        }
        //阻塞等待
        if (!flag){
            NodeCache nodeCache = new NodeCache(client, lockName);
            nodeCache.start();
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                public void nodeChanged() throws Exception {
                    countDownLatch.countDown();
                }
            });
            countDownLatch.await();
        }
    }
    public void acquireWriteLock() throws Exception {
        CuratorFramework client = getClient();
        final String jobName = ZKPaths.makePath(parentPath,writeLock);
        //创建子节点
        String path = client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(jobName);
        //获取子节点，在最后一个子节点上注册监听
        List<String> list = client.getChildren().forPath(parentPath);
        Collections.reverse(list);

        String lockName = null;
        for (String name:list) {
            if (path.contains(name)){
                break;
            }else{
                lockName = name;
            }
        }

        if (lockName != null) {
            lockName = ZKPaths.makePath(parentPath,lockName);
            final CountDownLatch countDownLatch = new CountDownLatch(1);
            NodeCache nodeCache = new NodeCache(client, lockName);
            nodeCache.start(true);
            final String finalLockName = lockName;
            nodeCache.getListenable().addListener(new NodeCacheListener() {
                public void nodeChanged() throws Exception {
                    countDownLatch.countDown();
                }
            });
            countDownLatch.await();
        }

    }

    public void release(){//删除临时节点
        CuratorFramework client = threadLocal.get();
        if (client !=null){
            client.close();
        }
    }

    public static void main(String args[]) throws Exception {
        final DistributedLock distributedLock = new DistributedLock();
        final CountDownLatch countDownLatch = new CountDownLatch(10);
        distributedLock.acquireWriteLock();
        Thread.sleep(5000);
        distributedLock.release();
        for (int i =0;i<10;i++){
            new Thread(new Runnable() {
                public void run() {
                    countDownLatch.countDown();
                    try {
                        distributedLock.acquireReadLock();
                        System.out.println("获取到锁");
                        Thread.sleep(100);
                        System.out.println("释放掉锁");
                    } catch (Exception e) {
                        System.out.println(e.getMessage());
                    }
                    finally {
                        distributedLock.release();
                    }
                }
            }).start();
        }
    }
}
