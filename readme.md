####基于zookeeper实现的分布式工具  

##### com.zookeeperutils.DistributeID   
    
    分布式ID生成器。
##### com.zookeeperutils.DistributedLock  
    
    分布式锁。  
    对于读请求：共享读锁，多个读操作可以一块执行。  
    对于写请求：独占写锁，只能单个执行。