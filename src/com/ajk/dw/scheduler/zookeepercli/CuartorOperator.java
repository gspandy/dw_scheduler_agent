package com.ajk.dw.scheduler.zookeepercli;

import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.SchedulerConfig;
import com.ajk.dw.scheduler.utils.IPUtils;


/**
 * @author Jason
 * scheduler 使用 zookeeper 作为锁
 * 
 * 本类使用 Curator 作为与 zookeeper 通讯的客户端
 *   Curator是Netflix公司开源的一个Zookeeper客户端，与Zookeeper提供的原生客户端相比，Curator的抽象层次更高，简化了Zookeeper客户端的开发量
 */
public class CuartorOperator {
    private static final Logger LOG = LoggerFactory.getLogger(CuartorOperator.class);
    
    // zookeeper 通讯客户端
    private CuratorFramework client;
    
    // zookeeper 连接对象
    private final String zookeeperConnection;
    
    // 调度程序锁路径
    private final String schedulerLocksPath;
    
    // 调度服务器的路径
    private final String schedulerServersPath;
    
    private InterProcessSemaphoreMutex lock;

    public CuartorOperator(SchedulerConfig config){
        zookeeperConnection = config.getZookeeperHosts();
        schedulerLocksPath = config.getSchedulerLockPath();
        schedulerServersPath = config.getSchedulerServerPath();
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        client = CuratorFrameworkFactory.newClient(zookeeperConnection, retryPolicy);
        client.start();
    }
    
    /**
     * 获取分布式互斥锁
     * @return
     */
    public boolean getMutexLock(){
        lock = new InterProcessSemaphoreMutex(client,schedulerLocksPath);
        try {
            LOG.info("getMutexLock:正在获取锁资源......");
            return lock.acquire(Long.MAX_VALUE, TimeUnit.DAYS);
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            return false;
        }
    }
    
    /*
     * 释放锁资源
     */
    public void releaseLock(){
        try {
            LOG.info("releaseLock:"+lock.isAcquiredInThisProcess());
            lock.release();
            LOG.info("releaseLock:成功释放锁资源");
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
        }finally{
            client.close();
        }
    }
    
    /**
     * 在Zookeeper目录/dw_scheduler/scheduler_servers 创建启动该调度server节点
     */
    public boolean createSchedulerServerNode(){
        String serverIp = IPUtils.getFirstNoLoopbackIP4Address();
        String serverHost = IPUtils.getHostName();
        try {
            createEphemeral(client,schedulerServersPath + "/" + serverHost + "," + serverIp,(serverHost + "," + serverIp).getBytes());
            return true;
        } catch (Exception e) {
            e.printStackTrace();
            LOG.error(e.getMessage(), e);
            return false;
        }
    }
    
    public  void create(CuratorFramework client, String path, byte[] payload) throws Exception {
         // this will create the given ZNode with the given data
         client.create().forPath(path, payload);
     }

     public  void createEphemeral(CuratorFramework client, String path, byte[] payload) throws Exception {
         // this will create the given EPHEMERAL ZNode with the given data
         client.create().withMode(CreateMode.EPHEMERAL).forPath(path, payload);
     }

     public  String createEphemeralSequential(CuratorFramework client, String path, byte[] payload) throws Exception {
         // this will create the given EPHEMERAL-SEQUENTIAL ZNode with the given
         // data using Curator protection.
         return client.create().withProtection().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(path, payload);
     }

     public  void setData(CuratorFramework client, String path, byte[] payload) throws Exception {
         // set data for the given node
         client.setData().forPath(path, payload);
     }

     public  void setDataAsync(CuratorFramework client, String path, byte[] payload) throws Exception {
         // this is one method of getting event/async notifications
         CuratorListener listener = new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework client, CuratorEvent event)
                    throws Exception {
                // TODO Auto-generated method stub
                
            }
         };
         client.getCuratorListenable().addListener(listener);
         // set data for the given node asynchronously. The completion
         // notification
         // is done via the CuratorListener.
         client.setData().inBackground().forPath(path, payload);
     }

     public  void setDataAsyncWithCallback(CuratorFramework client, BackgroundCallback callback, String path, byte[] payload) throws Exception {
         // this is another method of getting notification of an async completion
         client.setData().inBackground(callback).forPath(path, payload);
     }

     public  void delete(CuratorFramework client, String path) throws Exception {
         // delete the given node
         client.delete().forPath(path);
     }

     public  void guaranteedDelete(CuratorFramework client, String path) throws Exception {
         // delete the given node and guarantee that it completes
         client.delete().guaranteed().forPath(path);
     }

     public  List<String> watchedGetChildren(CuratorFramework client, String path) throws Exception {
         /**
          * Get children and set a watcher on the node. The watcher notification
          * will come through the CuratorListener (see setDataAsync() above).
          */
         return client.getChildren().watched().forPath(path);
     }

     public  List<String> watchedGetChildren(CuratorFramework client, String path, Watcher watcher) throws Exception {
         /**
          * Get children and set the given watcher on the node.
          */
         return client.getChildren().usingWatcher(watcher).forPath(path);
     }
}