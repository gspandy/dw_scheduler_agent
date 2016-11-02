package com.ajk.dw.scheduler;

import org.apache.log4j.xml.DOMConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.ajk.dw.scheduler.common.SchedulerConfig;
import com.ajk.dw.scheduler.common.SchedulerConfigFactory;
import com.ajk.dw.scheduler.common.SchedulerResourceFactory;
import com.ajk.dw.scheduler.common.SchedulerResources;
import com.ajk.dw.scheduler.common.SchdulerWorkerFactory;
import com.ajk.dw.scheduler.job.JobDBPool;
import com.ajk.dw.scheduler.utils.SchedulerUtils;
import com.ajk.dw.scheduler.worker.Worker;
import com.ajk.dw.scheduler.zookeepercli.CuartorOperator;

public class StartScheduler {
    private static final Logger LOG=LoggerFactory.getLogger(StartScheduler.class);

    public static void main(String[] args) {
 
        // 通过 SchedulerConfigFactory 工厂读取配置文件
        SchedulerConfig config = SchedulerConfigFactory.getDroneConf();

        // 创建 Scheduler 数据源连接对象
        final SchedulerResources resources = SchedulerResourceFactory.createResources(config);

        // 根据配置, 初始化 zookeeper 连接, 并使用 curatorOperator 操作 Scheduler 在 zookeeper 中的配置
        CuartorOperator curatorOperator =  new CuartorOperator(config);

        // 在 zookeeper 注册 server 节点
        if(!curatorOperator.createSchedulerServerNode()){
            //如果注册调度server失败就退出程序
            LOG.error("curatorOperator.createSchedulerServerNode():create Scheduler Server Node false,Exit startScheduler!!!");
            return;
        }

        //更新调度状态,并记录下来, 调度等待,Quarz未启动
        updateSchedulerStatus(2,3,resources);

        // 如果获取到互斥锁，就启动 scheduler agent，如果没有获取到就一直阻塞在这
        if(curatorOperator.getMutexLock()){
            LOG.info("互斥锁通过.......");
            // 根据 SchedulerConf 和 SchedulerResources 创建调度 SchdulerWorker
            final Worker worker = SchdulerWorkerFactory.createWorker(config, resources);
            try {
                LOG.info("worker 启动");
                // 开启 调度控制, 并开启 rpc 端口 
                worker.start();
            } catch (Exception e) {
                LOG.error(e.getMessage(),e);
                close(resources, worker);
                curatorOperator.releaseLock();
                return;
            }

            LOG.info("ShutdownHook.......");
            ShutdownHook shutdownHook = new ShutdownHook(resources, worker, curatorOperator); 
            /**
             * Runtime 获取 Java 运行时的环境
             * 当本程序在正常退出时, 增加一个关闭钩子
             *   在执行完指定的方法后, 再退出
             */
            Runtime.getRuntime().addShutdownHook(shutdownHook);

            LOG.info("quarz.......");
            //调度成功运行,quarz成功
            updateSchedulerStatus(1,1,resources);
        }
        LOG.info("调度成功启动......");
    }
    
    private static void close(SchedulerResources resources, Worker worker) {
        worker.close();
        SchedulerUtils.close(resources);
    }


    /**
     * 记录调度启动、服务状态
     * 1-调度正在运行，2-调度等待,3-调度未启动或关闭
     * @param status
     */
    private static void updateSchedulerStatus(int status,int quarzStatus,SchedulerResources resources){
        JobDBPool jobDBPool = new JobDBPool(resources.getDataSource());
        jobDBPool.recordSchedulerStatus(status,quarzStatus);
    }


    /**
     * 程序退出前, 指定的方法
     * @author Jason
     */
    public static class ShutdownHook extends Thread {
        private final SchedulerResources resources;
        private final Worker worker;
        private final CuartorOperator curatorOperator;
        
        public ShutdownHook(SchedulerResources resources, Worker worker,CuartorOperator curatorOperator) {
            this.resources = resources;
            this.worker = worker;
            this.curatorOperator = curatorOperator;
        }

        public void run() {
            LOG.info("关闭调度系统进程启动");
            //调度关闭,quarz也关闭
            updateSchedulerStatus(3,3,resources);
            worker.close();
            SchedulerUtils.close(resources);
            curatorOperator.releaseLock();
            LOG.info("关闭调度系统结束");
        }
    }
}
