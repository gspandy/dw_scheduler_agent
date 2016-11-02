package com.ajk.dw.scheduler.worker;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.SchedulerConfig;
import com.ajk.dw.scheduler.job.DWJob;
import com.ajk.dw.scheduler.utils.AgentServerHelper;

/**
 * @author Jason
 * Job 并发执行控制池, 
 * 
 * 本类使用 java.util.concurrent 作为 并发控制模块
 *  java.util.concurrent 包含许多线程安全、测试良好、高性能的并发构建块
 */
public class JobExecutorPool {

    private static final Logger LOG = LoggerFactory.getLogger(JobExecutorPool.class);
    private int slotCount = 20;
    public int thriftPort = 8900;
    
    // 使用 map 集合管理待执行的任务, ConcurrentHashMap<任务 id, 任务详细对象>
    private ConcurrentHashMap<Integer, JobExecutor> executorMap;
    
    // 执行服务
    private static ExecutorService executorService;

    /**
     * 初始化 
     * @param config
     */
    public JobExecutorPool(SchedulerConfig config) {
        this.slotCount = config.getWorkerSlotCount();
        this.thriftPort = config.getWorkerPort();
        executorService = AgentServerHelper.createThreadPool(slotCount, slotCount);
    }

    public void shutdown() {
        executorService.shutdown();
    }

    public boolean awaitTerminate(long timeout, TimeUnit unit)
            throws InterruptedException {
        return executorService.awaitTermination(timeout, unit);
    }

    void submitJob(JobExecutor executor, DWJob job) {
        executor.setJob(job);
        executorService.execute(executor);
        executorMap.put(job.getSchedulerId(), executor);
    }

    public List<Runnable> shutdownNow() {
        return executorService.shutdownNow();
    }

    public int executorSize() {
        return executorMap.size();
    }

    public boolean kill(Integer jobSchedulerId) {
        JobExecutor executor = executorMap.get(jobSchedulerId);
        if (executor == null) {
            return false;
        }
        //������ִ���б���ȥ��
        executorMap.remove(jobSchedulerId);
        return executor.killJob(jobSchedulerId);
    }

    @SuppressWarnings("unused")
    private class ExceptionHandler implements UncaughtExceptionHandler {

        public void uncaughtException(Thread t, Throwable e) {
            LOG.error(e.getMessage(), e);
        }

    }

    public ConcurrentHashMap<Integer, JobExecutor> getExecutorMap() {
        return executorMap;
    }

    public void setExecutorMap(ConcurrentHashMap<Integer, JobExecutor> executorMap) {
        this.executorMap = executorMap;
    }
}
