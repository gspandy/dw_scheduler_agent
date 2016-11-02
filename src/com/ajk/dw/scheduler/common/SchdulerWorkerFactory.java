//CHECKSTYLE OFF: ClassDataAbstractionCoupling
package com.ajk.dw.scheduler.common;

import java.util.concurrent.ConcurrentHashMap;

import com.ajk.dw.scheduler.common.exception.SchedulerException;
import com.ajk.dw.scheduler.job.DWJob;
import com.ajk.dw.scheduler.job.JobDBPool;
import com.ajk.dw.scheduler.job.JobMonitorFromDB;
import com.ajk.dw.scheduler.job.JobReRunFromDB;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactory;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactoryBuilder;
import com.ajk.dw.scheduler.quarz.QuartzScheduler;
import com.ajk.dw.scheduler.worker.JobExecutorPool;
import com.ajk.dw.scheduler.worker.JobSubmitter;
import com.ajk.dw.scheduler.worker.Worker;

/**
 * @author Jason
 * 
 */
public class SchdulerWorkerFactory {
    
    // 执行 jobs map 集合 
    public static ConcurrentHashMap<Integer, DWJob> executorJob = new ConcurrentHashMap<Integer, DWJob>();
    
    public static Worker createWorker(SchedulerConfig config,
            SchedulerResources resources) {
        SchedulerLoggerFactory loggerFactory = getSchedulerLoggerFactory(config);

        // 创建 Job 并发控制池
        JobExecutorPool executorPool = new JobExecutorPool(config);

        // 初始化 job 提交对象
        JobSubmitter submitter = new JobSubmitter(executorJob, executorPool, 
                resources.getDataSource(),loggerFactory);

        // 初始化 job db 池
        JobDBPool jobDBPool = new JobDBPool(resources.getDataSource());

        // Scheduler 定时任务控制, 新增、修改、重跑控制
        QuartzScheduler quartzJob = new QuartzScheduler(jobDBPool);

        // 监控 job 在 db 的变化做出处理
        JobMonitorFromDB jobMonitorFromDB = new JobMonitorFromDB(executorJob, jobDBPool, quartzJob);
        
        // job 重跑处理
        JobReRunFromDB jobReRunFromDB = new JobReRunFromDB(executorJob,jobDBPool);
        
        // work, 调度流程控制对象
        Worker worker = new Worker(executorPool, submitter,
                jobMonitorFromDB,jobReRunFromDB,loggerFactory) ;
        return worker;
    }
    
    private static SchedulerLoggerFactory getSchedulerLoggerFactory(SchedulerConfig config) {
        String builderClassName = config.getWorkerLoggerBuilderClassName();
        try {
            SchedulerLoggerFactoryBuilder builder = (SchedulerLoggerFactoryBuilder) Class
                    .forName(builderClassName).newInstance();
            return builder.createLoggerFactory(config);
        } catch (Exception e) {
            throw new SchedulerException(e);
        }
    }

}
