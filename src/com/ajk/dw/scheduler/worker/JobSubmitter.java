package com.ajk.dw.scheduler.worker;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.JobState;
import com.ajk.dw.scheduler.generated.Job;
import com.ajk.dw.scheduler.job.DWJob;
import com.ajk.dw.scheduler.job.JobDBPool;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactory;
import com.ajk.dw.scheduler.utils.SchedulerUtils;


public class JobSubmitter implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(JobSubmitter.class);
    private final JobExecutorPool executorPool;
    private final DataSource dataSource;
    private final SchedulerLoggerFactory loggerFactory;
    
    private ConcurrentHashMap<Integer, DWJob> executorJob = new ConcurrentHashMap<Integer, DWJob>();
    private final ConcurrentHashMap<Integer, JobExecutor> executorMap = new ConcurrentHashMap<Integer, JobExecutor>();    
    
    private volatile boolean running;
    private final JobDBPool jobDBPool;

    public JobSubmitter(
            // job 集合
            ConcurrentHashMap<Integer, DWJob> executorJob,
            // job 并发控制对象
            JobExecutorPool executorPool,
            // job 数据源
            DataSource dataSource,
            SchedulerLoggerFactory loggerFactory) {
        
        this.executorJob = executorJob;
        this.executorPool = executorPool;
        this.dataSource = dataSource;
        this.loggerFactory = loggerFactory;
        this.executorPool.setExecutorMap(executorMap);
        this.jobDBPool = new JobDBPool(dataSource);
    }

    public void directSubmit(DWJob job){
        executorPool.submitJob(new JobExecutor(dataSource,loggerFactory,executorMap), job);
    }

    public void run() {
        running=true;
        // 判断当前 线程是否被打断了
        while (running && !Thread.currentThread().isInterrupted()) {

            Iterator<Integer> iterator = executorJob.keySet().iterator();

            while(iterator.hasNext()) {

                DWJob job = executorJob.get(iterator.next());
                if (job == null) {
                    continue;
                }else if(executorMap.containsKey(job.getSchedulerId())){
                    //如果正在运行的job中已经存在了，这时候把job状态置为重复调度
                    setJobState(job, JobState.DUPSUB);
                    executorJob.remove(job.getExcuteId());
                    LOG.info("excute_id \"" +job.getExcuteId()+",\"task_id:"+ job.getTaskId() + "\"," +
                            " signal Files: " + job.getDependent_jobs()
                            + ",Duplicate submissions");
                    continue;
                }

                //if (SchedulerUtils.haveSignalFile(job.getDependent_jobs())
                if (    // job 依赖为空
                        (SchedulerUtils.isEmpty(job.getDependent_jobs()) 
                        //Job 依赖都生成了
                        || jobDBPool.shouldRunJob(job.getJobId()))
                        // 是否存在依赖的信号文件
                        || SchedulerUtils.haveSignalFile(job.getDependent_jobs()) 
                        // 运行是的其他模式的 job
                        || (job.getTaskId()!=1 && job.getRunType()==3)
                  ) {
                     // 初始化一个 job 执行分装
                     JobExecutor executor = new JobExecutor(dataSource,loggerFactory,executorMap);
                     // 提交给 job 线程池
                     executorPool.submitJob(executor, job);
                     executorJob.remove(job.getExcuteId());
                     LOG.info("excute_id \"" +job.getExcuteId()+",\"task_id:"+ job.getTaskId() + "\"," +
                                " signal Files: " + job.getDependent_jobs()
                                + ",submitJob success");
                } else {
                    if (LOG.isWarnEnabled()) {
                        LOG.warn("excute_id \"" + job.getExcuteId()+",\"task_id:"+ job.getTaskId() + "\"," +
                                " signal Files: " + job.getDependent_jobs()
                                + ",signal Files not generate success");
                    }
                }
            }
            
            
            
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.error("JobMonitorFromDB Thread.sleep Exception:"+e.getStackTrace());
            }
        }
    }

    public List<Job> getPendingJobs(){
        List<Job> pendingJobs = new ArrayList<Job>();
        Iterator<Integer> iterator = executorJob.keySet().iterator();
        while(iterator.hasNext()) {
            DWJob job = executorJob.get(iterator.next());
            pendingJobs.add(job.convertToJob());
        }    
        return pendingJobs;
    }
    
    public boolean stopPendingJob(int excuteId){
        boolean flag =true;
        DWJob job;
        try {
            if(executorJob.containsKey(excuteId)){
                job = executorJob.get(excuteId);
                executorJob.remove(excuteId);
                setJobState(job, JobState.KILLED);
            }else{
                flag = false;
            }
            
        } catch (Exception e) {
            LOG.error("stopPendingJob()+,excuteId:"+excuteId+"",e.getStackTrace());
            flag = false;
        }
        return flag;
    }
    
    private void setJobState(DWJob job, JobState state) {
        job.setJobState(state.getCode());
        jobDBPool.updateExecuteFinish(job);
        jobDBPool.SchedulerExcuteLog(job);
    }
    
    public void stop(){
        if(LOG.isInfoEnabled()){
            LOG.info("job submitter stop");
        }
        running=false;
    }
}
