package com.ajk.dw.scheduler.job;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.JobState;

/**
 * @author Jason
 * job 重跑处理
 */
public class JobReRunFromDB implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(JobReRunFromDB.class);
    private final ConcurrentHashMap<Integer, DWJob> executorJob ;
    private final JobDBPool jobDBPool;
    private volatile boolean running;
    
    public JobReRunFromDB(ConcurrentHashMap<Integer, DWJob> executorJob, JobDBPool jobDBPool) {
        this.executorJob = executorJob;
        this.jobDBPool = jobDBPool;
    }
    
    public void run() {
        running=true;
        while (running&&!Thread.currentThread().isInterrupted()) {
            List<DWJob> jobList = jobDBPool.reRunErrorJobs();
            for(DWJob job : jobList){
                LOG.info("系统重跑重跑出错调度-----"+job.getJobName());
                //把重跑的job放入执行队列中去
                if(job.getJobState()==0){
                    setJobState(job,JobState.JOB_WAITING_SIGNAL);
                    int executeId =jobDBPool.createExcuteLog(job);
                    executorJob.put(executeId, job);
                    //生成日志放入job运行过程日志表里
                    jobDBPool.SchedulerExcuteLog(job);
                    LOG.info(job.getConfigId() + "JobReRunFromDB-job put into excutorJob");
                }
            }
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                LOG.error("JobReRunFromDB Thread.sleep Exception:"+e.getStackTrace());
            }
        }    
        
    }
    
    private void setJobState(DWJob job, JobState state) {
        job.setJobState(state.getCode());
    }
    
    public void stop(){
        if(LOG.isInfoEnabled()){
            LOG.info("job JobReRunFromDB stop");
        }
        running=false;
    }
}
