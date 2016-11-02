package com.ajk.dw.scheduler.job;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.JobState;
import com.ajk.dw.scheduler.quarz.QuartzScheduler;

/**
 * 监控 job 在 db 的变化做出处理
 * @author Jason
 */
public class JobMonitorFromDB implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(JobMonitorFromDB.class);
    private final ConcurrentHashMap<Integer, DWJob> executorJob ;
    private final JobDBPool jobDBPool;
    private QuartzScheduler quartzJob;
    private volatile boolean running;

    public JobMonitorFromDB(ConcurrentHashMap<Integer, DWJob> executorJob, 
            JobDBPool jobDBPool,
            QuartzScheduler quartzJob) {
        this.executorJob = executorJob;
        this.jobDBPool = jobDBPool;
        this.quartzJob = quartzJob;
    }

    public void run() {
        running=true;
        while (running&&!Thread.currentThread().isInterrupted()) {
            List<DWJob> jobList = jobDBPool.getJobs();
            for(DWJob job : jobList){
                //System.out.println("新增调度-----"+job.getJobName());
                job.setRunCount(0);
                if(job.getTaskId()==1){
                    quartzJob.addschduler(job);
                }else{
                    //把手动调度的job(未调度)放入执行队列中去
                    if(job.getJobState()==0){
                        setJobState(job,JobState.JOB_WAITING_SIGNAL);
                        int executeId =jobDBPool.createExcuteLog(job);
                        executorJob.put(executeId, job);
                        //生成日志放入job运行过程日志表里
                        jobDBPool.SchedulerExcuteLog(job);
                        LOG.info(job.getConfigId() + "-job put into excutorJob");
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

    private void setJobState(DWJob job, JobState state) {
        job.setJobState(state.getCode());
    }

    public void stop(){
        if(LOG.isInfoEnabled()){
            LOG.info("job JobMonitorFromDB stop");
        }
        running=false;
    }

    public QuartzScheduler getQuartzJob() {
        return quartzJob;
    }

    public void setQuartzJob(QuartzScheduler quartzJob) {
        this.quartzJob = quartzJob;
    }

    public JobDBPool getJobDBPool() {
        return jobDBPool;
    }

}
