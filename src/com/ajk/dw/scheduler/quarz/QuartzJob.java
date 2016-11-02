
package com.ajk.dw.scheduler.quarz;

import java.util.Date;

import org.quartz.Job;

import com.ajk.dw.scheduler.common.JobState;
import com.ajk.dw.scheduler.common.SchdulerWorkerFactory;
import com.ajk.dw.scheduler.job.DWJob;
import com.ajk.dw.scheduler.job.JobDBPool;

import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QuartzJob implements Job {
	
	private static final Logger LOG = LoggerFactory.getLogger(QuartzJob.class);
	
	public QuartzJob(){
		
		super();
		
	}
	
    public void execute(JobExecutionContext context)throws JobExecutionException {
        // 这里实现业务逻辑
        DWJob job = (DWJob)context.getJobDetail().getJobDataMap().get("job");
        
        Date nextFireTime=context.getTrigger().getNextFireTime();
        
        job.setNextFireTime(nextFireTime);
        
        job.setSubmitTime(System.currentTimeMillis());
        
        LOG.info("job executing........" + job.getJobName()+";next_run_time:"+nextFireTime);
        
		try {
			
				JobDBPool jobDBPool = (JobDBPool) context.getScheduler().getContext().get("jobDBPool");
				
				setJobState(job,JobState.JOB_WAITING_SIGNAL);
				
				int executeId =jobDBPool.createExcuteLog(job);
				
				//生成日志放入job运行过程日志表里
				
				jobDBPool.SchedulerExcuteLog(job);
				
				SchdulerWorkerFactory.executorJob.put(executeId, job);
			
		} catch (SchedulerException e) {
			
			// TODO Auto-generated catch block
			LOG.error("QuartzJob 运行失败,config_id:"+job.getConfigId()+"," +
					"job_id:"+job.getJobId()+",error msg:"+e.getStackTrace());
			
		}
    }
    
    private void setJobState(DWJob job, JobState state) {
        job.setJobState(state.getCode());
    }

}
