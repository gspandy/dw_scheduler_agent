package com.ajk.dw.scheduler.worker;

import com.ajk.dw.scheduler.job.JobMonitorFromDB;
import com.ajk.dw.scheduler.quarz.QuartzScheduler;

/**
 * TODO Comment of CommandTaskThread
 * 
 * @author roland
 * 
 */
@SuppressWarnings("unused")
public class CommandTaskThread implements Runnable {
	
	private final JobExecutorPool executorPool;
    private final JobSubmitter submitter;
    private final JobMonitorFromDB jobMonitorFromDB;
    private final QuartzScheduler quartzJob;

	CommandTaskThread(JobExecutorPool executorPool, JobSubmitter submitter,
            JobMonitorFromDB jobMonitorFromDB,QuartzScheduler quartzJob) {
        this.executorPool = executorPool;
        this.submitter = submitter;
        this.jobMonitorFromDB = jobMonitorFromDB;
        this.quartzJob = quartzJob ;
	}

	public void run() {
		
	}

	private void killTask(Integer executeId) {
		
	}

}