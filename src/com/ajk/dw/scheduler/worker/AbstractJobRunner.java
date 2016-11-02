package com.ajk.dw.scheduler.worker;

import java.util.concurrent.atomic.AtomicReference;

import com.ajk.dw.scheduler.job.DWJob;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactory;

public abstract class AbstractJobRunner implements JobRunner {
    protected final SchedulerLoggerFactory jobLoggerFactory;

    protected AtomicReference<Integer> jobExcuteIdIdRef = new AtomicReference<Integer>();

    AbstractJobRunner(SchedulerLoggerFactory jobLoggerFactory) {
        this.jobLoggerFactory = jobLoggerFactory;
    }

    public synchronized int runJob(DWJob job) {
        jobExcuteIdIdRef.set(job.getSchedulerId());
        try {
            return doRun(job);
        } finally {
            jobExcuteIdIdRef.set(null);
        }
    }

    protected abstract int doRun(DWJob job);

    protected abstract boolean doKill();

    public boolean kill(Integer jobSchedulerId) {
        if (jobSchedulerId.equals(jobExcuteIdIdRef.get())) {
            return doKill();
        } else {
            return false;
        }
    }

}
