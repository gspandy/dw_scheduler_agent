package com.ajk.dw.scheduler.worker;

import com.ajk.dw.scheduler.job.DWJob;

/**
 * run job support class
 */
public interface JobRunner {
    /**
     *
     * @param job
     *            the job about to run
     * @return the exit code
     */
    int runJob(DWJob job);

    /**
     *
     * @param job
     * @return return true if success send kill command , which not means the
     *         job success killed
     */
    boolean kill(Integer jobDroneId);

}
