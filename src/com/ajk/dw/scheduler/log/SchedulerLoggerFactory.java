package com.ajk.dw.scheduler.log;

public interface SchedulerLoggerFactory {

    SchedulerLogger createRunnerLogger(int droneId, int taskId, int jobId,String command);
}
