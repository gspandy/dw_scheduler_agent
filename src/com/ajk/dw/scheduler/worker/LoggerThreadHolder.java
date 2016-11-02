package com.ajk.dw.scheduler.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.job.DWJob;
import com.ajk.dw.scheduler.log.SchedulerLogger;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactory;

class LoggerThreadHolder {

    private static final Logger LOG = LoggerFactory
            .getLogger(LoggerThreadHolder.class);

    private final SchedulerLogger schedulerLogger;
    private final Thread stdThread;
    private final Thread errThread;

    LoggerThreadHolder(SchedulerLogger schedulerLogger, Thread stdThread,
            Thread errThread) {
        this.schedulerLogger = schedulerLogger;
        this.stdThread = stdThread;
        this.errThread = errThread;
    }

    static LoggerThreadHolder getLogger(SchedulerLoggerFactory jobLoggerFactory,
            DWJob job, Process process) {
        int excuteId = job.getExcuteId();
        String command = job.getCommand();
        SchedulerLogger runnerLogger = jobLoggerFactory.createRunnerLogger(excuteId,
                job.getTaskId(), job.getJobId(), job.getCommand());

        runnerLogger.log("About to run " + command);

        Thread stdThread = new StreamLogger("ExecJobStdoutLoggerThread-"
                + command, process.getInputStream(), runnerLogger, true);
        Thread errThread = new StreamLogger("ExecJobStderrLoggerThread-"
                + command, process.getErrorStream(), runnerLogger, false);
        return new LoggerThreadHolder(runnerLogger, stdThread, errThread);
    }

    void start() {
        stdThread.start();
        errThread.start();
    }

    void finish() {
        try {
            stdThread.join();
            errThread.join();
        } catch (InterruptedException e) {
            LOG.warn("Wait log thread finish interrupted");
        } finally {
            schedulerLogger.finish();
        }
    }

}
