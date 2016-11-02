package com.ajk.dw.scheduler.worker;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.job.DWJob;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactory;

import com.ajk.dw.scheduler.common.SchedulerConfigFactory;

/**
 * @author Jason
 * 执行脚本实体方法
 */
public class ScriptJobRunner extends AbstractJobRunner {
    private static final Logger LOG = LoggerFactory
            .getLogger(ScriptJobRunner.class);
    private static final String SCRIPT_PATH = SchedulerConfigFactory.getDroneConf().getDwScriptPath() + "/scheduler_runner.py";
    protected AtomicReference<Process> processRef = new AtomicReference<Process>();

    public ScriptJobRunner(SchedulerLoggerFactory jobLoggerFactory) {
        super(jobLoggerFactory);
    }

    @Override
    protected int doRun(DWJob job) {
        String command = job.getCommand();
        Runtime runtime = Runtime.getRuntime();
        LOG.info("About to run " + command);
        if(command==null || command.trim().length()==0){
        	return 0;
        }
        Process process;
        try {
            process = runtime.exec("python " + SCRIPT_PATH + " "+ command);
        } catch (IOException e) {
            LOG.error("task execute error " + e.getStackTrace());
            return -1;
        }

        processRef.set(process);
        LoggerThreadHolder loggerHolder = LoggerThreadHolder.getLogger(
                jobLoggerFactory, job, process);
        loggerHolder.start();

        try {
            process.waitFor();
        } catch (InterruptedException e) {
            Thread.interrupted();
            doKill();
            try {
                process.waitFor();
            } catch (InterruptedException e1) {
                process.destroy();
            }

        }

        int exitCode = process.exitValue();

        if (LOG.isInfoEnabled()) {
            LOG.info(job.getExcuteId() + " execute finished, exicode: "
                    + exitCode);
        }
        loggerHolder.finish();


        processRef.set(null);
        return exitCode;
    }

    @Override
    protected boolean doKill() {
        Process process = processRef.get();
        if (process == null) {
            return false;
        }
        LOG.info("kill command received, " + process.getInputStream().toString());
        try {
            OutputStream os = process.getOutputStream();
            os.write("kill\n".getBytes());
            os.flush();
            return true;
        } catch (IOException e) {
            LOG.error("task send kill command error " + e.getMessage(), e);
            process.destroy();
            return false;
        }

    }

}