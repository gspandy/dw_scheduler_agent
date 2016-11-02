package com.ajk.dw.scheduler.worker;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.JobState;
import com.ajk.dw.scheduler.common.exception.SchedulerException;
import com.ajk.dw.scheduler.job.DWJob;
import com.ajk.dw.scheduler.job.JobDBPool;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactory;
import com.ajk.dw.scheduler.utils.SchedulerUtils;

/**
 * @author Jason
 * 运行执行, 每一个调度方法
 */
public class JobExecutor implements Runnable {
    private static final Logger LOG = LoggerFactory
            .getLogger(JobExecutor.class);
    private final JobRunner runner;

    private final AtomicReference<DWJob> jobRef = new AtomicReference<DWJob>();

    private final AtomicBoolean killed = new AtomicBoolean(false);
    private final JobDBPool jobDBPool;
    private ConcurrentHashMap<Integer, JobExecutor> executorMap;

    JobExecutor(DataSource dataSource,SchedulerLoggerFactory loggerFactory,
            ConcurrentHashMap<Integer, JobExecutor> executorMap) {
        this.runner = new ScriptJobRunner(loggerFactory);
        this.jobDBPool = new JobDBPool(dataSource);
        this.executorMap = executorMap;
    }

    public void setJob(DWJob job) {
        if (!jobRef.compareAndSet(null, job)) {
            throw new SchedulerException("set job called when current job not null");
        }
    }

    public DWJob getJob() {
        return jobRef.get();
    }

    @SuppressWarnings("unused")
    public void run() {
        DWJob job = jobRef.get();
        System.out.println("JobExecutor------:"+job.getJobName());
        try {
            if (job == null) {
                LOG.warn("executor run with null job ref");
                return;
            }
            Thread.currentThread().setName("threadName-JobExecutor" + job.getExcuteId() + " - "+ job.getJobId() + " - "
                            + job.getCommand());

            job.setExecuteStartTime(System.currentTimeMillis());
            setJobState(job, JobState.EXECUTING);
            int exitCode = runner.runJob(job);
            job.setExecuteEndTime(System.currentTimeMillis());
            job.setExitCode(exitCode);
            if (exitCode == 0) {
                setJobState(job, JobState.SUCCESS);
            } else if (killed.get()) {
                setJobState(job, JobState.KILLED);
            } else {
                setJobState(job, JobState.FAIL);
            }
        } finally {
            /**
             * 信号文件生成规则问题点
            if(job.getJobState()== JobState.SUCCESS.getCode() ||
                    //(job.getRunType()==1 && job.getRunCount()>=job.getRetry() && job.getJobState()!= JobState.KILLED.getCode())){
                //如果是点击单独运行的或者重跑运行次数大于等于设定重跑次数就生成信号文件
                    (job.getRetry() > 0 && job.getRunCount()>=job.getRetry() && job.getJobState()!= JobState.KILLED.getCode())){
                //如果运行成功，或者是设置了重跑
                SchedulerUtils.generateSignalFile(job.getJobName());
                //生成信号插入数据库
                jobDBPool.generateSignal(job);
            }
            */
            // 跑成功生成信号文件, 否则不生成
            if(job.getJobState() == JobState.SUCCESS.getCode() 
             ) {

                SchedulerUtils.generateSignalFile(job.getJobName());
                jobDBPool.generateSignal(job);

                System.out.println("--------------------------------------------------------------5");

            }


            executorMap.remove(job.getSchedulerId());
            LOG.info("JobExecutor-" + job.getTaskId() + "-"
                            + job.getJobId() + " idle,executorMap:"+executorMap.size());
            clearState();
        }
    }

    private void clearState() {
        jobRef.set(null);
        killed.set(false);
    }

    private void setJobState(DWJob job, JobState state) {
        job.setJobState(state.getCode());
        jobDBPool.updateExecuteFinish(job);
        jobDBPool.SchedulerExcuteLog(job);
    }

    public boolean killJob(Integer jobSchedulerId) {
        killed.set(true);
        return runner.kill(jobSchedulerId);
    }
}
