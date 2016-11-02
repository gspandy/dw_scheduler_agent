package com.ajk.dw.scheduler.worker;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.exception.SchedulerException;
import com.ajk.dw.scheduler.job.JobMonitorFromDB;
import com.ajk.dw.scheduler.job.JobReRunFromDB;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactory;
import com.ajk.dw.scheduler.utils.SchedulerUtils;
import com.ajk.dw.scheduler.generated.Job;
import com.ajk.dw.scheduler.generated.TriggerBean;
import com.ajk.dw.scheduler.generated.WorkerService;
import com.ajk.dw.scheduler.generated.WorkerService.Processor;

public class Worker {

    //private static final int DEFAULT_TERMINAL_TIMEOUT_MINUTES = 10;
    private static final int DEFAULT_TERMINAL_TIMEOUT_SECONDS = 2;
    private static final Logger LOG = LoggerFactory.getLogger(Worker.class);
    /*private final ExecutorService daemonServiceExecutor = Executors
            .newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat(
                    "worker-daemon-pool-%d").build());*/
    private final ExecutorService daemonServiceExecutor = Executors.newFixedThreadPool(4);

    private final JobExecutorPool executorPool;
    private final JobSubmitter submitter;

    private volatile boolean start;
    private final AtomicBoolean running = new AtomicBoolean(false);
    private final SchedulerLoggerFactory loggerFactory;
    private final JobMonitorFromDB jobMonitorFromDB;
    private final JobReRunFromDB jobReRunFromDB;
    private RpcServiceRunner rpcRunner;

    public Worker(
            JobExecutorPool executorPool, 
            JobSubmitter submitter,
            JobMonitorFromDB jobMonitorFromDB,
            JobReRunFromDB jobReRunFromDB,
            SchedulerLoggerFactory loggerFactory ) {
        this.executorPool = executorPool;
        this.submitter = submitter;
        this.jobMonitorFromDB = jobMonitorFromDB;
        this.jobReRunFromDB = jobReRunFromDB;
        this.start = false;
        this.loggerFactory = loggerFactory;
    }

    public void start() throws Exception {
        try {
            start = true;
            running.set(true);
            //quartzJob.startSchedule();
            daemonServiceExecutor.execute(jobMonitorFromDB);
            daemonServiceExecutor.execute(submitter);
            daemonServiceExecutor.execute(jobReRunFromDB);
            //run thrift RPC
            rpcRunner = new RpcServiceRunner();
            rpcRunner.init();
            daemonServiceExecutor.execute(rpcRunner);
        } catch (SchedulerException e) {
            start = false;
            throw e;
        }

    }

    public void close() {
        if (!start) {
            // start fail, no need to close;
            executorPool.shutdownNow();
            daemonServiceExecutor.shutdownNow();
            return;
        }
        boolean terminated;
        submitter.stop();
        daemonServiceExecutor.shutdown();
        try {
            daemonServiceExecutor.awaitTermination(
                    DEFAULT_TERMINAL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e1) {
            LOG.warn("daemonServiceExecutor threads termination await interrupt");
        }
        start = false;
        running.set(false);

        executorPool.shutdown();
        rpcRunner.close();
        try {
            //关闭调度
            jobMonitorFromDB.getQuartzJob().shutdown();
        } catch (org.quartz.SchedulerException e1) {
            e1.printStackTrace();
            LOG.error("QuartzJob shutdown fail");
        }
        LOG.info("worker slot thread shutdown and wait for current task finish for 2 seconds");
        try {
            terminated = executorPool.awaitTerminate(
                    DEFAULT_TERMINAL_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOG.warn("executorPool threads termination await interrupt");
            terminated = false;
        }
        if (terminated) {
            LOG.info("worker slot thread success shutdown");
        } else {
            LOG.info("worker slot thread shutdown fail, force kill task");
            executorPool.shutdownNow();
        }
    }

    public SchedulerLoggerFactory getLoggerFactory() {
        return loggerFactory;
    }

    
    /**
     * 开启 RPC 端口
     * @author Jason
     */
    private class RpcServiceRunner implements Runnable {

        TServerSocket serverTransport;
        TServer server;

        void init() {
            try {
                serverTransport = new TServerSocket(executorPool.thriftPort);
                WorkerService.Processor<RpcService> process = new Processor<RpcService>(
                        new RpcService());

                TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory(
                        true, true);
                TThreadPoolServer.Args args = new TThreadPoolServer.Args(
                        serverTransport);
                args.processor(process);
                args.protocolFactory(protocolFactory);
                server = new TThreadPoolServer(args);
            } catch (Exception e) {
                throw new SchedulerException(e);
            }
        }

        void close() {
            server.stop();
            serverTransport.close();
        }

        @Override
        public void run() {
            server.serve();
        }
    }

    private class RpcService implements WorkerService.Iface {

        @Override
        public List<Job> getPendingJobs() throws TException {
            return submitter.getPendingJobs();
        }

        @Override
        public boolean kill(int schedulerId) throws TException {
            return executorPool.kill(schedulerId);
        }

        @Override
        public boolean stopPendingJob(int excuteId) throws TException {
            return submitter.stopPendingJob(excuteId);
        }

        @Override
        public boolean switchQuarz(int onOff) throws TException {
            boolean flag =  true;
            //1-恢复，2-暂停,3-关闭调度
            try {
                if(onOff == 1){
                    jobMonitorFromDB.getQuartzJob().resumeAll();
                }else if(onOff == 2){
                    jobMonitorFromDB.getQuartzJob().pauseAll();
                }else if(onOff == 3){
                    jobMonitorFromDB.getQuartzJob().shutdown();
                }
                jobMonitorFromDB.getJobDBPool().updateQuarzStatus(onOff);
            } catch (org.quartz.SchedulerException e) {
                e.printStackTrace();
                LOG.error(e.getMessage());
                flag = false;
            }
            return flag;
        }

        @Override
        public List<Job> getQuarzAllJobs() throws TException {
            try {
                return jobMonitorFromDB.getQuartzJob().getAllJobs();
            } catch (org.quartz.SchedulerException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public List<TriggerBean> getQuarzAllTriggers() throws TException {
            try {
                return jobMonitorFromDB.getQuartzJob().getAllTriggers();
            } catch (org.quartz.SchedulerException e) {
                LOG.error(e.getMessage());
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public boolean removeQuarzTrigdger(int schedulerId, String groupName)
                throws TException {
            return jobMonitorFromDB.getQuartzJob().removeTrigdger(String.valueOf(schedulerId), groupName);
        }

        //@Override
//		public boolean systemExitScheduler()
//				throws TException {
//			//先把调度agent调度起来的所有子进程Kill掉(kill -2等同于Ctrl+c)
//			SchedulerUtils.killProcessGroup(SchedulerUtils.getProcessId());
//			return true;
//		}
    }

}
