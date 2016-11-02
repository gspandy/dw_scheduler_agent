 package com.ajk.dw.scheduler.quarz;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.quartz.CronTrigger;
import org.quartz.JobDetail;
import org.quartz.JobExecutionContext;
import org.quartz.SchedulerException;
import org.quartz.Scheduler;
import org.quartz.Trigger;
import org.quartz.impl.StdSchedulerFactory;
import org.quartz.utils.Key;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.JobState;
import com.ajk.dw.scheduler.common.SchdulerWorkerFactory;
import com.ajk.dw.scheduler.job.DWJob;
import com.ajk.dw.scheduler.generated.Job;
import com.ajk.dw.scheduler.generated.TriggerBean;
import com.ajk.dw.scheduler.job.JobDBPool;
import com.ajk.dw.scheduler.utils.SchedulerUtils;
import com.google.common.collect.Lists;


/**
 * Scheduler 定时任务控制, 新增、修改、重跑控制
 * 本类使用:
 *  org.quartz 作为 定时任务框架
 */
public class QuartzScheduler {
    private final String SCHEDULER_GROUP = "DW_QUARZ_GROUP";
    private Scheduler scheduler;
    private static final Logger LOG = LoggerFactory
            .getLogger(QuartzScheduler.class);
    private final JobDBPool jobDBPool;
    private ExecutorService executorService = Executors.newSingleThreadExecutor();
    private boolean isPause = false;

    public QuartzScheduler(JobDBPool jobDBPool) {
        this.jobDBPool = jobDBPool;
        try {
            scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();
            scheduler.getContext().put("jobDBPool", jobDBPool);
            //quarz启动时候先加载漏掉的调度
            loadNotScheduleJobs();
            executorService.submit(new BackUpQuarzData());
        } catch (SchedulerException e) {
            LOG.error("creater QuartzJob Fail :" ,e);
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        QuartzScheduler test = new QuartzScheduler(null);
        try {

            test.startSchedule();
            Thread.sleep(3000);
            // test.resetJob("*/2 11 00 * * ?");
            // test.addschduler(new DWJob());

        } catch (Exception e) {

            e.printStackTrace();

        } finally {
            // test.cancelschduler("");
            System.out.println("结束！！！！！！！！！！！！！");
        }

    }

    public void startSchedule() throws Exception {

        JobDetail jobDetail = new JobDetail("testJob", SCHEDULER_GROUP,
                QuartzJob.class);
        CronTrigger cronTrigger = new CronTrigger("yourTriggerName",
                SCHEDULER_GROUP, "*/3 * * * * ?");
        // 执行10次，每3秒执行一次，到9秒后结束
        // SimpleTrigger trigger = new SimpleTrigger("test", null, new
        // Date(),new Date(end), 10, 3000L);

        scheduler.scheduleJob(jobDetail, cronTrigger);

        cronTrigger = new CronTrigger("yourTriggerName2", SCHEDULER_GROUP,
                "*/5 * * * * ?");

        jobDetail = new JobDetail("testJob6", SCHEDULER_GROUP, QuartzJob.class);
        scheduler.scheduleJob(jobDetail, cronTrigger);

        scheduler.start();

        Thread.sleep(10000);

        //System.out.println("test jobs数量--------"+ scheduler.getJobNames(SCHEDULER_GROUP).length);

    }

    public void resetJob(DWJob job) {
        try {
            if (job.getJobStatus() == 2) {
                // 由于暂停方法，不会把触发器去除,resumeJob时候会把之前错过运行时间的job重新调度。
                // pauseJob(String.valueOf(job.getSchedulerId()),SCHEDULER_GROUP);
                scheduler.unscheduleJob(String.valueOf(job.getSchedulerId()),
                        SCHEDULER_GROUP);
                return;
            } else if (job.getJobStatus() == 3) {
                // 删除这个job
                deleteJob(String.valueOf(job.getSchedulerId()), SCHEDULER_GROUP);
                return;
            }

            // 运行时可通过动态注入的scheduler得到trigger，
            // 注意采用这种注入方式在有的项目中会有问题，如果遇到注入问题，
            // 可以采取在运行方法时候，获得bean来避免错误发生。
            CronTrigger trigger = (CronTrigger) scheduler.getTrigger(
                    String.valueOf(job.getSchedulerId()), SCHEDULER_GROUP);
            JobDetail jobDetail = scheduler.getJobDetail(
                    String.valueOf(job.getSchedulerId()), SCHEDULER_GROUP);
            jobDetail.getJobDataMap().put("job", job);
            scheduler.addJob(jobDetail, true);
            String originConExpression = trigger.getCronExpression();
            // System.out.println("originConExpression---------"+
            // originConExpression);
            // 如果相等，则表示用户并没有重新设定数据库中的任务时间，这种情况不需要重新rescheduleJob
            if (!originConExpression.equalsIgnoreCase(job.getRunTime())) {
                trigger.setCronExpression(job.getRunTime());
                scheduler.interrupt(String.valueOf(job.getSchedulerId()),
                        SCHEDULER_GROUP);
                scheduler.unscheduleJob(String.valueOf(job.getSchedulerId()),
                        SCHEDULER_GROUP);
                scheduler.rescheduleJob(String.valueOf(job.getSchedulerId()),
                        SCHEDULER_GROUP, trigger);
            } else {
                resumeJob(String.valueOf(job.getSchedulerId()), SCHEDULER_GROUP);
            }
        } catch (SchedulerException e) {
            LOG.error("resetJob() :",e);
            e.printStackTrace();
        } catch (ParseException e) {
            LOG.error("resetJob() :" ,e);
            e.printStackTrace();
        }

    }

    public void cancelschduler(String cronExpression) {
        try {
            scheduler.shutdown();
        } catch (SchedulerException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void addschduler(DWJob job) {
        //如果job为空或者调度器已经暂停就不继续进行
        if (job == null || isPause)
            return;
        try {
            CronTrigger cronTrigger = (CronTrigger) scheduler.getTrigger(
                    String.valueOf(job.getSchedulerId()), SCHEDULER_GROUP);
            if (cronTrigger != null) {
                // 如果已经存在的话
                resetJob(job);
            } else if (job.getJobStatus() <= 1) {// 新加job的话过滤掉删除和暂停job
                // 如果是新的job往里面添加
                JobDetail jobDetail = new JobDetail(String.valueOf(job
                        .getSchedulerId()), SCHEDULER_GROUP, QuartzJob.class);
                jobDetail.getJobDataMap().put("job", job);
                cronTrigger = new CronTrigger(String.valueOf(job
                        .getSchedulerId()), SCHEDULER_GROUP, job.getRunTime());
                scheduler.scheduleJob(jobDetail, cronTrigger);
                LOG.info("addschduler():jobs数量----------------"+ scheduler.getJobNames(SCHEDULER_GROUP).length);
            }
        } catch (SchedulerException e) {
            //JOB出错（设定时间不能被执行等）
            //jobDBPool.pauseTaskExcuteCfg(job);之前想要暂停，有问题
            LOG.error("添加job出错了1！");
            LOG.error("addschduler(jobId:"+job.getJobId()+",schedulerId:"+job.getSchedulerId()+") have error:",e);
        } catch (ParseException e) {
            LOG.error("添加job出错了2！");
            LOG.error("addschduler(jobId:"+job.getJobId()+") have error:" ,e);
        }

    }

    /**
     * 每次启动时候加载调度关闭前到现在启动
     * 错过的调度Jobs
     */
    @SuppressWarnings("unchecked")
    private void loadNotScheduleJobs(){
        try {
            QuarzDataBean quarzData = jobDBPool.getNewestQuarzBackData();
            Map<String,Trigger> triggersMap = null;
            Map<String,JobDetail> jobDetailsMap = null;
            Date backupDate = null;
            if(quarzData != null && quarzData.getStatus()==1){
                triggersMap = (Map<String, Trigger>) SchedulerUtils.BlobToObject(quarzData.getTriggers_map());
                jobDetailsMap = (Map<String, JobDetail>) SchedulerUtils.BlobToObject(quarzData.getJobs_map());
                backupDate = quarzData.getCreate_time();
            }
            //获取有可能需要重跑的Job
            Map<String,DWJob> excuteJobMap = jobDBPool.getNewestExcuteLog(backupDate);
            if(triggersMap != null){
                for (String key : triggersMap.keySet()) {
                    CronTrigger trig = (CronTrigger) triggersMap.get(key);
                    //key值都设置成job调度Id
                    if(!excuteJobMap.containsKey(key) && jobDetailsMap.containsKey(key)){
                        //如果不在excuteJobMap并且存在jobDetail,就直接加到调度器中,这样会把未来要跑的调度起来所以注释掉
                        //scheduler.scheduleJob(jobDetailsMap.get(key), triggersMap.get(key));
                        jobDetailsMap.remove(key);
                    }else if(excuteJobMap.containsKey(key) && jobDetailsMap.containsKey(key)){
                        //如果在excuteJobMap并且存在jobDetail,就对trigger比对是否已经运行过了
                        DWJob dwjob = excuteJobMap.get(key);
                        //如果调度器中的日期，跟日志表最后下次运行时间小，并且
                        if(dwjob.getNextFireTime()!=null && !dwjob.getCreateTime().before(trig.getNextFireTime())){
                            trig.setStartTime(dwjob.getCreateTime());
                            scheduler.scheduleJob(jobDetailsMap.get(key), trig);
                        }
                        if(dwjob.getJobState()<3){
                            //如果运行没有完成的job直接放入执行队列
                            dwjob.setJobState(JobState.JOB_WAITING_SIGNAL.getCode());
                            dwjob.setTaskId(2);
                            int executeId =jobDBPool.createExcuteLog(dwjob);
                            //生成日志放入job运行过程日志表里
                            jobDBPool.SchedulerExcuteLog(dwjob);
                            SchdulerWorkerFactory.executorJob.put(executeId, dwjob);
                        }
                        jobDetailsMap.remove(key);
                        excuteJobMap.remove(key);
                    }
                }
            }

            //如果触发器中都加载完成了，但是调度日志中含有的调度，也需要重新调度
            for (String key : excuteJobMap.keySet()) {
                DWJob dwjob = excuteJobMap.get(key);
                if(dwjob.getJobState()<3){
                    //如果运行没有完成的job直接放入执行队列
                    dwjob.setJobState(JobState.JOB_WAITING_SIGNAL.getCode());
                    int executeId =jobDBPool.createExcuteLog(dwjob);
                    //生成日志放入job运行过程日志表里
                    jobDBPool.SchedulerExcuteLog(dwjob);
                    SchdulerWorkerFactory.executorJob.put(executeId, dwjob);
                }
            }
        } catch (Exception e) {
            LOG.error("loadNotScheduleJobs have error:" ,e);
        }
    }

    /**
     * 是否启动
     *
     * @return
     * @throws org.quartz.SchedulerException
     */
    public boolean isStartTimerTisk() throws SchedulerException {
        return this.scheduler.isStarted();
    }

    /**
     * 是否关闭
     *
     * @return
     * @throws org.quartz.SchedulerException
     */
    public boolean isShutDownTimerTisk() throws SchedulerException {
        return this.scheduler.isShutdown();
    }

    /**
     * 停止作业
     *
     * @param jobName
     * @param groupName
     */
    public void pauseJob(String jobName, String groupName)
            throws SchedulerException {
        this.scheduler.pauseJob(jobName, groupName);
    }

    /**
     * 恢复 job
     *
     * @param jobName
     * @param groupName
     */
    public void resumeJob(String jobName, String groupName)
            throws SchedulerException {
        this.scheduler.resumeJob(jobName, groupName);
    }

    /**
     * 删除指定的job
     *
     * @param jobName
     * @param groupName
     * @return
     * @throws org.quartz.SchedulerException
     */
    public boolean deleteJob(String jobName, String groupName)
            throws SchedulerException {
        return this.scheduler.deleteJob(jobName, groupName);
    }

    /**
     * 停止触发器
     *
     * @param triggerName
     * @param group
     */
    public void pauseTrigger(String triggerName, String group) {
        try {
            this.scheduler.pauseTrigger(triggerName, group);// 停止触发器
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 重启触发器
     *
     * @param triggerName
     * @param group
     */
    public void resumeTrigger(String triggerName, String group) {
        try {
            this.scheduler.resumeTrigger(triggerName, group);// 重启触发器
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 移除触发器
     *
     * @param triggerName
     * @param group
     * @return
     */
    public boolean removeTrigdger(String triggerName, String group) {
        try {
            this.scheduler.pauseTrigger(triggerName, group);// 停止触发器
            return this.scheduler.unscheduleJob(triggerName, group);// 移除触发器
        } catch (SchedulerException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 暂停调度中所有的job任务
     *
     * @throws SchedulerException
     */
    public void pauseAll() throws SchedulerException {
        this.isPause = true;
        scheduler.pauseAll();
    }

    /**
     * 恢复调度中所有的job的任务
     *
     * @throws SchedulerException
     */
    public void resumeAll() throws SchedulerException {
        scheduler.resumeAll();
        this.isPause = false;
    }

    /**
     * 关闭调度
     *
     * @throws SchedulerException
     */
    public void shutdown() throws SchedulerException {
        executorService.shutdownNow();
        scheduler.shutdown();
    }

    @SuppressWarnings("unchecked")
    public List<DWJob> getRunningJobs() throws SchedulerException {
        List<JobExecutionContext> contextList = scheduler
                .getCurrentlyExecutingJobs();
        List<DWJob> result = Lists.newArrayListWithCapacity(contextList.size());
        for (JobExecutionContext context : contextList) {
            Key jobKey = context.getJobDetail().getKey();
            System.out.println(jobKey);
            result.add((DWJob) context.getJobDetail().getJobDataMap()
                    .get("job"));
        }
        return result;
    }

    public List<Job> getAllJobs() throws SchedulerException {
        String[] jobNames = scheduler.getJobNames(SCHEDULER_GROUP);
        List<Job> result = Lists.newArrayListWithCapacity(jobNames.length);
        for (String jobName : scheduler.getJobNames(SCHEDULER_GROUP)) {
            JobDetail jobDetail = scheduler.getJobDetail(jobName,
                    SCHEDULER_GROUP);
            result.add(((DWJob) jobDetail.getJobDataMap().get("job")).convertToJob());
        }
        return result;
    }

    /**
     * 读取Quarz所有触发器情况
     * @return
     * @throws SchedulerException
     */
    public List<TriggerBean> getAllTriggers() throws SchedulerException {
        String[] triggerKey = scheduler.getTriggerNames(SCHEDULER_GROUP);
        List<TriggerBean> result = Lists
                .newArrayListWithCapacity(triggerKey.length);
        DWJob dwJob = null;
        for (String key : triggerKey) {
            TriggerBean bean = new TriggerBean();
            bean.setName(SCHEDULER_GROUP);
            Trigger trigger = scheduler.getTrigger(key, SCHEDULER_GROUP);
            if(trigger == null){
                LOG.info("getAllTriggers() :scheduler.getTrigger("+key+", "+SCHEDULER_GROUP+") return null");
                continue;
            }
            String jobName = trigger.getJobName();
            JobDetail jobDetail = scheduler.getJobDetail(jobName,
                    SCHEDULER_GROUP);
            dwJob = (DWJob) jobDetail.getJobDataMap().get("job");
            bean.setJobName(dwJob.getJobName());
            bean.setJobId(dwJob.getJobId());
            bean.setSchedulerId(dwJob.getSchedulerId());
            bean.setState(TriggerState.getState(scheduler.getTriggerState(key,
                    SCHEDULER_GROUP)));
            bean.setPreviousFireTime(SchedulerUtils.formatDate(trigger.getPreviousFireTime()));
            bean.setNextFireTime(SchedulerUtils.formatDate(trigger.getNextFireTime()));
            result.add(bean);
        }
        return result;
    }

    /**
     * 获取Quarz调度数据
     * @return
     */
    public QuarzDataBean getQuarzData(){
        Date startTime=new Date();//备份开始时间
        Trigger trigger = null;
        String jobName = null;

        try {
            String[] triggerKey = scheduler.getTriggerNames(SCHEDULER_GROUP);
            Map<String,Trigger> triggersMap = new HashMap<String,Trigger>();
            Map<String,JobDetail> jobDetailsMap = new HashMap<String,JobDetail>();
            QuarzDataBean quarzData = new QuarzDataBean();
            for (String key : triggerKey) {
                trigger = scheduler.getTrigger(key, SCHEDULER_GROUP);
                if(trigger == null) {
                    LOG.info("getQuarzData() :scheduler.getTrigger("+key+", "+SCHEDULER_GROUP+") return null");
                    continue;
                }
                jobName = trigger.getJobName();
                JobDetail jobDetail = scheduler.getJobDetail(jobName,
                        SCHEDULER_GROUP);
                triggersMap.put(jobName,trigger);
                jobDetailsMap.put(jobName, jobDetail);
            }
            quarzData.setTriggers_map(SchedulerUtils.ObjectToBlob(triggersMap));
            quarzData.setJobs_map(SchedulerUtils.ObjectToBlob(jobDetailsMap));
            quarzData.setCreate_time(startTime);
            quarzData.setUpdate_time(startTime);
            return quarzData;
        } catch (SchedulerException e) {
            e.printStackTrace();
            LOG.error("getQuarzData() :",e);
            return null;
        }
    }

    public Scheduler getScheduler() {
        return scheduler;
    }

    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * 自动保存quarz信息线程
     * @author rolandwang
     *
     */
    private class BackUpQuarzData implements Runnable {
        public void run() {
            Thread.currentThread().setName("threadName-BackUpQuarzData");
            while (true) {
                try {
                    QuarzDataBean quarzData = getQuarzData();
                    jobDBPool.saveQuarzRealTimeData(quarzData);
                    Thread.sleep(60*1000);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOG.error("QuartzScheduler.BackUpQuarzData:",e);
                }
            }
        }
    }
}