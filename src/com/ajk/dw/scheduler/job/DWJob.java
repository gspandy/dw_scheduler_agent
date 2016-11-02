package com.ajk.dw.scheduler.job;

import java.io.Serializable;
import java.util.Date;

import com.ajk.dw.scheduler.generated.Job;

/**
 *  job 对象实体
 */
public class DWJob implements Serializable{
	private static final long serialVersionUID = 3470640780616058568L;
	private int excuteId; // required
	private int schedulerId;
	private int configId; // required
	private int taskId; // required
	private int jobId; // required
	private int runType;
	private String jobName; // required
    private String command; // required
    private String runTime; // required
    private long submitTime; // required
    private int jobState; // 项目运行状态
    private int jobStatus; // 项目更改状态
    private int jobType; // 项目类型
    private long executeStartTime; // optional
    private long executeEndTime; // optional
    private int exitCode; // optional
    private String dependent_jobs; // optional
    private int retry; // 重跑次数
    private Date createTime;
	private Date updateTime;
	private Date nextFireTime;//下次调度时间
	private int runCount; // 重跑了次数
    
    
	@SuppressWarnings("unused")
	public Job convertToJob(){
    	Job job = new Job();
    	job.setExcuteId(this.excuteId);
    	job.setSchedulerId(this.schedulerId);
    	job.setConfigId(this.configId);
    	job.setTaskId(this.taskId);
    	job.setJobId(this.jobId);
    	job.setJobName(this.jobName);
    	job.setCommand(this.command);
    	job.setRunTime(this.runTime);
    	job.setJobState(this.jobState);
    	job.setSubmitTime(this.submitTime);
    	return job;
    }
	
	public int getExcuteId() {
		return excuteId;
	}
	public void setExcuteId(int excuteId) {
		this.excuteId = excuteId;
	}
	
	public int getSchedulerId() {
		return schedulerId;
	}
	public void setSchedulerId(int schedulerId) {
		this.schedulerId = schedulerId;
	}
	public int getConfigId() {
		return configId;
	}
	public void setConfigId(int configId) {
		this.configId = configId;
	}
	public int getTaskId() {
		return taskId;
	}
	public void setTaskId(int taskId) {
		this.taskId = taskId;
	}
	public int getJobId() {
		return jobId;
	}
	public int getRunType() {
		return runType;
	}
	public void setRunType(int runType) {
		this.runType = runType;
	}
	public String getJobName() {
		return jobName;
	}
	public void setJobName(String jobName) {
		this.jobName = jobName;
	}
	public void setJobId(int jobId) {
		this.jobId = jobId;
	}
	
	public String getRunTime() {
		return runTime;
	}
	public void setRunTime(String runTime) {
		this.runTime = runTime;
	}
	public String getCommand() {
		return command;
	}
	public void setCommand(String command) {
		this.command = command;
	}
	public long getSubmitTime() {
		return submitTime;
	}
	public void setSubmitTime(long submitTime) {
		this.submitTime = submitTime;
	}
	public int getJobState() {
		return jobState;
	}
	public void setJobState(int jobState) {
		this.jobState = jobState;
	}
	public int getJobStatus() {
		return jobStatus;
	}
	public void setJobStatus(int jobStatus) {
		this.jobStatus = jobStatus;
	}
	public int getJobType() {
		return jobType;
	}
	public void setJobType(int jobType) {
		this.jobType = jobType;
	}
	public long getExecuteStartTime() {
		return executeStartTime;
	}
	public void setExecuteStartTime(long executeStartTime) {
		this.executeStartTime = executeStartTime;
	}
	public long getExecuteEndTime() {
		return executeEndTime;
	}
	public void setExecuteEndTime(long executeEndTime) {
		this.executeEndTime = executeEndTime;
	}
	public int getExitCode() {
		return exitCode;
	}
	public void setExitCode(int exitCode) {
		this.exitCode = exitCode;
	}
	public String getDependent_jobs() {
		return dependent_jobs;
	}
	public void setDependent_jobs(String dependent_jobs) {
		this.dependent_jobs = dependent_jobs;
	}
	public int getRetry() {
		return retry;
	}
	public void setRetry(int retry) {
		this.retry = retry;
	}
	public Date getCreateTime() {
		return createTime;
	}
	public void setCreateTime(Date createTime) {
		this.createTime = createTime;
	}
	public Date getUpdateTime() {
		return updateTime;
	}
	public void setUpdateTime(Date updateTime) {
		this.updateTime = updateTime;
	}
	public Date getNextFireTime() {
		return nextFireTime;
	}
	public void setNextFireTime(Date nextFireTime) {
		this.nextFireTime = nextFireTime;
	}
	public int getRunCount() {
		return runCount;
	}
	public void setRunCount(int runCount) {
		this.runCount = runCount;
	}
}
