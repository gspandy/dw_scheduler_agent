namespace java com.ajk.dw.scheduler.generated

struct Job {
    1: required i32 schedulerId,
    2: required i32 configId,
    3: required i32 taskId,
    4: required i32 excuteId,
    5: optional i32 jobId,
    6: optional string jobName,
    7: optional string command,
    8: optional string runTime,
    9: optional i32 jobState,
    10: optional i64 submitTime
}

struct TriggerBean {
    optional string name;
    required i32 jobId;
    required i32 schedulerId;
    optional string jobName;
    optional string state;
    optional string nextFireTime;
    optional string previousFireTime
}

service WorkerService{
    list<Job> getPendingJobs(),
    bool kill(1:i32 schedulerId),
    bool stopPendingJob(1:i32 excuteId),
    bool switchQuarz(1:i32 onOff),
    list<Job> getQuarzAllJobs(),
    list<TriggerBean> getQuarzAllTriggers(),
    bool removeQuarzTrigdger(1:i32 schedulerId,string groupName),
    //bool systemExitScheduler()
}

