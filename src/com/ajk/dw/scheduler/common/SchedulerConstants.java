package com.ajk.dw.scheduler.common;

public interface SchedulerConstants {

    int JOB_WAITING_SIGNAL = 1;//JOB等待信号
    
    int JOB_EXECUTING = 2;//JOB正在执行状态
    
    int JOB_SUCCESS = 3;//JOB正在执行状态
    
    int JOB_FAIL = 4;//JOB执行失败状态
    
    int JOB_KILLED = 5;//JOB被KILL
    
    int JOB_DUPSUB  = 6;//JOB堵塞状态
    
}
