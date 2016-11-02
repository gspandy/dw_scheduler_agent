package com.ajk.dw.scheduler.log;

public interface SchedulerLogger {
	
    void log(String msg);

    void log(String msg, Throwable throwable);

    void err(String msg);

    void err(String msg, Throwable throwable);

    void finish();
    
}
