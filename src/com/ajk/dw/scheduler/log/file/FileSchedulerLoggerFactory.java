package com.ajk.dw.scheduler.log.file;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.ajk.dw.scheduler.log.SchedulerLogger;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactory;

public class FileSchedulerLoggerFactory implements SchedulerLoggerFactory {
    private final String logPath;

    public FileSchedulerLoggerFactory(String logPath) {
    	
    	super();
           
        this.logPath = logPath;
        
    }

    public SchedulerLogger createRunnerLogger(int excuteId, int taskId, int jobId,String command) {
    	
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    	
        String file = logPath + File.separator + taskId+"_excuteId_" + excuteId  + "_"+sdf.format(new Date())+".log";
        
        return new FileSchedulerLogger(file,excuteId);
        
    }

}
