package com.ajk.dw.scheduler.log.file;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.ajk.dw.scheduler.common.exception.SchedulerException;
import com.ajk.dw.scheduler.log.AbstractDroneLogger;
import com.ajk.dw.scheduler.log.SchedulerLogger;
//import com.ajk.dw.scheduler.utils.HbaseUtil;

public class FileSchedulerLogger extends AbstractDroneLogger implements SchedulerLogger {

    private final PrintWriter writer;
    private final Integer excuteId;

    public FileSchedulerLogger(String file,Integer excuteId) {
        super();
        try {
                this.excuteId = excuteId;
                writer = new PrintWriter(file);
        } catch (FileNotFoundException e) {
                throw new SchedulerException(e);
        }
    }

    public void log(String msg) {
        writeLog("INFO", msg);
    }

    private synchronized void writeLog(String type, String msg) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,S");
        long curTimeMillis = System.currentTimeMillis();
        String logLine = String.format("%s %s - %s", type,sdf.format(new Date(curTimeMillis)), msg);
        writer.println(logLine);
        writer.flush();
//        HbaseUtil.hbasePutRestApi("dw_scheduler_job_log",excuteId.toString(),
//        		"runlog:"+type+System.currentTimeMillis(),logLine);
    }

    public void err(String msg) {
        writeLog("INFO-MSG", msg);
    }

    public synchronized void finish() {
        writer.flush();
        writer.close();
    }

}
