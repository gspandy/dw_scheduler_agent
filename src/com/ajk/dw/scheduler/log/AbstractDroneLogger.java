package com.ajk.dw.scheduler.log;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.commons.lang.SystemUtils;

public abstract class AbstractDroneLogger implements SchedulerLogger {
	
    private final String lineSeparator = SystemUtils.LINE_SEPARATOR;

    public void log(String msg, Throwable throwable) {
        log(mergeMsg(msg, throwable));
    }

    public void err(String msg, Throwable throwable) {
        err(mergeMsg(msg, throwable));
    }

    private String mergeMsg(String msg, Throwable throwable) {
        Writer result = new StringWriter();
        throwable.printStackTrace(new PrintWriter(result));
        return msg + lineSeparator + result.toString();
    }

}
