package com.ajk.dw.scheduler.log.file;

import java.io.File;
import java.io.IOException;

import com.ajk.dw.scheduler.common.SchedulerConfig;
import com.ajk.dw.scheduler.common.exception.SchedulerException;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactory;
import com.ajk.dw.scheduler.log.SchedulerLoggerFactoryBuilder;

public class FileSchedulerLoggerFactoryBuilder implements SchedulerLoggerFactoryBuilder{

    public SchedulerLoggerFactory createLoggerFactory(SchedulerConfig config) {
        //fast fail;
        File file=new File(config.getLoggerFilePath()+File.separator+"test.log");
        try {
            if(file.exists()){
                file.delete();
            }
            file.createNewFile();
            file.delete();
        } catch (IOException e) {
            throw new  SchedulerException(e);
        }
        return new FileSchedulerLoggerFactory(config.getLoggerFilePath());
    }

}
