package com.ajk.dw.scheduler.log;

import com.ajk.dw.scheduler.common.SchedulerConfig;

public interface SchedulerLoggerFactoryBuilder {
	
    SchedulerLoggerFactory createLoggerFactory(SchedulerConfig config);
    
}
