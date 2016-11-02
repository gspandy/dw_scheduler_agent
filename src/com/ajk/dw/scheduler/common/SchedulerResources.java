package com.ajk.dw.scheduler.common;

import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Jason
 * apache dbcp 数据源配置
 */
public class SchedulerResources {

    private Logger logger = LoggerFactory.getLogger(SchedulerResources.class);
    
    private final BasicDataSource dataSource;

    public SchedulerResources( BasicDataSource dataSource) {
        super();
        this.dataSource = dataSource;
    }

    public BasicDataSource getDataSource() {
        return dataSource;
    }

    public void close() {
        try {
            	dataSource.close();
        } catch (SQLException e) {
            	logger.error(e.getMessage(), e);
        }
    }

}
