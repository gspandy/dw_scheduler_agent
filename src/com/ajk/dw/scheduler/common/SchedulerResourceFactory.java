package com.ajk.dw.scheduler.common;

import java.sql.SQLException;
import org.apache.commons.dbcp.BasicDataSource;

import com.ajk.dw.scheduler.common.exception.SchedulerException;

public class SchedulerResourceFactory {

    private static final int DATASOURCE_SIZE = 2;
    private static final int MaxIdle = 5;
    private static final int MaxActive=	30;
    private static final int DATASOURCE_TIMEOUT = 60000;
    private static final String VALIDATE_QUERY="SELECT 1;";

    private static final int MIN_EVICT_IDLE_TIME=25000;
    private static final int TIME_BETWEEN_EVICT=60000;


    /**
     * 根据配置, 创建一个 Scheduler 数据源连接对象
     * @param SchedulerConfig config
     * @return SchedulerResources
     */
    public static SchedulerResources createResources(SchedulerConfig config) {
        BasicDataSource dataSource = createDataSource(config);
        return new SchedulerResources(dataSource);
    }

    /**
     * 创建 数据源连接对象
     * @param config
     * @return
     */
    private static BasicDataSource createDataSource(SchedulerConfig config) {
        final BasicDataSource bds = new BasicDataSource();
        bds.setDriverClassName(config.getJdbcDriverName());
        bds.setUsername(config.getJdbcUsername());
        bds.setPassword(config.getJdbcPassword());
        bds.setUrl(config.getJdbcUrl());
        
        bds.setInitialSize(DATASOURCE_SIZE);
        bds.setMaxActive(MaxActive);
        bds.setMaxIdle(MaxIdle);
        bds.setMinIdle(DATASOURCE_SIZE);
        bds.setMaxWait(DATASOURCE_TIMEOUT);
        bds.setMinEvictableIdleTimeMillis(MIN_EVICT_IDLE_TIME);
        bds.setTimeBetweenEvictionRunsMillis(TIME_BETWEEN_EVICT);
        bds.setValidationQuery(VALIDATE_QUERY);
        try {
            	bds.getConnection().close();
        } catch (SQLException e) {
            	throw new SchedulerException(e);
        }
        	return bds;
    }

}
