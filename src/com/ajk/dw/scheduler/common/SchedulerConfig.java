package com.ajk.dw.scheduler.common;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 调度配置配置文件类
 * @author Jason
 */
public class SchedulerConfig {

    public static final String CONF_DIR_KEY = "scheduler.config";

    public static final String DRONE_HOME_KEY = "scheduler.home";

    public static final String JDBC_DRIVER_NAME = "scheduler.jdbc.driver";

    public static final String JDBC_URL = "scheduler.jdbc.url";

    public static final String JDBC_USERNAME = "scheduler.jdbc.username";

    public static final String JDBC_PASSWORD = "scheduler.jdbc.password";

    public static final String SIGNAL_FILE_PATH = "scheduler.signal.file.path";

    public static final String LOGGER_BUILDER_CLASS = "worker.logger.builder";

    public static final String LOGGER_FILE_PATH = "worker.logger.file.path";

    public static final String WORKER_SLOT_COUNT = "worker.slot.count";

    public static final String WORKER_PORT = "worker.port";

    public static final String HBASE_REST_URL = "scheduler.hbase.rest.url";

    public static final String ZOOKEEPER_HOSTS = "scheduler.zookeeper.hosts";

    public static final String SCHEDULER_LOCK_PATH = "scheduler.locks.path";

    public static final String SCHEDULER_SERVERS_PATH = "scheduler.servers.path";

    public static final String DW_SCRIPT_PATH = "dw.script.path";
    
    private final Map<String, Object> config;

    private static final String USER_HOME = System.getProperty("user.home");

    public SchedulerConfig(Map<String, Object> config) {
        super();
        this.config = new HashMap<String, Object>(config);
    }


    public String getDwScriptPath () {
        return getString(DW_SCRIPT_PATH);
    }
    
    public String getSchedulerHome () {
        return getString(DRONE_HOME_KEY);
    }

    public String getJdbcDriverName() {
        return getString(JDBC_DRIVER_NAME);
    }

    public String getJdbcUrl() {
        return getString(JDBC_URL);
    }

    public String getJdbcUsername() {
        return getString(JDBC_USERNAME);
    }

    public String getJdbcPassword() {
        return getString(JDBC_PASSWORD);
    }

    public String getSignalFilePath() {
        String signalFilePath = getString(SIGNAL_FILE_PATH);
        signalFilePath = signalFilePath.replace("${user.home}", USER_HOME);
        return signalFilePath;
    }

    public String getWorkerLoggerBuilderClassName() {
        return getString(LOGGER_BUILDER_CLASS,
                "com.ajk.schduler.log.file.FileDroneLoggerFactoryBuilder");
    }

    public String getLoggerFilePath() {
        return getString(LOGGER_FILE_PATH);
    }

    public int getWorkerSlotCount() {
        return getInt(WORKER_SLOT_COUNT);
    }

    public int getWorkerPort() {
        return getInt(WORKER_PORT);
    }

    public String getString(String key) {
        return getString(key, null);
    }

    public String getZookeeperHosts() {
        return getString(ZOOKEEPER_HOSTS);
    }

    public String getSchedulerLockPath() {
        return getString(SCHEDULER_LOCK_PATH);
    }

    public String getSchedulerServerPath() {
        return getString(SCHEDULER_SERVERS_PATH);
    }

    public String getString(String key, String defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            if (defaultValue == null) {
                throw new NullPointerException();
            }
            return defaultValue;
        }
        return (String) value;
    }

    public int getInt(String key) {
        return getInt(key, null);
    }

    public int getInt(String key, Integer defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            if (defaultValue == null) {
                throw new NullPointerException();
            }
            return defaultValue;
        }
        return ((Number) value).intValue();
    }

    public List<String> getList(String key) {
        return getList(key, null);
    }

    @SuppressWarnings("unchecked")
    public List<String> getList(String key, List<String> defaultValue) {
        Object value = config.get(key);
        if (value == null) {
            if (defaultValue == null) {
                throw new NullPointerException();
            }
            return defaultValue;
        }
        return Collections.unmodifiableList((List<String>) value);
    }

}