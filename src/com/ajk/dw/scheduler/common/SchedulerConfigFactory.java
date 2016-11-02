package com.ajk.dw.scheduler.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;


public class SchedulerConfigFactory {
    private static final Logger LOG = LoggerFactory
            .getLogger(SchedulerConfigFactory.class);

    private static final SchedulerConfig DRONE_CONF;

    
    /**
     * 加载配置文件
     */
    static {

        String confDir = System.getProperty(SchedulerConfig.CONF_DIR_KEY);
 
        //在 schedul.default.yaml 中加上 scheduler.config : 配置文件路径，则加载自定义配置
        LOG.info("conf path is " + confDir);
        DRONE_CONF = SchedulerConfigFactory.readConf(confDir);

    }

    private SchedulerConfigFactory() {}

    /**
     * 
     * @param customerConfDir
     * @return
     */
    @SuppressWarnings("unchecked")
    private static SchedulerConfig readConf(String customerConfDir) {
        InputStream isDefault = null;
        InputStream isCustom = null;
        Yaml yaml = new Yaml();
        try {
            isDefault = SchedulerConfigFactory.class.getResourceAsStream("/schedul.default.yaml");

            Map<String, Object> defaultConf = (Map<String, Object>) yaml
                    .load(isDefault);
            Map<String, Object> customConf = null;
            
            if (customerConfDir != null && !customerConfDir.isEmpty()) {
                String path = customerConfDir + File.separator + "drone.yaml";
                LOG.info("loading config from " + path + "");
                if (new File(path).exists()) {
                    isCustom = new FileInputStream(path);
                    customConf = (Map<String, Object>) yaml.load(isCustom);
                }
            }

            if (customConf != null) {
                defaultConf.putAll(customConf);
            }
            //String home = System.getProperty(SchedulerConfig.DRONE_HOME_KEY, ".");
            //defaultConf.put(SchedulerConfig.DRONE_HOME_KEY, home);
            return new SchedulerConfig(defaultConf);

        } catch (Exception e) {
            LOG.error(e.getMessage(), e);
            throw new Error(e);
        } finally {
            if (isCustom != null) {
                try {
                    isCustom.close();
                } catch (Exception e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
    }

    public static SchedulerConfig getDroneConf() {
        return DRONE_CONF;
    }
}