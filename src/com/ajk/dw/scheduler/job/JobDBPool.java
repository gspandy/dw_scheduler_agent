package com.ajk.dw.scheduler.job;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ajk.dw.scheduler.common.exception.SchedulerException;
import com.ajk.dw.scheduler.quarz.QuarzDataBean;
import com.ajk.dw.scheduler.quarz.QuarzResultSetHandler;
import com.ajk.dw.scheduler.utils.IPUtils;
import com.ajk.dw.scheduler.utils.SchedulerUtils;


/**
 * job 在 db 的数据池
 * @author Jason
 */
public class JobDBPool implements java.io.Serializable{
    private static final Logger LOG = LoggerFactory.getLogger(JobDBPool.class);
    private static final long serialVersionUID = 4755266084580198765L;
    private final DataSource dataSource;

    public JobDBPool(DataSource dataSource) {
        super();
        this.dataSource = dataSource;
    }

    public int createExcuteLog(DWJob job) {
        final String sql = " insert into dw_scheduler_task_excute_log"
                + "(scheduler_id,task_id,job_id,job_name,job_state,dispatch_command,run_time,next_fire_time,type,dependent_jobs,retry,create_time,update_time) "
                + "values(?,?,?,?,?,?,?,?,?,?,?,?,?)";
        Connection conn = null;
        PreparedStatement stmt = null;
        QueryRunner queryRunner = new QueryRunner();
        try {
                conn = dataSource.getConnection();
                stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
                queryRunner.fillStatement(stmt, job.getSchedulerId(), job.getTaskId(), job.getJobId(),job.getJobName(),
                    job.getJobState(),job.getCommand(), job.getRunTime(),job.getNextFireTime(),
                    job.getRunType(), job.getDependent_jobs(), job.getRetry(), new Date(), new Date());
                stmt.executeUpdate();
                ResultSet resultSet = stmt.getGeneratedKeys();
                resultSet.next();
                int key = resultSet.getInt(1);
                job.setExcuteId(key);
                //更新task表为已调度
                updateTaskStatus(job);
                //更新该job信号为无效
                updateJobSignal(job.getJobId());
                //需要把之前生成的信号文件也删除
                SchedulerUtils.deleteSignalFile(job.getJobName());
                return key;
        } catch (SQLException e) {
            throw new SchedulerException(e);
        } finally {
            if(conn != null){
              try{ conn.close(); }catch(Throwable ex1){}
            }
            DbUtils.closeQuietly(conn, stmt, null);
        }
    }

    /**
     * 记录ETL job调度过程
     * @param job
     */
    public void SchedulerExcuteLog(DWJob job) {
        final String sql = "insert into dw_scheduler_task_excute_process_log"
                + "(excute_id,task_id,job_id,job_name,job_state,dispatch_command,run_time,next_fire_time,type,dependent_jobs,create_time) "
                + "values(?,?,?,?,?,?,?,?,?,?,?)";
        Connection conn = null;
        PreparedStatement stmt = null;
        QueryRunner queryRunner = new QueryRunner();
        try {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            queryRunner.fillStatement(stmt,job.getExcuteId(), job.getTaskId(), job.getJobId(),job.getJobName(),
                    job.getJobState(),job.getCommand(), job.getRunTime(),job.getNextFireTime(),
                    job.getRunType(), job.getDependent_jobs(), new Date());
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new SchedulerException(e);
        } finally {
            if(conn != null){
                try{ conn.close(); }catch(Throwable ex1){}
              }
            DbUtils.closeQuietly(conn, stmt, null);
        }

        //更新dw_scheduler_task_job_cfg这张表
        updateTaskExcuteCfg(job);

    }

    /**
     * 跑job之前顺便把它信号设置为无效
     * @param job
     */
    public void updateJobSignal(int jobId) {
        //更新信号为无效
        final String sql = "update dw_scheduler_job_signal set status=2  where job_id=?";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            queryRunner.update(sql,jobId);
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }

    }

    /**
     * 执行完成生成信号记录
     * @param job
     */
    public void generateSignal(DWJob job){
        final String sql = "replace into dw_scheduler_job_signal"
            + "(job_id,job_name,status,create_time,update_time) "
            + "values(?,?,?,?,?)";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            queryRunner.update(sql, job.getJobId(),job.getJobName(),1,
                    new Date(), new Date());
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }

    }

    /**
     * 判断该job依赖的信号都生成，是返回true，没有生成返回false
     * @param needRunSet
     */
    public boolean shouldRunJob(int jobId){
        final String sql = "select a.job_id,count(distinct a.parent_id) total,count(distinct case when b.status =1 then b.job_id end) as run  "
            +" from dw_scheduler_job_relation  a  "
            +" left join dw_scheduler_job_signal b on a.parent_id = b.job_id "
            +" where a.status =1 and a.job_id="+jobId+"  GROUP BY a.job_id ";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            List<Map<String,Object>> haveSiganlMapList = queryRunner.query(sql,new MapListHandler());
            for(Map<String,Object> needRunJob : haveSiganlMapList){
                Integer total = ((Number)needRunJob.get("total")).intValue();
                Integer run = ((Number)needRunJob.get("run")).intValue();
                if(total.equals(run)){
                    return true;
                }else{
                    return false;
                }
            }
            return true;
        } catch (SQLException e) {
            LOG.error("shouldRunJob", e);
            return false;
        }

    }

    public void updateTaskStatus(DWJob job) {
        //跟新task为已调度
        final String sql = "update dw_scheduler_task set status=1  where id=?";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            queryRunner.update(sql,job.getTaskId());
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }

    }

    public void updateExecuteFinish(DWJob job) {
        final String sql = "update dw_scheduler_task_excute_log set job_state=?,exit_code=?,update_time=? where id=?";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            queryRunner.update(sql, job.getJobState(),
                    job.getExitCode(),
                    new Date(),
                    job.getExcuteId());
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }
        //更新dw_scheduler_task_job_cfg这张表
        updateTaskExcuteCfg(job);
    }

    public List<DWJob> getJobs() {
        //获取未调度的记录，包括常规ETL调度(task_id=1)
        final String sql = "select id,scheduler_id,task_id,job_id,job_name,status,job_state,"
                +"dispatch_command,retry,run_time,type,dependent_jobs "
                + "from dw_scheduler_task_job_cfg "
                + "where (job_state=0 or task_id=1) and status <> 0";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            return queryRunner.query(sql, new JobListResultSetHandler());
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }
    }

    public void updateTaskExcuteCfg(DWJob job) {
        final String sql = "update dw_scheduler_task_job_cfg set job_state=? where id=?";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            queryRunner.update(sql, job.getJobState(),
                            job.getConfigId());
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }

    }
    /**
     * 如果job未知错误，则暂停调度（例如调度时间过期）
     * @param job_id
     */
    public void pauseTaskExcuteCfg(DWJob job) {
        final String sql = "update dw_scheduler_task_job_cfg set status=0 where id=?";
        final String sql2 = "update dw_scheduler_job set status=99 where id=?";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            queryRunner.update(sql, job.getConfigId());
            queryRunner.update(sql2, job.getSchedulerId());
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }

    }
    /**
     * 获取报错需要从跑的job
     * @return
     */
    public List<DWJob> reRunErrorJobs() {
        //获取需要重跑调度的记录，只针对常规ETL调度(task_id=1), 等待了3分钟,被KILL之后，不再执行重跑
        final String sql = "select scheduler_id,count(case when job_state=4 then 1 end) run_count," +
                "max(update_time) complete_time," +
                "retry, " +
                "count(case when job_state in(1,2,3,5) then 1 end) run_success " +
                "from dw_scheduler_task_excute_log " +
                "where create_time >=CURDATE() and retry > 0 " +
                "GROUP BY scheduler_id HAVING run_success=0 and MINUTE(timediff(CURRENT_TIMESTAMP(),complete_time))>=3";
        //获取需要重新调度的记录，常规ETL调度(task_id=1)，job被改为重跑次数0,不再执行重跑
        final String getJobsSql = "select id,scheduler_id,task_id,job_id,job_name,status,0 as job_state,"
                +"dispatch_command,retry,run_time,type,dependent_jobs "
                + "from dw_scheduler_task_job_cfg "
                + "where task_id=1 and retry >0 and status in (1,2) and scheduler_id in (?)";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            List<DWJob> retryJobList = queryRunner.query(sql, new JobRetryListResultSetHandler());
            StringBuffer schedulerIds = new StringBuffer();
            Map<Integer, Integer> jobRunCountMap = new HashMap<Integer,Integer>();
            for(DWJob dwJob : retryJobList){
                if(dwJob.getRetry()>(dwJob.getRunCount()-1)) {
                    schedulerIds.append(dwJob.getSchedulerId()+",");
                    jobRunCountMap.put(dwJob.getSchedulerId(), dwJob.getRunCount());
                }
            }
            if(schedulerIds.length()>0){
                schedulerIds.deleteCharAt(schedulerIds.length()-1);
            }
            @SuppressWarnings("deprecation")
            List<DWJob> jobList = queryRunner.query(getJobsSql,schedulerIds.toString(),new JobListResultSetHandler());
            for(DWJob dwJob : jobList){
                dwJob.setRunCount(jobRunCountMap.get(dwJob.getSchedulerId()));
            }
            return jobList;
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }
    }


    /**
     * 记录HA服务器上面调度运行状态
     * 1-调度正在运行，2-调度等待,3-调度未启动或关闭
     * 1-quarz正在运行，2-quarz暂停,3-quarz未启动或关闭
     * @param job
     * @return
     */
    public void recordSchedulerStatus(int status,int quarzStatus) {
        String serverIp = IPUtils.getFirstNoLoopbackIP4Address();
        String serverHost = IPUtils.getHostName();
        final String sql = "replace into dw_scheduler_ha_host"
                + "(host_ip,host_name,status,quarz_status,create_time,update_time) "
                + "values(?,?,?,?,?,?)";
        final String logSql = "insert into dw_scheduler_ha_host_log"
            + "(host_name,host_ip,status,quarz_status,create_time) "
            + "values(?,?,?,?,?)";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            queryRunner.update(sql, serverIp,serverHost,status,quarzStatus,new Date(), new Date());
            //记录操作日志
            queryRunner.update(logSql,serverHost,serverIp,status,quarzStatus,new Date());
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }
    }

    /**
     * 更新HA服务器上面调度Quarz运行状态
     * 1-quarz正在运行，2-quarz暂停,3-quarz未启动或关闭
     * @param job
     * @return
     */
    public void updateQuarzStatus(int quarzStatus) {
        final String sql = "update dw_scheduler_ha_host set quarz_status =? where status =1";
        final String logSql = "insert into dw_scheduler_ha_host_log"
            + "(host_name,host_ip,status,quarz_status,create_time) "
            + "select host_name,host_ip,status,?,? from dw_scheduler_ha_host where status =1";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            queryRunner.update(sql,quarzStatus);
            //记录操作日志
            queryRunner.update(logSql,quarzStatus,new Date());
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }
    }

    /**
     * 备份最新Quarz数据
     * @param job
     * @return
     */
    public void saveQuarzRealTimeData(QuarzDataBean quarzDataBean) {
        final String sql = "insert into dw_scheduler_ha_quarz"
                + "(triggers_map,jobs_map,create_time,update_time) "
                + "values(?,?,?,?)";
        Connection conn = null;
        PreparedStatement stmt = null;
        QueryRunner queryRunner = new QueryRunner();
        try {
            conn = dataSource.getConnection();
            stmt = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            queryRunner.fillStatement(stmt, quarzDataBean.getTriggers_map(),quarzDataBean.getJobs_map(),
                    new Date(), new Date());
            stmt.executeUpdate();
            ResultSet resultSet = stmt.getGeneratedKeys();
            resultSet.next();
            int key = resultSet.getInt(1);
            if(key>10){
                queryRunner = new QueryRunner(dataSource);
                queryRunner.update("delete from dw_scheduler_ha_quarz where id <"+(key-10));
            }
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }finally {
            if(conn != null){
                try{ conn.close(); }catch(Throwable ex1){}
              }
              DbUtils.closeQuietly(conn, stmt, null);
        }
    }

    /**
     * 获取最新quarz备份数据
     */
    public QuarzDataBean getNewestQuarzBackData(){
        //最新quarz备份数据
        final String sql = "select * from dw_scheduler_ha_quarz where id =(select max(id) from dw_scheduler_ha_quarz)";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            QuarzDataBean quarzData = queryRunner.query(sql, new QuarzResultSetHandler(true));
            if(quarzData!=null){
                updateQuarzBackUpData(quarzData);
            }
            return quarzData;
        } catch (SQLException e) {
            LOG.error("getNewestQuarzBackData",e);
            return null;
        }
    }

    //取出备份的quarzData，立刻更新下该状态
    public void updateQuarzBackUpData(QuarzDataBean quarzData) {
        final String sql = "update dw_scheduler_ha_quarz set status=2 where id=?";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            queryRunner.update(sql, quarzData.getId());
        } catch (Exception e) {
            LOG.error("updateQuarzBackUpData",e);
        }

    }

    /**
     * 获取最后执行所有调度数据
     */
    public Map<String,DWJob> getNewestExcuteLog(Date date){
        //最新quarz备份数据
        final String sql = "select * from dw_scheduler_task_excute_log where task_id in (1,2) and create_time>curdate() and create_time<=? order by scheduler_id,create_time asc";
        try {
            QueryRunner queryRunner = new QueryRunner(dataSource);
            List<Map<String,Object>> excuteLogMapList = queryRunner.query(sql,new MapListHandler(),date);
            Map<String,DWJob> excuteJobMap = new HashMap<String,DWJob>();
            String mapKey = null;
            for(Map<String,Object> excuteLog : excuteLogMapList){
                DWJob task = transExcuteLogMapToDwJob(excuteLog);
                mapKey = String.valueOf(task.getSchedulerId());
                excuteJobMap.put(mapKey, task);
            }
            return excuteJobMap;
        } catch (SQLException e) {
            throw new SchedulerException(e);
        }
    }

    private DWJob transExcuteLogMapToDwJob(Map<String,Object> excuteLogMap){
        DWJob task = new DWJob();
        try {
            task.setExcuteId((Integer) excuteLogMap.get("id"));
            task.setSchedulerId((Integer) excuteLogMap.get("scheduler_id"));
            task.setJobId((Integer) excuteLogMap.get("job_id"));
            task.setJobName((String) excuteLogMap.get("job_name"));
            task.setTaskId((Integer) excuteLogMap.get("task_id"));
            task.setCommand((String) excuteLogMap.get("dispatch_command"));
            task.setRunTime((String) excuteLogMap.get("run_time"));
            task.setRunType((Integer) excuteLogMap.get("type"));
            task.setNextFireTime((Date) excuteLogMap.get("next_fire_time"));
            task.setJobState((Integer) excuteLogMap.get("job_state"));
            task.setDependent_jobs((String) excuteLogMap.get("dependent_jobs"));
            task.setRetry((Integer) excuteLogMap.get("retry"));
            task.setCreateTime( (Date) excuteLogMap.get("create_time"));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return task;
    }

}
