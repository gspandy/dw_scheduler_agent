package com.ajk.dw.scheduler.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbutils.ResultSetHandler;

public class JobListResultSetHandler implements
        ResultSetHandler<List<DWJob>> {


    public JobListResultSetHandler() {
        super();
    }

    public List<DWJob> handle(ResultSet rs) throws SQLException {
        List<DWJob> list = new ArrayList<DWJob>();
        while (rs.next()) {
            DWJob task = new DWJob();
            task.setConfigId(rs.getInt("id"));
            task.setSchedulerId(rs.getInt("scheduler_id"));
            task.setJobId(rs.getInt("job_id"));
            task.setJobName(rs.getString("job_name"));
            task.setTaskId(rs.getInt("task_id"));
            task.setCommand(rs.getString("dispatch_command"));
            task.setRunTime(rs.getString("run_time"));
            task.setJobStatus(rs.getInt("status"));
            task.setRunType(rs.getInt("type"));
            task.setJobState(rs.getInt("job_state"));
            task.setDependent_jobs(rs.getString("dependent_jobs"));
            task.setRetry(rs.getInt("retry"));
            list.add(task);
        }

        return list;
    }
}
