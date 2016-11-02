package com.ajk.dw.scheduler.job;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.ResultSetHandler;

public class JobResultSetHandler implements ResultSetHandler<DWJob> {

    private final boolean next;

    public JobResultSetHandler(boolean next) {
        super();
        this.next = next;
    }

    public DWJob handle(ResultSet rs) throws SQLException {
        if (next) {
            if (!rs.next()) {
                return null;
            }
        }
        DWJob task = new DWJob();
        task.setExcuteId(rs.getInt("excute_id"));
        task.setCommand(rs.getString("command"));
        task.setJobId(rs.getInt("job_id"));
        task.setJobState(rs.getInt("job_stat"));
        task.setExecuteStartTime(rs.getLong("execute_start_time") * 1000);
        task.setExecuteEndTime(rs.getLong("execute_end_time") * 1000);
        task.setExitCode(rs.getInt("exit_code"));
        return task;
    }

}
