package com.ajk.dw.scheduler.job;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.dbutils.ResultSetHandler;

public class JobRetryListResultSetHandler implements
        ResultSetHandler<List<DWJob>> {


    public JobRetryListResultSetHandler() {
        super();
    }

    public List<DWJob> handle(ResultSet rs) throws SQLException {
        List<DWJob> list = new ArrayList<DWJob>();
        while (rs.next()) {
            DWJob task = new DWJob();
            task.setSchedulerId(rs.getInt("scheduler_id"));
            task.setRunCount(rs.getInt("run_count"));
            task.setRetry(rs.getInt("retry"));
            list.add(task);
        }

        return list;
    }
}
