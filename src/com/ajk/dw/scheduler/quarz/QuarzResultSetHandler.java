package com.ajk.dw.scheduler.quarz;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.dbutils.ResultSetHandler;

public class QuarzResultSetHandler implements ResultSetHandler<QuarzDataBean> {

    private final boolean next;

    public QuarzResultSetHandler(boolean next) {
        super();
        this.next = next;
    }

    public QuarzDataBean handle(ResultSet rs) throws SQLException {
        if (next) {
            if (!rs.next()) {
                return null;
            }
        }
        QuarzDataBean quarzData = new QuarzDataBean();
        quarzData.setId(rs.getInt("id"));
        quarzData.setTriggers_map(rs.getBlob("triggers_map"));
        quarzData.setJobs_map(rs.getBlob("jobs_map"));
        quarzData.setStatus(rs.getInt("status"));
        quarzData.setCreate_time(rs.getTimestamp("create_time"));
        quarzData.setUpdate_time(rs.getTimestamp("update_time"));
        return quarzData;
    }

}
