package com.ajk.dw.scheduler.quarz;

import java.sql.Blob;
import java.util.Date;

public class QuarzDataBean {
	
	private Integer id;
	private Blob triggers_map;
	private Blob jobs_map;
	private Integer status;
	private Date create_time;
	private Date update_time;
	
	public Integer getId() {
		return id;
	}
	
	public void setId(Integer id) {
		this.id = id;
	}
	
	public Blob getTriggers_map() {
		return triggers_map;
	}
	
	public void setTriggers_map(Blob triggers_map) {
		this.triggers_map = triggers_map;
	}
	
	public Blob getJobs_map() {
		return jobs_map;
	}
	
	public void setJobs_map(Blob jobs_map) {
		this.jobs_map = jobs_map;
	}
	
	public Integer getStatus() {
		return status;
	}
	
	public void setStatus(Integer status) {
		this.status = status;
	}
	
	public Date getCreate_time() {
		return create_time;
	}
	
	public void setCreate_time(Date create_time) {
		this.create_time = create_time;
	}
	
	public Date getUpdate_time() {
		return update_time;
	}
	
	public void setUpdate_time(Date update_time) {
		this.update_time = update_time;
	}
	
}
